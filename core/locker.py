import contextlib
import os

import pandas as pd

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import functools
from bson.objectid import ObjectId
from datetime import datetime
import socket
from easydict import EasyDict as edict

class task_locker:

    def __init__(self, url, version, rollback=True, remove_failed=True):
        """

        :param url:
        :param version:
        :param rollback:
        :param remove_failed: It only work when the task is manager by sacred
        """
        self.url = url
        self.client = MongoClient(url)
        self.task = self.client['task'].task
        self.task.create_index([('_version', 1), ('_task_id', 1)], unique=True);
        self.version = version
        self.rollback = rollback
        self.remove_failed = remove_failed

    def register_lock(self, _task_id, **kwargs):
        if self.version is None or self.version is False:
            return True
        try:
            print('begin insert')
            lock_id = self.task.insert({
                "_version": self.version,
                "_task_id": _task_id,
                'server_ip': socket.gethostname(),
                'ct': datetime.now(),
                **kwargs})
            print(f'create lock success for block#{_task_id} with:{lock_id}')
            return str(lock_id)
        except Exception as e:
            if isinstance(e, DuplicateKeyError):
                print(f'Already has same taskid:{self.version},{_task_id}')
            else:
                raise e
            return False

    def update_lock(self, lock_id, result):
        #         res = self.task.find_one({'_id':ObjectId(lock_id)})
        #         begin = res.get('ct')
        #         time.sleep(3)
        #         end = datetime.now()
        #         duration = end-begin
        #         print('duration=',duration)

        if self.version is None or self.version is False:
            return True

        self.task.update_one({'_id': ObjectId(lock_id)},
                             {'$set': {'result': result, },
                              "$currentDate": {"mt": True}
                              },
                             )
        print(f'block done with {lock_id}')


    def lock(self, max_time=-1):
        locker = self

        def decorator(f):
            @functools.wraps(f)
            def wrapper(*args, **kwargs):

                job_paras = {'fn_name': f.__name__,
                             'args': args,
                             'kwargs': kwargs}
                print('job_paras', job_paras)
                print(f"{f.__name__}({args},{kwargs})")
                lock_id = locker.register_lock(_task_id=str(job_paras), **job_paras, )
                if lock_id:
                    try:
                        res = f(*args, **kwargs)

                        locker.update_lock(lock_id, res)
                        return res
                    except Exception as e:
                        if self.rollback:
                            print(f'Rollback the locker:{lock_id}')
                            locker.task.remove({'_id': ObjectId(lock_id)})
                        raise e
                else:
                    exist_lock = locker.task.find_one({"_version": self.version,
                                                       '_task_id': str(job_paras)})

                    raise Warning(f'Already had lock#{exist_lock}')

            return wrapper

        return decorator

    @contextlib.contextmanager
    def lock_block(self, task_id='Default_block', **job_paras):
        import sys
        if self.remove_failed:
            self.remove_failed_lock(task_id)

        lock_id = self.register_lock(_task_id=task_id, **job_paras, )
        if lock_id:
            try:
                yield lock_id
                self.update_lock(lock_id, result=None)
            except Exception as e:
                if self.rollback:
                    print(f'Rollback the locker:{lock_id}')
                    self.task.remove({'_id': ObjectId(lock_id)})
                raise e

        else:
            exist_lock = self.task.find_one({"_version": self.version,
                                               '_task_id': task_id
                                             })
            if exist_lock:
                raise Exception(f'Already had lock#{exist_lock}')
            else:
                raise Exception(f'Can not create or find any lock')


    def remove_version(self):
        self.task.remove({'_version' : self.version})

    def remove_failed_lock(self, task_id):

        exist_lock = self.client.task.task.find_one({"_version": self.version, '_task_id': task_id})

        try:
            exist_task = self.client.db.runs.find_one({'config.lock_name': task_id,
                                                       'config.version':self.version,
                                                       'status':'FAILED'
                                                       })
        except Exception as e:
            exist_task = None

        if exist_lock and exist_task:
            self.task.remove({"_version": self.version, '_task_id': task_id})
            print(f'Remove the lock {exist_lock}, job is failed')


