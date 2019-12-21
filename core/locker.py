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
import sys
from sacred.run import Run

class task_locker:

    def __init__(self, url, version, rollback=True, remove_failed=2):
        """

        :param url:
        :param version:
        :param rollback:
        :param remove_failed: 0: Don't remove
                              1: Remove locker
                              2: Remove locker and sacred task
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

        if self.remove_failed:
            self.remove_failed_lock(_task_id)

        try:

            kwargs_mini = dict([(k, v) for k, v in kwargs.items() if not isinstance(v,Run) ])
            print('begin insert')
            lock_id = self.task.insert({
                "_version": self.version,
                "_task_id": _task_id,
                'server_ip': socket.gethostname(),
                'ct': datetime.now(),
                **kwargs_mini})
            print(f'create lock success for block#{_task_id} with:{lock_id}')
            return str(lock_id)
        except Exception as e:
            if isinstance(e, DuplicateKeyError):
                print(f'Already has same taskid:{self.version},{_task_id}')
            else:
                print(kwargs)
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

    def lock(self, ex=None):
        locker = self

        def decorator(f):
            @functools.wraps(f)
            def wrapper(*args, **kwargs):

                job_paras = {'fn_name': f.__name__,
                             'args': args,
                             'kwargs': kwargs}

                if ex is not None:
                    lock_name = f.__name__ + ',' + ','.join([item for item in sys.argv])
                else:
                    print('job_paras', job_paras)
                    print(f"{f.__name__}({args},{kwargs})")
                    lock_name = str(job_paras)

                lock_id = locker.register_lock(_task_id=lock_name, **job_paras, )

                if lock_id:
                    try:
                        if ex is not None:
                            ex.add_config({
                                'lock_id': lock_id,
                                'lock_name': lock_name,
                                'version': self.version,
                            })
                            print('======ex', ex)
                        print('222222======ex', ex)
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
        """
        It's only work when the job is running with sacred job
        :param task_id:
        :return:
        """

        exist_lock = self.client.task.task.find_one({"_version": self.version, '_task_id': task_id})

        try:
            exist_task = self.client.db.runs.find_one({'config.lock_name': task_id,
                                                       'config.version':self.version,
                                                       'status':'FAILED'
                                                       })
        except Exception as e:
            exist_task = None

        if exist_lock and exist_task and self.remove_faile >= 1:
            self.task.remove({"_version": self.version, '_task_id': task_id})
            print(f'Remove the lock {exist_lock}, job is failed')

        if exist_lock and exist_task and self.remove_faile == 2:
            self.client.db.runs.remove({'config.lock_name': task_id,
                                       'config.version':self.version,
                                       'status':'FAILED'
                                       })
            print(f'Remove the failed sacred Job task_id:{task_id}, version:{self.version}')


