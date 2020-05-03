from task_distribute.locker import task_locker
import sys
locker = task_locker('mongodb://sample:password@localhost:27017/db?authSource=admin', version='v306')


for i in range(1,4) :
    with locker.lock_block(task_id=f'task_t4_{i}') as lock_id:
        if lock_id is not None:
            print('====='*10, lock_id)
            import time
            time.sleep(60)
            #raise Exception('block')
            print('test')


for i in range(1,4) :
    with locker.lock_block(task_id=f'task_t4_{i}', abc='def') as lock_id:
        if lock_id is not None:
            print('====='*10, lock_id)
            import time
            time.sleep(60)
            #raise Exception('block')
            print('test')
