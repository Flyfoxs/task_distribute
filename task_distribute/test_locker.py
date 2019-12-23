from task_distribute.locker import task_locker
import sys
locker = task_locker('mongodb://sample:password@mongo:27017/db?authSource=admin', version='v300')



with locker.lock_block('task_3', abc='def') as lock_id:
    print('====='*10, lock_id)

    raise Exception('block')
    print('test')
