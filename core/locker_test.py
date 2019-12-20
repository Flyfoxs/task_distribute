from core.locker import task_locker

locker = task_locker('mongodb://sample:password@mongo:27017/db?authSource=admin', version='v100')


locker.remove_version()


@locker.lock()
def testabc(a, b, c=4, d=5):
    raise Exception('Test')
    return 'done'

testabc(1, d=3, b=3, c=24)



@locker.lock()
def testabc_exception(a, b, c=4, d=5):
    raise Exception('Test')
    return 'done'

testabc_exception(1, d=3, b=3, c=24)


with locker.lock_block('task_2', abc='def'):
    print('test')
