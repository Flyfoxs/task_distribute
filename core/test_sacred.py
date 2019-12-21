# Connect to client
from pymongo import MongoClient

client = MongoClient('mongodb://sample:password@mongo:27017/db?authSource=admin')

from sacred import Experiment

ex = Experiment('hello_config', interactive=True)
from sacred.observers import MongoObserver, FileStorageObserver

ex.observers.append(MongoObserver(url='mongodb://sample:password@mongo:27017/db?authSource=admin',
                                  db_name='db'))


@ex.config
def my_config():
    recipient = "world"
    message = "Hello %s!" % recipient


@ex.main
def my_main(message):
    print(message)
if  __name__ == '__main__':

    ex.run_commandline()