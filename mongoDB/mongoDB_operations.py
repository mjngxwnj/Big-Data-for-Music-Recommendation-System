from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from contextlib import contextmanager

""" Context manager for mongoDB connection. """
@contextmanager
def mongoDB_client(username: str, password: str, 
                    host: str = 'mongo', port: str = 27017):
    #set path
    path = f"mongodb://{username}:{password}@{host}:{port}/"

    #init
    try:
        print("Starting connect mongoDB...")
        client = MongoClient(path)
        
        print("Client connected successfully!")
        yield client

    #handle error
    except ConnectionFailure:
        print("Connection to mongoDB failed!")

    except OperationFailure:
        print("Operation failed!")

    #close client
    finally:
        client.close()
        print("The connection to MongoDB has stopped!")

""" Class mongoDB for operations. """
class mongoDB_operations:
    """ Init """
    def __init__(self, client: MongoClient, database: str = None, collection: str = None):
        #check params
        if not isinstance(client, MongoClient):
            raise TypeError('client must be MongoClient!')
        
        #set value for class attrs
        self.client = client
        self.database = database
        self.collection = collection

    """ Check whether the database exists. """
    def check_database_exists(self, database: str) -> bool:
        #list database name
        return database in self.client.list_database_names()

    """ Check whether collection exists. """
    def check_collection_exists(self, database: str, collection: str) -> bool:
        #check whether database exists
        if self.check_database_exists(database):
            #list collection name
            return collection in self.client[database].list_collection_names()
        #collection does not exist
        return False

    """ Create new database. """
    def create_database(self, database: str):
        #check whether database exists
        if self.check_database_exists(database):
            print(f"The database '{database}' exists!")
        #create db
        else:
            self.client[database]
    
    """ Create new collection. """
    def create_collection(self, database: str, collection: str):
        #check whether database exists
        if self.check_collection_exists(database, collection):
            print(f"The collection '{collection} exists!")
        #create collection
        else:
            self.client[database][collection]

    """ Insert data"""
    def insert_data(self, database: str, collection: str, data: list[dict]):
        #check if data are right type
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise TypeError("data must be a list of dictionaries!")
        
        #check whether collection exists
        if self.check_collection_exists(database, collection):
            #insert data
            self.client[database][collection].insert_many(data)

        #db or collection does not exist
        else:
            print(f"Collection '{collection}' or database '{database}' does not exist!")

if __name__ == '__main__':
    with mongoDB_client('huynhthuan', 'password') as client:
        client = mongoDB_operations(client)
        client.create_database('test')
