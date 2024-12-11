from pymongo import MongoClient, database, collection
from pymongo.errors import ConnectionFailure, OperationFailure
from contextlib import contextmanager
import pandas as pd
""" Context manager for mongoDB connection. """
@contextmanager
def mongoDB_client(username: str, password: str, 
                    host: str = 'mongo', port: str = 27017):
    #set path
    path = f"mongodb://{username}:{password}@{host}:{port}"
    client = None

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
    def __init__(self, client: MongoClient):
        #check params
        if not isinstance(client, MongoClient):
            raise TypeError('client must be MongoClient!')
        
        #set value for class attrs
        self.client = client

    """ Check whether the database exists. """
    def check_database_exists(self, database_name: str) -> bool:
        #list database name
        return database_name in self.client.list_database_names()

    """ Check whether collection exists. """
    def check_collection_exists(self, database_obj: database.Database, collection: str) -> bool:
        #check params
        if not isinstance(database_obj, database.Database):
            raise TypeError("database_obj must be a database.Database!")
        
        #list collection name
        return collection in self.client[database_obj.name].list_collection_names()

    """ Create new database. """
    def create_database_if_not_exists(self, database_name: str) -> database.Database:
        #check whether database exists
        if self.check_database_exists(database_name):
            print(f"Don't create the database '{database_name}' because it already exists.")
        else:
            print(f"Successfully created database '{database_name}'.")

        #return database
        return self.client[database_name]
    
    """ Create new collection. """
    def create_collection_if_not_exists(self, database_obj: database.Database, collection: str) -> collection.Collection:
        #check params
        if not isinstance(database_obj, database.Database):
            raise TypeError("database_obj must be a database.Database!")
        
        #check whether collection exists
        if self.check_collection_exists(database_obj, collection):
            print(f"Don't create the collection '{collection}' because it already exists.")
        else:
            print(f"Successfully created collection '{collection}'.")

        #return collection
        return self.client[database_obj.name][collection]
    
    """ Insert data. """
    def insert_data(self, database_name: str, collection_name: str, data = pd.DataFrame):
        #check params
        if not isinstance(data, pd.DataFrame):
            raise TypeError("data must be a DataFrame!")
        
        database_obj = self.create_database_if_not_exists(database_name)
        collection_obj = self.create_collection_if_not_exists(database_obj, collection_name)
        #insert data
        data = data.to_dict(orient = 'records')
        collection_obj.insert_many(data)

        print(f"Successfully inserted data into collection '{collection_obj.name}'.")
    
    """ Read data. """
    def read_data(self, database_name: str, collection_name:str, query: dict = None) -> pd.DataFrame:
        #check params
        if query is not None and not isinstance(query, dict):
            raise TypeError("query must be a dict!")
        
        #check database and collection exist
        if not self.check_database_exists(database_name):
            raise Exception(f"Database '{database_name}' does not exist!")
        if not self.check_collection_exists(database_obj = self.client[database_name], collection = collection_name):
            raise Exception(f"Collection '{collection_name}' does not exist!")
        

        data = self.client[database_name][collection_name].find(query)
        data = pd.DataFrame(list(data))
        return data