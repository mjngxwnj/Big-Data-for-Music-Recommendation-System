from mongoDB.mongoDB_operations import mongoDB_client, mongoDB_operations
import pandas as pd

""" Convert data to dictionaries. """
def get_dict_data(csv_path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    df = df.to_dict(orient = 'records')

    return df

def load_data_mongodb_artist(artist_path: str = '/opt/data/Artist.csv'):
    #use mongoDB client
    with mongoDB_client(username = 'huynhthuan', password = 'password') as client:
        client = mongoDB_operations(client)
        #create artist database
        client_artist_database = client.create_database_if_not_exists(database_name= 'artist_database')

        #create artist collection
        client_artist_collection = client.create_collection_if_not_exists(database_obj = client_artist_database, 
                                                                          collection = 'artist_collection')

        #get data
        data = get_dict_data(artist_path)    

        #insert artist data
        client_artist_insert = client.insert_data(collection_obj = client_artist_collection, data = data)
        

