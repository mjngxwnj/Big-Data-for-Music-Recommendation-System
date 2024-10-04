from mongoDB.mongoDB_operations import mongoDB_client, mongoDB_operations

def main():
    #use mongoDB client
    with mongoDB_client(username = 'huynhthuan', password = 'password') as client:
        client = mongoDB_operations(client)
        client_db = client.create_database_if_not_exists('testdb')
        client_collection = client.create_collection_if_not_exists(client_db, 'testcollection')
        client_data = client.insert_data(client_collection, [{'name': 'Thuan'}])