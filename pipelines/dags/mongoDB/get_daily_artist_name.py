import pandas as pd # add pandas
from datetime import datetime
from mongoDB.mongoDB_operations import *

def crawl_artist_name(Execution_date: str):
    # The URL of the website containing the art listing
    url = 'https://kworb.net/itunes/extended.html'
    # Read all tables from the website into a list
    table = pd.read_html(url)
    # Get the first table from the site data (The table includes the names of the artists)
    ArtistName = table[0][['Pos','Artist']]
    ArtistName['Execution_date'] = Execution_date
    print("Completed")
    return ArtistName.astype(str)

# crawl_new_artist_name()
def load_daily_artist_name_mongoDB(Execution_date: str):
    with mongoDB_client(username = 'huynhthuan', password = 'password') as client:
        client_operations = mongoDB_operations(client)
        old_artist_name_data = client_operations.read_data(database_name = 'music_database', collection_name = 'artist_name_collection')    
        old_artist_name_data = old_artist_name_data[['Artist']]
        old_artist_name_data.rename(columns = {'Artist': 'Old_Artist'}, inplace = True)
        new_artist_name_data = crawl_artist_name(Execution_date)

        daily_artist_name_data = pd.merge(old_artist_name_data, new_artist_name_data, left_on = 'Old_Artist', right_on = 'Artist', how = 'right')
        daily_artist_name_data = daily_artist_name_data[daily_artist_name_data['Old_Artist'].isnull()][['Pos', 'Artist', 'Execution_date']]
        daily_artist_name_data = daily_artist_name_data.head(1000)
        
        client_operations.insert_data(database_name = 'music_database', collection_name = 'artist_name_collection', data = daily_artist_name_data)
