import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import pandas as pd
from mongoDB.mongoDB_operations import *

# Function to divide album list into small groups (chunks)
def chunk_album_ids(album_ids,chunk_size=20):
    for i in range(0,len(album_ids),chunk_size):
        yield album_ids[i:i+chunk_size]
        
# Function to get album data from Spotify API
def crawl_album_track(dfArtist: pd.DataFrame, Execution_date: str):

    sp =spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id='1c7d0aa2f79f40738f056ea7a28af970'
                                                          ,client_secret='1ff9dbf85639452fb46fcb8c8c2c3e20'))
    
    Album_Data =[]
    Track_Data =[]
    album_id_list = []
    i=1
    for artist_id in dfArtist['Artist_ID']:
        result = sp.artist_albums(artist_id=artist_id,album_type ='album') #Get information from spotify and save it to result
        if result and result['items']:
            for album in result['items']: # Browse through each album to save to the list
                album_id_list.append(album['id']) # Add album information to the list
                
        else:
            print(f"No albums found for artist ID: {artist_id}")

    #Split album list into chunks
    for chunk in chunk_album_ids(album_id_list):
        print(str(i)+f" )Calling API for {len(chunk)} albums")
        print(chunk)
        albums = sp.albums(chunk) # Get information about the albums from the API
        for album in albums['albums']:
            copyrights = album.get('copyrights', []) # Get information about the copyrights
            copyrights_info = ', '.join([c['text'] for c in copyrights]) if copyrights else "No copyrights information"
            Album_Data.append({ # Add album information to the list
                'Artist':album['artists'][0]['name'],
                'Artist_ID':album['artists'][0]['id'],
                'Album_ID':album['id'],
                'Name':album['name'],
                'Type':album['album_type'],
                'Genres': ','.join(album.get('genres', [])),
                'Label':album.get('label','Unknown'),
                'Popularity':album.get('popularity',None),
                'Available_Markets':','.join(album.get('available_markets',[])),
                'Release_Date':album.get('release_date','Unknow'),
                'ReleaseDatePrecision':album.get('release_date_precision','Unknow'),
                'TotalTracks':album.get('total_tracks',None),
                'Copyrights': copyrights_info,
                'Restrictions': album.get('restrictions', {}).get('reason', None),
                'External_URL': album.get('external_urls', {}).get('spotify',None),
                'Href': album.get('href',None),
                'Image': album['images'][0]['url'] if album.get('images') and len(album['images']) > 0 else None,
                'Uri': album.get('uri',None)
            })
            for track in album['tracks']['items']:
                Track_Data.append({
                    'Artists': ', '.join(artist['name'] for artist in track['artists']),  # Join the artists' names into a string
                    'Album_Name':album['name'],
                    'Album_ID':album['id'],
                    'Track_ID': track['id'],
                    'Name': track['name'],
                    'Track_Number': track['track_number'],
                    'Type': track['type'],
                    'AvailableMarkets': ','.join(track.get('available_markets', [])),
                    'Disc_Number': track['disc_number'],
                    'Duration_ms': track['duration_ms'],
                    'Explicit': track['explicit'],
                    'External_urls': track['external_urls'].get('spotify') if track.get('external_urls') else None,  # check externalURL
                    'Href': track['href'],
                    'Restrictions': track.get('restrictions', {}).get('reason', None),
                    'Preview_url': track.get('preview_url',None),
                    'Uri': track['uri'],
                    'Is_Local': track['is_local']
                })
        i+=1
    Album_Data, Track_Data = pd.DataFrame(Album_Data), pd.DataFrame(Track_Data)
    Album_Data['Execution_date'] = Execution_date
    Track_Data['Execution_date'] = Execution_date

    return Album_Data, Track_Data

def load_daily_album_track_mongoDB(Execution_date: str):
    with mongoDB_client(username = 'huynhthuan', password = 'password') as client:
        client_operations = mongoDB_operations(client)
        daily_artist_data = client_operations.read_data(database_name = 'music_database', collection_name = 'artist_collection', query = {'Execution_date': Execution_date})
        daily_album_data, daily_track_data = crawl_album_track(daily_artist_data, Execution_date)
        client_operations.insert_data(database_name = 'music_database', collection_name = 'album_collection', data = daily_album_data)
        client_operations.insert_data(database_name = 'music_database', collection_name = 'track_collection', data = daily_track_data)

        