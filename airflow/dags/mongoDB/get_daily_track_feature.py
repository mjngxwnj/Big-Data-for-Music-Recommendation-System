import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import pandas as pd
from mongoDB.mongoDB_operations import *
# Function to divide track id list into small groups (chunks)
def chunk_track_ids(track_ids,chunk_size=100):
    for i in range(0,len(track_ids),chunk_size):
        yield track_ids[i:i+chunk_size]


def crawl_track_feature(dfTrack: pd.DataFrame, Execution_date: str): 
    sp =spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id='1c7d0aa2f79f40738f056ea7a28af970'
                                                          ,client_secret='1ff9dbf85639452fb46fcb8c8c2c3e20'))
    Track_Feature_Data =[]
    i=1

    track_ids = dfTrack['Track_ID'].tolist()
    # Split track id list into chunks of 100
    for chunk in chunk_track_ids(track_ids):
        print(chunk)
        print(str(i)+f" )Calling API for {len(chunk)} tracks")
        try:
            tracks = sp.audio_features(chunk) # Get information about multiple tracks
        except Exception as e:
            return None
        
        if tracks:
            for track,track_id in zip(tracks or [],chunk):
                if track:
                    Track_Feature_Data.append({
                        'Track_ID':track_id,
                        'Danceability':track.get('danceability',None),
                        'Energy':track.get('energy',None),
                        'Key':track.get('key',None),
                        'Loudness':track.get('loudness',None),
                        'Mode':track.get('mode',None),
                        'Speechiness':track.get('speechiness',None),
                        'Acousticness':track.get('acousticness',None),
                        'Instrumentalness':track.get('instrumentalness',None),
                        'Liveness':track.get('liveness',None),
                        'Valence':track.get('valence',None),
                        'Tempo':track.get('tempo',None),
                        'Time_signature':track.get('time_signature',None),
                        'Track_href':track.get('track_href',None),
                        'Type_Feature':track.get('type',None),
                        'Analysis_Url':track.get('analysis_url',None)
                    })
                else:
                    Track_Feature_Data.append({
                        'Track_ID':track_id,
                        'Danceability':None,
                        'Energy':None,
                        'Key':None,
                        'Loudness':None,
                        'Mode':None,
                        'Speechiness':None,
                        'Acousticness':None,
                        'Instrumentalness':None,
                        'Liveness':None,
                        'Valence':None,
                        'Tempo':None,
                        'Time_signature':None,
                        'Track_href':None,
                        'Type_Feature':None,
                        'Analysis_Url':None
                })
        i+=1
    Track_Feature_Data = pd.DataFrame(Track_Feature_Data)
    Track_Feature_Data['Execution_date'] = Execution_date
    return Track_Feature_Data

def load_daily_track_feature_mongoDB(Execution_date: str):
    with mongoDB_client(username = 'huynhthuan', password = 'password') as client:
        client_operations = mongoDB_operations(client)
        daily_track_data = client_operations.read_data(database_name = 'music_database', collection_name = 'track_collection', query = {'Execution_date': Execution_date})
        daily_track_feature_data = crawl_track_feature(daily_track_data, Execution_date)
        client_operations.insert_data(database_name = 'music_database', collection_name = 'trackfeature_collection', data = daily_track_feature_data)
