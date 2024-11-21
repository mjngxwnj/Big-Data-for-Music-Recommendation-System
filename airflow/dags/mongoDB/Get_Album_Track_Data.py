import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import pandas as pd
import os
import requests
import csv
# Import necessary libraries
# dfAlbumID = pd.read_csv('D:\\Study\\C++\\Source Code\\Python\\Project_Music_recommend\\mydata.csv')
# album_file = 'D:\\Study\\C++\\Source Code\\Python\\Project_Music_recommend\\AlbumDatatmp.csv'
# track_file = 'D:\\Study\\C++\\Source Code\\Python\\Project_Music_recommend\\TrackData.csv'
# album_columns=['Artist','Artist_ID', 'Album_ID', 'Name', 'Type', 'Genres', 'Label', 'Popularity', 'Available_Markets',
#                'Release_Date', 'ReleaseDatePrecision', 'TotalTracks', 'Copyrights', 'Restrictions', 'External_URL', 'Href', 'Image', 'Uri']
# track_columns=['Artists','Album_ID','Album_Name','Track_ID', 'Name', 'Track_Number', 'Type', 'AvailableMarkets', 'Disc_Number', 
#                'Duration_ms', 'Explicit', 'External_urls', 'Href', 'Restrictions', 'Preview_url', 
#                'Uri', 'Is_Local']
sp =spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id='05e2ff0a21954615b11878a9eb038e7f'
                                                          ,client_secret='7f2e7dc0bd0e41caa3665b5dea9ab8e0'))
# def write_to_csv(file_path, data, columns):
#     # Convert data into dataframe with specified columns
#     df = pd.DataFrame(data, columns=columns)
#     # Open the file in append mode ('a') and write data to it
#     with open(file_path, 'a', newline='', encoding='utf-8') as f:
#         # Write data with `header=False` flag if file already exists, write header only if file is empty
#         df.to_csv(f, header=f.tell() == 0, index=False)

# Function to divide album list into small groups (chunks)
def chunk_album_ids(album_ids,chunk_size=20):
    for i in range(0,len(album_ids),chunk_size):
        yield album_ids[i:i+chunk_size]

# Function to get album data from Spotify API
def getAlbumData(album_ids): 
    album_info =[]
    track_info =[]
    i=1
    #Split album list into chunks
    for chunk in chunk_album_ids(album_ids):
        print(str(i)+f" )Calling API for {len(chunk)} albums")
        albums = sp.albums(chunk) # Get information about the albums from the API
        for album in albums['albums']:
            copyrights = album.get('copyrights', []) # Get information about the copyrights
            copyrights_info = ', '.join([c['text'] for c in copyrights]) if copyrights else "No copyrights information" #If there is information, get the opposite information and return No information
            album_info.append({ # Add album information to the list
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
                track_info.append({
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
    return album_info,track_info

albumIds = dfAlbumID['Album_ID'].tolist()  # Get the album IDs from the dataframe
album_info, track_info = getAlbumData(albumIds)

print("Successful")
                