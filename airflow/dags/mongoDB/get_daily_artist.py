import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import pandas as pd
from mongoDB.mongoDB_operations import *

def getArtistData(artistName):
    sp =spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id='1c7d0aa2f79f40738f056ea7a28af970'
                                                          ,client_secret='1ff9dbf85639452fb46fcb8c8c2c3e20'))
    
    result = sp.search(q='artist:' + artistName, type='artist') #Sử dụng biến api_call để lưu lại hàm lambda với lời gọi api tương ứng
    if result and result['artists']['items']:  # Kiểm tra nếu tìm thấy nghệ sĩ
        artist = result['artists']['items'][0]  # Lấy nghệ sĩ đầu tiên
        artistId = artist['id']  # ID nghệ sĩ
        artistInfo = { #Thêm các thông tin của nghệ sĩ vào list 
            'name': artist['name'],
            'genres': ', '.join(artist['genres']),  # Nối các thể loại lại thành chuỗi
            'followers': artist['followers']['total'],
            'popularity':artist['popularity'],
            'image':artist['images'][0]['url'] if artist['images'] else None,
            'type':artist['type'],
            'externalURL':artist['external_urls'],
            'href':artist['href'],
            'uri':artist['uri']
    }
        return artistId, artistInfo #Trả về ID và thông tin của nghệ sĩ
    print(f"Can't find artist: {artistName}")
    return None,None #Trả về None nếu không có thông tin 

def crawl_artist(dfArtistName: pd.DataFrame, Execution_date: str):
    Artist_Data = [] 
    i=1
    for artistName in dfArtistName['Artist']: #Lặp từng nghệ sĩ trong danh sách
        print(str(i)+")Loading Artist..."+ artistName)
        artistId,artistInfo = getArtistData(artistName) #Lấy thông tin từ hàm đã cài đặt
        if artistId and artistInfo:
            Artist_Data.append({ #Thêm thông tin vào List lưu trữ
                            'Artist_ID':artistId,
                            'Artist_Name':artistInfo['name'],
                            'Genres':artistInfo['genres'],
                            'Followers':artistInfo['followers'],
                            'Popularity':artistInfo['popularity'],
                            'Artist_Image':artistInfo['image'],
                            'Artist_Type':artistInfo['type'],
                            'External_Url':artistInfo['externalURL'],
                            'Href':artistInfo['href'],
                            'Artist_Uri':artistInfo['uri']
                    })
        i+=1
    Artist_Data = pd.DataFrame(Artist_Data)
    Artist_Data['Execution_date'] = Execution_date
    print("Successfully") 
    return Artist_Data   

def load_daily_artist_mongoDB(Execution_date: str):
    with mongoDB_client(username = 'huynhthuan', password = 'password') as client:
        client_operations = mongoDB_operations(client)
        daily_artist_name_data = client_operations.read_data(database_name = 'music_database', collection_name = 'artist_name_collection', query = {'Execution_date': Execution_date})
        daily_artist_data = crawl_artist(daily_artist_name_data, Execution_date)
        client_operations.insert_data(database_name = 'music_database', collection_name = 'artist_collection', data = daily_artist_data)
    