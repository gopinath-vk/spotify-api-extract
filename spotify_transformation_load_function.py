import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_list=[]
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_rel_date = row['track']['album']['release_date']
        album_url = row['track']['album']['external_urls']['spotify']
        album_tot_track = row['track']['album']['total_tracks']
        album_dict = {'Album_ID':album_id, 'Album_Name':album_name, 'Release_Date':album_rel_date, 'URL':album_url, 'Total_Tracks':album_tot_track}
        album_list.append(album_dict)
    return album_list
    
def artist(data):
    artist_list=[]
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for value in value['artists']:
                    artist_dict = {'Artist_ID':value['id'], 'Artist_Name':value['name'], 'URL':value['href']}
                    artist_list.append(artist_dict)
    return artist_list
    
def track(data):
    track_list=[]
    for row in data['items']:
        track_id = row['track']['id']
        track_name = row['track']['name']
        track_duration = row['track']['duration_ms']
        track_popularity = row['track']['popularity']
        track_url = row['track']['external_urls']['spotify']
        album_id = row['track']['album']['id']
        track_dict = {'Track_ID':track_id, 'Track_Name':track_name, 'Track_Duration':track_duration, 'Track_Popularity':track_popularity, 'Track_URL':track_url, 'Album_ID':album_id}
        track_list.append(track_dict)
    return track_list
    
    
def lambda_handler(event, context):
    
    s3 = boto3.client("s3")
    
    Bucket = 'spotify-etl-project-gopi'
    Key = "raw_data/to_processed/"
    
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        track_list = track(data)
        
        album_df = pd.DataFrame(album_list)
        album_df = album_df.drop_duplicates(subset='Album_ID')
        album_df['Release_Date'] = pd.to_datetime(album_df['Release_Date'])
        
        artist_df = pd.DataFrame(artist_list)
        artist_df = artist_df.drop_duplicates(subset='Artist_ID')
        
        track_df = pd.DataFrame(track_list)
        track_df = track_df.drop_duplicates(subset='Track_ID')
        
        songs_key = "transformed_data/track_data/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer=StringIO()
        track_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])    
        s3_resource.Object(Bucket, key).delete()