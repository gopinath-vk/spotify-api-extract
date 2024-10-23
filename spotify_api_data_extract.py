import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    auth_manager = SpotifyClientCredentials(client_id = client_id, client_secret = client_secret)
    spotify = spotipy.Spotify(client_credentials_manager=auth_manager)
    
    playlist_url = 'https://open.spotify.com/playlist/37i9dQZF1DX4Im4BTs2WMg'
    req_uri = playlist_url.split('/')[4]
    
    df = spotify.playlist_tracks(req_uri)
    
    client = boto3.client("s3")
    
    filename = 'spotify_raw_' + str(datetime.now()) + '.json'
    
    client.put_object(
        Bucket = 'spotify-etl-project-gopi',
        Key = 'raw_data/to_processed/' + filename,
        Body = json.dumps(df)
        )