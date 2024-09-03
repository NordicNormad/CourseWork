import json
import sqlite3
from datetime import datetime
import glob

def create_connection():
    connection = None
    try:
        connection = sqlite3.connect('recommendation.db')
        return connection
    except sqlite3.Error as e:
        print(f"Error: '{e}'")
        return None

def create_tables():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Playlists (
            pid INTEGER PRIMARY KEY,
            name TEXT,
            collaborative BOOLEAN,
            modified_at DATETIME,
            num_tracks INTEGER,
            num_albums INTEGER,
            num_followers INTEGER
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Tracks (
            track_uri TEXT PRIMARY KEY,
            playlist_id INTEGER,
            pos INTEGER,
            track_name TEXT,
            artist_name TEXT,
            artist_uri TEXT,
            album_uri TEXT,
            album_name TEXT,
            duration_ms INTEGER,
            FOREIGN KEY (playlist_id) REFERENCES Playlists(pid)
        )
        ''')
        
        connection.commit()
        cursor.close()
        connection.close()

def insert_data(connection, combined_data):
    cursor = connection.cursor()
    
    for playlist in combined_data:
        playlist_query = '''
        INSERT OR IGNORE INTO Playlists (pid, name, collaborative, modified_at, num_tracks, num_albums, num_followers)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        playlist_values = (
            playlist["pid"],
            playlist["name"],
            playlist["collaborative"] == "true",
            datetime.fromtimestamp(playlist["modified_at"]),
            playlist["num_tracks"],
            playlist["num_albums"],
            playlist["num_followers"]
        )
        cursor.execute(playlist_query, playlist_values)
        
        for track in playlist["tracks"]:
            track_query = '''
            INSERT OR IGNORE INTO Tracks (track_uri, playlist_id, pos, track_name, artist_name, artist_uri, album_uri, album_name, duration_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            track_values = (
                track["track_uri"],
                playlist["pid"],
                track["pos"],
                track["track_name"],
                track["artist_name"],
                track["artist_uri"],
                track["album_uri"],
                track["album_name"],
                track["duration_ms"]
            )
            cursor.execute(track_query, track_values)
    
    connection.commit()
    cursor.close()
    connection.close()

def main():
    create_tables()

    combined_data = []
    playlistCount = 0
    #Loads data from json files
    for filename in glob.glob('data/*.json'):
        print(playlistCount)
        playlistCount+=1
        with open(filename, 'r') as f:
            data = json.load(f)
            combined_data.extend(data['playlists'])  # Merge playlists

    # Insert combined data into the database
    connection = create_connection()
    if connection:
        insert_data(connection, combined_data)

if __name__ == "__main__":
    main()
