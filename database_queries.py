import sqlite3

def create_connection():
    """Create a connection to the SQLite database."""
    try:
        connection = sqlite3.connect('recommendation.db')
        return connection
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None

def get_top_albums_by_track_count():
    """Find top albums with the most tracks, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT t.album_name, COUNT(t.track_uri) AS track_count
                FROM Tracks t
                JOIN Playlists p ON t.playlist_id = p.pid
                WHERE p.num_followers > 1000
                AND t.album_name IS NOT NULL
                GROUP BY t.album_name
                ORDER BY track_count DESC
                LIMIT 5;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def calculate_average_track_duration_per_album():
    """Calculate average track duration per album, limited to 15.(more than 10 tracks)"""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT t.artist_name, AVG(t.duration_ms) AS avg_duration
                FROM Tracks t
                JOIN (
                    SELECT artist_name
                    FROM Tracks
                    GROUP BY artist_name
                    HAVING COUNT(*) > 10
                ) AS ta ON t.artist_name = ta.artist_name
                JOIN Playlists p ON t.playlist_id = p.pid
                GROUP BY t.artist_name
                ORDER BY avg_duration DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def identify_playlists_with_most_artists():
    """Identify playlists with tracks from the most distinct artists, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT p.name, COUNT(DISTINCT t.artist_name) AS artist_count
                FROM Playlists p
                JOIN Tracks t ON p.pid = t.playlist_id
                GROUP BY p.name
                ORDER BY artist_count DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def get_top_artists_by_track_count():
    """Get top artists with the most tracks, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT t.artist_name, COUNT(t.track_uri) AS track_count
                FROM Tracks t
                JOIN Playlists p ON t.playlist_id = p.pid
                WHERE p.num_followers > 1000
                GROUP BY t.artist_name
                ORDER BY track_count DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def calculate_average_tracks_per_playlist():
    """Calculate the average number of tracks per playlist(atleeast 1 track)."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT AVG(track_count) AS avg_tracks_per_playlist
                FROM (
                    SELECT p.pid, COUNT(t.track_uri) AS track_count
                    FROM Playlists p
                    JOIN Tracks t ON p.pid = t.playlist_id
                    GROUP BY p.pid
                ) AS playlist_track_counts;
            """)
            avg_tracks = cursor.fetchone()
            return avg_tracks[0] if avg_tracks else None
        finally:
            cursor.close()
            connection.close()

def get_albums_with_more_than_five_tracks():
    """Get albums that have more than five tracks, limited to 15.(additional filters)"""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT a.album_name
                FROM (
                    SELECT t.album_name, COUNT(t.track_uri) AS track_count
                    FROM Tracks t
                    JOIN Playlists p ON t.playlist_id = p.pid
                    WHERE p.num_followers > 500
                    GROUP BY t.album_name
                    HAVING COUNT(t.track_uri) > 5
                ) AS a;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def find_playlists_with_multiple_artists():
    """Find playlists that include tracks from multiple artists, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT p.name, COUNT(DISTINCT t.artist_name) AS artist_count
                FROM Playlists p
                JOIN Tracks t ON p.pid = t.playlist_id
                GROUP BY p.name
                HAVING artist_count > 1
                ORDER BY artist_count DESC
                LIMIT 15;

            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def get_artist_popularity_by_track_occurrences():
    """Get artist popularity based on track occurrences, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT t.artist_name, COUNT(t.track_uri) AS occurrence_count
                FROM Tracks t
                JOIN Playlists p ON t.playlist_id = p.pid
                WHERE p.num_followers > 1000
                GROUP BY t.artist_name
                ORDER BY occurrence_count DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def find_playlists_with_high_avg_track_duration_artists():
    """Find playlists with artists having the highest average track durations in popular playlists."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT p.name AS playlist_name, AVG(t.duration_ms) AS avg_duration
                FROM Tracks t
                JOIN Playlists p ON t.playlist_id = p.pid
                JOIN (
                    SELECT artist_name, AVG(duration_ms) AS artist_avg_duration
                    FROM Tracks
                    GROUP BY artist_name
                    HAVING COUNT(track_uri) > 5
                ) AS artist_avg ON t.artist_name = artist_avg.artist_name
                WHERE p.num_followers > 500
                GROUP BY p.name
                ORDER BY avg_duration DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def get_total_tracks_in_collaborative_playlists():
    """Calculate total number of tracks in collaborative playlists.(more than 1000 followers)"""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT SUM(t.track_count) AS total_tracks
                FROM (
                    SELECT playlist_id, COUNT(track_uri) AS track_count
                    FROM Tracks
                    GROUP BY playlist_id
                ) t
                JOIN Playlists p ON t.playlist_id = p.pid
                WHERE p.collaborative = TRUE
                AND p.num_followers > 1000;
            """)
            total_tracks = cursor.fetchone()
            return total_tracks[0] if total_tracks else 0
        finally:
            cursor.close()
            connection.close()

def calculate_average_track_duration():
    """Calculate average track duration for artists with more than 10 tracks, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT artist_name, AVG(duration_ms) AS avg_duration
                FROM Tracks
                WHERE artist_name IN (
                    SELECT artist_name
                    FROM Tracks
                    GROUP BY artist_name
                    HAVING COUNT(*) > 10
                )
                GROUP BY artist_name
                ORDER BY avg_duration DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def find_top_artists_with_collaborations():
    """Find artists with the most collaborations, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT t1.artist_name, COUNT(DISTINCT t2.artist_name) AS collaboration_count
                FROM Tracks t1
                JOIN Tracks t2 ON t1.playlist_id = t2.playlist_id AND t1.artist_name <> t2.artist_name
                GROUP BY t1.artist_name
                ORDER BY collaboration_count DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def get_most_popular_tracks_by_artist():
    """Get most popular tracks by artist, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT artist_name, track_name, MAX(track_count) AS max_count
                FROM (
                    SELECT artist_name, track_name, COUNT(*) AS track_count
                    FROM Tracks
                    GROUP BY artist_name, track_name
                ) AS artist_tracks
                GROUP BY artist_name, track_name
                ORDER BY max_count DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

def find_playlists_with_diverse_artists_and_albums():
    """Find playlists with the most diverse combination of artists and albums."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            # SQL query to find playlists with diverse combinations of artists and albums
            cursor.execute("""
                SELECT p.name AS playlist_name,
                       COUNT(DISTINCT t.artist_name) AS artist_count,
                       COUNT(DISTINCT t.album_name) AS album_count
                FROM Playlists p
                JOIN Tracks t ON p.pid = t.playlist_id
                GROUP BY p.name
                ORDER BY (artist_count + album_count) DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return None
        finally:
            cursor.close()
            connection.close()



def calculate_artist_popularity_index():
    """Calculate artist popularity index based on tracks and followers, limited to 15."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT artist_name, 
                       SUM(track_count * 0.7 + num_followers * 0.3) AS popularity_index
                FROM (
                    SELECT t.artist_name, COUNT(*) AS track_count, p.num_followers
                    FROM Tracks t
                    JOIN Playlists p ON t.playlist_id = p.pid
                    GROUP BY t.artist_name, p.num_followers
                ) AS artist_popularity
                GROUP BY artist_name
                ORDER BY popularity_index DESC
                LIMIT 15;
            """)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()
