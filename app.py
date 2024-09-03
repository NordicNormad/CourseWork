import base64
import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
import hashlib
from PIL import Image
from database_queries import (
    get_top_albums_by_track_count,
    calculate_average_track_duration_per_album,
    identify_playlists_with_most_artists,
    get_top_artists_by_track_count,
    calculate_average_tracks_per_playlist,
    get_albums_with_more_than_five_tracks,
    find_playlists_with_multiple_artists,
    get_artist_popularity_by_track_occurrences,
    find_playlists_with_high_avg_track_duration_artists,
    get_total_tracks_in_collaborative_playlists,
    calculate_average_track_duration,
    find_top_artists_with_collaborations,
    get_most_popular_tracks_by_artist,
    find_playlists_with_diverse_artists_and_albums,
    calculate_artist_popularity_index,
)


# B+ Tree Node Class
class BPlusTreeNode:
    def __init__(self, order):
        self.order = order
        self.keys = []
        self.values = [] 
        self.children = []
        self.is_leaf = True

    def insert_non_full(self, key, value):
        i = len(self.keys) - 1
        if self.is_leaf:
            self.keys.append(None)
            self.values.append(None)
            while i >= 0 and key < self.keys[i]:
                self.keys[i + 1] = self.keys[i]
                self.values[i + 1] = self.values[i]
                i -= 1
            self.keys[i + 1] = key
            self.values[i + 1] = value
        else:

            while i >= 0 and key < self.keys[i]:
                i -= 1
            i += 1
            if len(self.children[i].keys) == self.order - 1:
                self.split_child(i)
                if key > self.keys[i]:
                    i += 1
            self.children[i].insert_non_full(key, value)

    def split_child(self, i):
        order = self.order
        new_node = BPlusTreeNode(order)
        node_to_split = self.children[i]
        new_node.is_leaf = node_to_split.is_leaf
        mid = order // 2

        new_node.keys = node_to_split.keys[mid:]
        new_node.values = node_to_split.values[mid:]
        node_to_split.keys = node_to_split.keys[:mid]
        node_to_split.values = node_to_split.values[:mid]

        if not node_to_split.is_leaf:
            new_node.children = node_to_split.children[mid:]
            node_to_split.children = node_to_split.children[:mid]

        self.children.insert(i + 1, new_node)
        self.keys.insert(i, node_to_split.keys.pop())
        self.values.insert(i, node_to_split.values.pop())

    def traverse(self):
        if self.is_leaf:
            for key, value in zip(self.keys, self.values):
                print(f'Artist: {key}, Total Plays: {value}')
        else:
            for i in range(len(self.keys)):
                self.children[i].traverse()
                print(f'Artist: {self.keys[i]}, Total Plays: {self.values[i]}')
            self.children[-1].traverse()


class BPlusTree:
    def __init__(self, order):
        self.root = BPlusTreeNode(order)
        self.order = order

    def insert(self, key, value):
        root = self.root
        if len(root.keys) == self.order - 1:
            new_node = BPlusTreeNode(self.order)
            new_node.is_leaf = False
            new_node.children.append(self.root)
            new_node.split_child(0)
            self.root = new_node

        self.root.insert_non_full(key, value)

    def traverse(self):
        self.root.traverse()

    def get_top_artists(self, top_n=5):
        result = []

        def _gather_artists(node):
            if node.is_leaf:
                result.extend(zip(node.keys, node.values))
            else:
                for i in range(len(node.keys)):
                    _gather_artists(node.children[i])
                    result.append((node.keys[i], node.values[i]))
                _gather_artists(node.children[-1])

        _gather_artists(self.root)
        result.sort(key=lambda x: x[1], reverse=True)
        return result[:top_n]


    def get_artist_popularity(self, artist_name):
        node = self.root
        while node:
            if artist_name in node.keys:
                index = node.keys.index(artist_name)
                return node.values[index]
            elif node.is_leaf:
                return None
            else:
                for i, key in enumerate(node.keys):
                    if artist_name < key:
                        node = node.children[i]
                        break
                else:
                    node = node.children[-1]
        return None
    
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def create_connection():
    connection = None
    try:
        connection = sqlite3.connect('recommendation.db')
        return connection
    except sqlite3.Error as e:
        st.error(f"Error: '{e}'")
        return None

# Function to create necessary tables
def create_users_table():
    """Create necessary tables for the application."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS FavoriteArtists (
            username TEXT,
            artist_name TEXT,
            FOREIGN KEY (username) REFERENCES Users(username)
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Recommendations (
            username TEXT,
            recommendation TEXT,
            date DATE DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (username) REFERENCES Users(username)
        )
        ''')
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


def register_user(username, password):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute('INSERT INTO Users (username, password) VALUES (?, ?)', (username, hash_password(password)))
            connection.commit()
        except sqlite3.IntegrityError:
            st.error("Username already taken")
        finally:
            cursor.close()
            connection.close()

def login_user(username, password):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM Users WHERE username = ? AND password = ?', (username, hash_password(password)))
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result is not None
    return False


def get_favorite_artists(username):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute('SELECT artist_name FROM FavoriteArtists WHERE username = ?', (username,))
        artists = cursor.fetchall()
        cursor.close()
        connection.close()
        return [artist[0] for artist in artists]
    return []


def add_favorite_artist(username, artist_name):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute('INSERT INTO FavoriteArtists (username, artist_name) VALUES (?, ?)', (username, artist_name))
            connection.commit()
        except sqlite3.IntegrityError:
            st.error("This artist is already in your favorites.")
        finally:
            cursor.close()
            connection.close()

def get_recent_recommendations(username):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute('SELECT recommendation FROM Recommendations WHERE username = ? ORDER BY date DESC LIMIT 5', (username,))
        recs = cursor.fetchall()
        cursor.close()
        connection.close()
        return [rec[0] for rec in recs]
    return []


def build_artist_popularity_index():
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute('''
        SELECT artist_name, COUNT(*) as track_count
        FROM Tracks
        GROUP BY artist_name
    ''')
    artists_data = cursor.fetchall()
    connection.close()

    bptree = BPlusTree(order=4)

    for artist in artists_data:
        if artist[1] > 0:  
            bptree.insert(artist[0], artist[1])

    return bptree

bptree = build_artist_popularity_index()

def get_tracks_for_favorite_artists(favorite_artists):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        query = '''
            SELECT artist_name, track_name, duration_ms, playlist_id
            FROM Tracks
            WHERE artist_name IN ({})
        '''.format(','.join('?' for _ in favorite_artists))
        cursor.execute(query, favorite_artists)
        tracks = cursor.fetchall()
        cursor.close()
        connection.close()
        return pd.DataFrame(tracks, columns=['Artist', 'Track', 'Duration', 'PlaylistID'])
    return pd.DataFrame(columns=['Artist', 'Track', 'Duration', 'PlaylistID'])


def suggest_new_artists(favorite_artists):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        query = '''
            SELECT DISTINCT t2.artist_name, COUNT(*) as artist_count
            FROM Tracks t1
            JOIN Tracks t2 ON t1.playlist_id = t2.playlist_id
            WHERE t1.artist_name IN ({}) AND t2.artist_name NOT IN ({})
            GROUP BY t2.artist_name
            ORDER BY artist_count DESC
            LIMIT 10
        '''.format(','.join('?' for _ in favorite_artists), ','.join('?' for _ in favorite_artists))
        cursor.execute(query, favorite_artists + favorite_artists)
        suggested_artists = cursor.fetchall()
        cursor.close()
        connection.close()
        return pd.DataFrame(suggested_artists, columns=['Artist', 'Artist Count'])
    return pd.DataFrame(columns=['Artist', 'Artist Count'])


def get_recommended_tracks(favorite_artists):
    """Fetch recommended tracks and return a plot based on favorite artists' co-occurrence in playlists."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        placeholders = ', '.join(['?'] * len(favorite_artists))

        try:
            query = f"""
                SELECT DISTINCT t2.track_name, t2.artist_name, COUNT(*) AS appearance_count
                FROM Tracks t1
                INNER JOIN Tracks t2 ON t1.playlist_id = t2.playlist_id
                WHERE t1.artist_name IN ({placeholders}) AND t2.artist_name NOT IN ({placeholders})
                GROUP BY t2.track_name, t2.artist_name
                ORDER BY appearance_count DESC
                LIMIT 10;
            """
            cursor.execute(query, favorite_artists + favorite_artists)
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=['Track Name', 'Artist', 'Appearances'])
                fig = px.bar(df, x='Track Name', y='Appearances', color='Artist', title="Recommended Tracks Based on Co-occurrences")
                return fig
            else:
                print("No recommended tracks found.")
        finally:
            cursor.close()
            connection.close()
    else:
        print("Failed to connect to the database.")
    return None

def search_albums_and_tracks_by_artist(artist_name):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT t.album_name, t.track_name
                FROM Tracks t
                WHERE t.artist_name LIKE ?
                ORDER BY t.album_name, t.track_name;
            """, ('%' + artist_name + '%',))
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=['Album', 'Track'])
                return df
            else:
                st.write("No albums or tracks found for the specified artist.")
        finally:
            cursor.close()
            connection.close()
    return pd.DataFrame(columns=['Album', 'Track'])

# Initialize the database 
create_users_table()

# Helper function for background
def set_background_image(image_path):
    with open(image_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode()
    
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("data:image/jpeg;base64,{encoded_string}");
            background-size: cover;
            background-repeat: no-repeat;
            background-position: center;
        }}
        .sidebar .sidebar-content {{
            background-color: rgba(51, 51, 51, 0.8); /* Semi-transparent dark mode for sidebar */
            color: white; /* White text for sidebar */
        }}
        .stButton>button {{
            background-color: #A0522D;
            color: white;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

# Set the background image
set_background_image("pexels-nickcollins-1293120.jpg")

st.markdown(
    """
    <style>
    .stApp {
        /* Assume background image is already set in the function, ensure text is readable */
        color: #FFFFFF;  /* White text for main screen for contrast against orange */
    }
    .sidebar .sidebar-content {
        background-color: rgba(51, 51, 51, 0.8); /* Semi-transparent dark sidebar */
        color: #FFFFFF; /* White text for sidebar for good contrast */
    }
    .sidebar .stButton>button {
        background-color: #FFFFFF; /* White color for sidebar buttons */
        color: #212121; /* Dark text for good contrast on white */
        border: none;
        border-radius: 8px;
    }
    .sidebar .stButton>button:hover {
        background-color: #F0F0F0; /* Light gray on hover */
        color: #212121; /* Dark text on hover for good contrast */
    }
    .stButton>button {
        background-color: #36454F; /* Charcoal button on main screen */
        color: #212121; /* Dark text for main screen buttons */
        border-radius: 8px;
    }
    .stButton>button:hover {
        background-color: #F0F0F0; /* Light gray on hover */
        color: #212121; /* Dark text on hover for good contrast */
    }
    h1, h2, h3, h4, h5, h6, p, div, span, .stMarkdown, .stText {
        color: #FFFFFF;  /* White text for all main screen elements for contrast */
    }
    </style>
    """,
    unsafe_allow_html=True
)



###### Streamlit App Interface ########
st.title("🍂 Groove Guide")

# Check if user is logged in from session state
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None

# Redirect to login page if not authenticated
if not st.session_state.authenticated:
    page = "Login/Register"
else:
    page = "Profile"

# Navigation 
if st.session_state.authenticated:
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Profile", "Recommendations", "Search","Database Queries"], index=0)

# Login Page
if page == "Login/Register":
    st.subheader("Login or Register")
    auth_choice = st.radio("Choose an option", ["Login", "Register"])
    
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    
    if auth_choice == "Login":
        if st.button("Login"):
            if login_user(username, password):
                st.session_state.authenticated = True
                st.session_state.username = username
                st.success("Logged in successfully!")
                page = "Profile"  # For redirection
            else:
                st.error("Invalid username or password")
    
    elif auth_choice == "Register":
        if st.button("Register"):
            try:
                register_user(username, password)
                st.success("Account created successfully!")
            except Exception as e:
                st.error(f"Error: {e}")

# Recommendations Page
elif page == "Recommendations":
    if st.session_state.authenticated:
        st.subheader("Top Track and Artist Recommendations by Artist")
        artist_name = st.text_input("Enter Artist Name")
        if st.button("Get Recommendations"):
            connection = create_connection()
            if connection:
                cursor = connection.cursor()
                
                # Recommend top tracks by the artist themselves
                cursor.execute("""
                    SELECT track_name, COUNT(*) as track_count
                    FROM Tracks 
                    WHERE artist_name = ?
                    GROUP BY track_name
                    ORDER BY track_count DESC
                    LIMIT 5
                """, (artist_name,))
                tracks = cursor.fetchall()
                
                if tracks:
                    st.write(f"### Top Tracks by {artist_name}:")
                    for track in tracks:
                        st.write(f"{track[0]}")
                else:
                    st.write("No tracks found for this artist.")
                
                # Recommend other artists based on playlists with the input artist
                cursor.execute("""
                    SELECT DISTINCT t2.artist_name, COUNT(*) as artist_count
                    FROM Tracks t1
                    JOIN Tracks t2 ON t1.playlist_id = t2.playlist_id
                    WHERE t1.artist_name = ? AND t2.artist_name != ?
                    GROUP BY t2.artist_name
                    ORDER BY artist_count DESC
                    LIMIT 5
                """, (artist_name, artist_name))
                artists = cursor.fetchall()

                if artists:
                    st.write(f"### Recommended Artists with {artist_name}:")
                    for artist in artists:
                        st.write(f"{artist[0]} - {artist[1]} appearances")
                else:
                    st.write("No recommended artists found.")

                cursor.close()
                connection.close()
    else:
        st.error("Please log in to access recommendations.")

# Profile Page
elif page == "Profile":
        if st.session_state.authenticated:
            st.subheader("Your Profile")
            st.write(f"Logged in as: {st.session_state.username}")


            st.subheader("Your Favorite Artists")
            # Add favorite artist
            new_artist = st.text_input("Add a favorite artist:")
            if st.button("Add Artist"):
                if new_artist:
                    add_favorite_artist(st.session_state.username, new_artist)
                    st.success(f"Added {new_artist} to your favorite artists!")
                else:
                    st.error("Please enter an artist name.")

            favorite_artists = get_favorite_artists(st.session_state.username)
            if favorite_artists:
                for artist in favorite_artists:
                    st.write(f"- {artist}")

                tracks_df = get_tracks_for_favorite_artists(favorite_artists)
                tracks_df['Duration'] = tracks_df['Duration'] / 60000  # Convert ms to minutes

                if not tracks_df.empty:
                    suggested_artists_df = suggest_new_artists(favorite_artists)

                    # Plot: Suggested New Artists
                    suggested_artists_fig = px.bar(
                        suggested_artists_df,
                        x='Artist',
                        y='Artist Count',
                        title='Suggested New Artists Based on Favorite Artists',
                        labels={'Artist Count': 'Number of Collaborations'},
                        color='Artist'
                    )

                    # Plot: Track Duration Distribution
                    duration_hist_fig = px.histogram(
                        tracks_df,
                        x='Duration',
                        nbins=20,
                        title='Track Duration Distribution for Favorite Artists (Minutes)',
                        labels={'Duration': 'Duration (Minutes)'},
                        color='Artist'
                    )

                    # Plot: Popularity Index of Favorite Artists
                    popularity_indices = [bptree.get_artist_popularity(artist) for artist in favorite_artists]
                    popularity_df = pd.DataFrame({
                        'Artist': favorite_artists,
                        'Popularity Index': popularity_indices
                    })
                    popularity_fig = px.bar(
                        popularity_df,
                        x='Artist',
                        y='Popularity Index',
                        title='Popularity Index of Your Favorite Artists',
                        labels={'Popularity Index': 'Track Count'},
                        color='Artist'
                    )

                    # Plot: Top Recommended Tracks from Other Artists
                    recommended_tracks_fig = get_recommended_tracks(favorite_artists)  # Assumes function exists


                    col1, col2 = st.columns(2)
                    with col1:
                        st.plotly_chart(suggested_artists_fig, use_container_width=True)
                        st.plotly_chart(popularity_fig, use_container_width=True)
                    with col2:
                        st.plotly_chart(duration_hist_fig, use_container_width=True)
                        st.plotly_chart(recommended_tracks_fig, use_container_width=True)

                else:
                    st.write("No tracks found for your favorite artists.")

            else:
                st.write("You haven't added any favorite artists yet!")

            if st.button("Logout"):
                st.session_state.authenticated = False
                st.session_state.username = None
                st.success("Logged out successfully!")
        else:
            st.error("Please log in to view your profile.")

# Search page
elif page == "Search":
        st.title("Search Albums and Tracks by Artist")

        # Search for artist
        search_query = st.text_input("Enter Artist Name")

        if st.button("Search"):
            if search_query:
                results_df = search_albums_and_tracks_by_artist(search_query)
                
                if not results_df.empty:
                    st.write(f"### Albums and Tracks for Artist: '{search_query}'")
                    for album in results_df['Album'].unique():
                        st.write(f"**Album: {album}**")
                        tracks = results_df[results_df['Album'] == album]['Track'].tolist()
                        for track in tracks:
                            st.write(f"- {track}")
                    

                    popularity_index = bptree.get_artist_popularity(search_query)
                    if popularity_index is not None:
                        st.write(f"### Popularity Index for '{search_query}': {popularity_index}")
                    else:
                        st.write("Popularity Index: Not available for the specified artist.")
                else:
                    st.write("No results found.")
            else:
                st.error("Please enter an artist name.")


# Database Queries Page
if page == "Database Queries":
    st.sidebar.subheader("Database Queries")
    query_page = st.sidebar.radio("Select Query", [
        "Top Albums by Track Count",
        "Average Track Duration per Album",
        "Playlists with Most Artists",
        "Top Artists by Track Count",
        "Average Tracks per Playlist",
        "Albums with More Than Five Tracks",
        "Playlists with Multiple Artists",
        "Artist Popularity by Track Occurrences",
        "High avg Track duration",
        "Tracks in Collaborative Playlists",
        "Average Track Duration",
        "Top Artists with Collaborations",
        "Most Popular Tracks by Artist",
        "PLaylists with diverse artists",
        "Artist Popularity Index"
    ])

    def display_results(title, results):
        st.subheader(title)
        if results:
            for result in results:
                st.write(result)
        else:
            st.write("No data available.")

    if query_page == "Top Albums by Track Count":
        results = get_top_albums_by_track_count()
        display_results("Top 5 Albums by Track Count", results)

    elif query_page == "Average Track Duration per Album":
        results = calculate_average_track_duration_per_album()
        display_results("Average Track Duration per Album", results)

    elif query_page == "Playlists with Most Artists":
        results = identify_playlists_with_most_artists()
        display_results("Playlists with Most Artists", results)

    elif query_page == "Top Artists by Track Count":
        results = get_top_artists_by_track_count()
        display_results("Top 5 Artists by Track Count", results)

    elif query_page == "Average Tracks per Playlist":
        avg_tracks = calculate_average_tracks_per_playlist()
        st.subheader("Average Number of Tracks per Playlist")
        st.write(avg_tracks)

    elif query_page == "Albums with More Than Five Tracks":
        results = get_albums_with_more_than_five_tracks()
        display_results("Albums with More Than Five Tracks", results)

    elif query_page == "Playlists with Multiple Artists":
        results = find_playlists_with_multiple_artists()
        display_results("Playlists with Multiple Artists", results)

    elif query_page == "Artist Popularity by Track Occurrences":
        results = get_artist_popularity_by_track_occurrences()
        display_results("Artist Popularity by Track Occurrences", results)

    elif query_page == "High avg Track duration":
        results = find_playlists_with_high_avg_track_duration_artists()
        display_results("High Track duration Playlists", results)

    elif query_page == "Tracks in Collaborative Playlists":
        total_tracks = get_total_tracks_in_collaborative_playlists()
        st.subheader("Total Number of Tracks in Collaborative Playlists")
        st.write(total_tracks)

    elif query_page == "Average Track Duration":
        results = calculate_average_track_duration()
        display_results("Average Track Duration for Artists with More Than 10 Tracks", results)

    elif query_page == "Top Artists with Collaborations":
        results = find_top_artists_with_collaborations()
        display_results("Top Artists with Most Collaborations", results)

    elif query_page == "Most Popular Tracks by Artist":
        results = get_most_popular_tracks_by_artist()
        display_results("Most Popular Tracks by Artist", results)

    elif query_page == "PLaylists with diverse artists":
        results = find_playlists_with_diverse_artists_and_albums()
        display_results("PLaylists with diverse artists", results)

    elif query_page == "Artist Popularity Index":
        results = calculate_artist_popularity_index()
        display_results("Artist Popularity Index", results)
