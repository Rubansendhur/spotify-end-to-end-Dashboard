import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time

#set up client credentials
auth_manager = SpotifyClientCredentials(
    client_id= "", # Replace it with your actual credentials
    client_secret=""
)

sp = spotipy.Spotify(auth_manager=auth_manager)

# Function to fetch tracks released in 2024
def fetch_2024_tracks(limit=50, max_tracks=1000):
    """Fetch tracks released in 2024 from Spotify."""
    tracks = []
    offset = 0
    while len(tracks) < max_tracks:
        try:
            results = sp.search(q="year:2024", type="track", limit=limit, offset=offset)
            tracks.extend(results["tracks"]["items"])
            offset += limit
            if len(results["tracks"]["items"]) == 0:
                break
        except Exception as e:
            print(f"Error fetching tracks: {e}")
            break
    return tracks

# Function to fetch audio features with delay
def fetch_audio_features_with_delay(track_ids, batch_size=100):
    """Fetch audio features for a list of track IDs with delay."""
    audio_features = []
    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i:i + batch_size]
        try:
            batch_features = sp.audio_features(batch)
            if batch_features:
                audio_features.extend(batch_features)
            time.sleep(5)  # Delay to prevent rate-limiting
        except Exception as e:
            print(f"Error fetching batch: {e}")
            time.sleep(10)  # Additional delay in case of errors
    return audio_features

# Fetch tracks released in 2024
tracks_2024 = fetch_2024_tracks()

# Extract relevant track data
track_data = []
track_ids = []
for track in tracks_2024:
    release_date = track["album"].get("release_date", "Unknown")
    year, month, day = (release_date.split("-") + [None, None])[:3]
    track_ids.append(track["id"])
    track_data.append({
        "track_id": track["id"],
        "track_name": track["name"],
        "artist(s)_name": ", ".join([artist["name"] for artist in track["artists"]]),
        "artist_count": len(track["artists"]),
        "album_name": track["album"]["name"],
        "release_date": release_date,
        "released_year": year,
        "released_month": month,
        "released_day": day,
        "popularity": track["popularity"],
        "cover_url": track["album"]["images"][0]["url"] if track["album"]["images"] else None
    })

# Fetch audio features for the tracks
audio_features = fetch_audio_features_with_delay(track_ids)

# Combine track data with audio features
for track, features in zip(track_data, audio_features):
    if features:
        track.update({
            "bpm": features.get("tempo"),
            "key": features.get("key"),
            "mode": features.get("mode"),
            "danceability_%": features.get("danceability", 0) * 100,
            "valence_%": features.get("valence", 0) * 100,
            "energy_%": features.get("energy", 0) * 100,
            "acousticness_%": features.get("acousticness", 0) * 100,
            "instrumentalness_%": features.get("instrumentalness", 0) * 100,
            "liveness_%": features.get("liveness", 0) * 100,
            "speechiness_%": features.get("speechiness", 0) * 100,
        })

# Convert to DataFrame
df_2024_tracks = pd.DataFrame(track_data)

# Save to CSV
output_file = "spotify_2024_tracks_extended.csv"
df_2024_tracks.to_csv(output_file, index=False)
print(f"Dataset saved to {output_file}")

# Display the dataset
print(df_2024_tracks.head())
