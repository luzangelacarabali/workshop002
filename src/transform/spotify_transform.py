import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")


def transforming_spotify_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Eliminar valores nulos y duplicados
        df = df.dropna().reset_index(drop=True)
        df = df.drop(columns=["Unnamed: 0"], errors='ignore')
        df = df.drop_duplicates()
        df = df.drop_duplicates(subset=["track_id"]).reset_index(drop=True)

        # Agrupaciones para análisis
        song_artist_grouped_count = (
            df.groupby(["track_name", "artists"])
            .agg(counts=('track_name', 'size'))
            .reset_index()
            .sort_values(by="counts", ascending=False)
        )

        filtered_songs_extrac = df.copy()

        # Mapeo de géneros
        genre_map = {
            "rock": ["alt-rock", "alternative", "hard-rock", "grunge", "psych-rock", "punk-rock", "punk", "rock", "rock-n-roll", "rockabilly", "guitar", "emo"],
            "pop": ["pop", "power-pop", "indie-pop", "pop-film", "synth-pop", "british", "indie"],
            "hip-hop": ["hip-hop", "rap"],
            "electronic": ["edm", "electronic", "electro", "deep-house", "house", "techno", "trance", "dubstep", "minimal-techno", "idm", "detroit-techno", "progressive-house", "club", "dance", "drum-and-bass", "breakbeat", "dub", "trip-hop", "garage"],
            "metal": ["metal", "black-metal", "death-metal", "heavy-metal", "metalcore", "grindcore", "hardcore", "industrial", "goth"],
            "japanese": ["anime", "j-pop", "j-rock", "j-idol", "j-dance"],
            "latin": ["latin", "latino", "samba", "salsa", "reggaeton", "brazil", "forro", "sertanejo", "mpb", "pagode"],
            "classical": ["classical", "opera", "piano"],
            "soul_funk_rnb": ["soul", "funk", "r-n-b", "gospel", "groove"],
            "folk_country": ["folk", "country", "bluegrass", "honky-tonk"],
            "world": ["world-music", "indian", "french", "german", "iranian", "turkish", "malay", "mandopop", "cantopop", "spanish", "swedish"],
            "jazz": ["jazz"],
            "children": ["children", "kids", "disney"],
            "reggae": ["reggae", "dancehall"],
            "misc": ["acoustic", "ambient", "chill", "study", "sleep", "happy", "sad", "party", "comedy", "show-tunes", "romance", "new-age", "singer-songwriter", "tango", "blues", "ska"]
        }

        genre_category_mapping = {
            genre: category
            for category, genres in genre_map.items()
            for genre in genres
        }

        filtered_songs_extrac["track_genre"] = filtered_songs_extrac["track_genre"].map(genre_category_mapping)

        # Eliminar duplicados por columnas relevantes
        cols_to_check = [col for col in filtered_songs_extrac.columns if col not in ["track_id", "album_name"]]
        filtered_songs_extrac = filtered_songs_extrac.drop_duplicates(subset=cols_to_check, keep="first")

        # Mantener canciones únicas con mayor popularidad
        filtered_songs_extrac = (
            filtered_songs_extrac
            .sort_values(by="popularity", ascending=False)
            .groupby(["track_name", "artists"])
            .head(1)
            .reset_index(drop=True)
        )

        df = filtered_songs_extrac.copy()

        # Agregar duración en minutos
        df["duration_min"] = df["duration_ms"] // 60000

        # Clasificación por duración
        df["duration_category"] = "Unknown"
        df.loc[df["duration_ms"] < 150000, "duration_category"] = "Short"
        df.loc[(df["duration_ms"] >= 150000) & (df["duration_ms"] <= 300000), "duration_category"] = "Average"
        df.loc[df["duration_ms"] > 300000, "duration_category"] = "Long"

        # Clasificación de popularidad
        df["popularity_category"] = "Unknown"
        df.loc[df["popularity"] < 40, "popularity_category"] = "Low"
        df.loc[(df["popularity"] >= 40) & (df["popularity"] < 70), "popularity_category"] = "Medium"
        df.loc[df["popularity"] >= 70, "popularity_category"] = "High"

        # Clasificación de energía
        df["energy_category"] = "Unknown"
        df.loc[df["energy"] < 0.4, "energy_category"] = "Low"
        df.loc[(df["energy"] >= 0.4) & (df["energy"] <= 0.7), "energy_category"] = "Medium"
        df.loc[df["energy"] > 0.7, "energy_category"] = "High"

        # Logging al final del proceso
        logging.info(f"The dataframe has been cleaned and transformed. You are left with {df.shape[0]} rows and {df.shape[1]} columns.")
        return df

    except Exception as e:
        logging.error(f"An error has occurred: {e}.")
