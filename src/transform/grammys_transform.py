# grammy_transform.py

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def standardize_columns(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

def remove_duplicates(df):
    return df.drop_duplicates()

def handle_missing_values(df):
    # Puedes personalizar este comportamiento según tu criterio
    return df.dropna(subset=['year', 'category', 'nominee'])

def infer_decade(df):
    df['decade'] = (df['year'] // 10) * 10
    return df

def infer_genre(category):
    category = category.lower()
    if 'pop' in category:
        return 'Pop'
    elif 'rock' in category:
        return 'Rock'
    elif 'r&b' in category or 'rhythm' in category:
        return 'R&B'
    elif 'rap' in category or 'hip hop' in category:
        return 'Rap/Hip Hop'
    elif 'country' in category:
        return 'Country'
    elif 'jazz' in category:
        return 'Jazz'
    elif 'classical' in category or 'opera' in category:
        return 'Classical'
    elif 'dance' in category or 'electronic' in category:
        return 'Dance/Electronic'
    else:
        return 'Other'

def apply_genre_inference(df):
    df['genre'] = df['category'].apply(infer_genre)
    return df

def convert_dates(df):
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce', utc=True)
    df['year'] = df['published_at'].dt.year.fillna(df['year']).astype('Int64')
    return df

def transform_grammy_data(df):
    df = standardize_columns(df)
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = convert_dates(df)
    df = infer_decade(df)
    df = apply_genre_inference(df)
    return df
