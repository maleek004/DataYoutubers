# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5c8c217e-16f4-4499-b7c3-244fd38f8694",
# META       "default_lakehouse_name": "youtubers_LH",
# META       "default_lakehouse_workspace_id": "ec80c81a-c58e-4508-817c-17f6c253f83a",
# META       "known_lakehouses": [
# META         {
# META           "id": "5c8c217e-16f4-4499-b7c3-244fd38f8694"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

def transform_channels_silver(df):
    """
    Transforms the raw silver layer channels dataset by:
    1. Extracting just the date from 'channel_created_at' timestamp.
    2. Dropping unwanted columns ['playlist_ID', 'Unnamed: 0'].
    3. Adding a 'loadDate' column with the current datetime.
    """
    # Ensure the 'channel_created_at' column exists before transformation
    if 'channel_created_at' in df.columns:
        df['channel_created_at'] = pd.to_datetime(df['channel_created_at'],format='mixed',errors='coerce').dt.date
    else:
        print('channel creation date not found, you might have provided the wrong dataset')

    # Drop unwanted columns if they exist
    cols_to_drop = ['playlist_ID', 'Unnamed: 0']
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')

    #add load date column
    df['loadDate'] = datetime.now().date()
    #df['loadDate'] = datetime.now().date() - timedelta(days=1)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

lang_map = {
    'en': 'English',
    'en-US': 'English (US)',
    'en-GB': 'English (UK)',
    'en-IN': 'English (India)',
    'en-CA': 'English (Canada)',
    'en-AU': 'English (Australia)',
    'en-IE': 'English (Ireland)',
    'es': 'Spanish',
    'es-419': 'Spanish (Latin America)',
    'es-MX': 'Spanish (Mexico)',
    'es-US': 'Spanish (US)',
    'pt': 'Portuguese',
    'pt-BR': 'Portuguese (Brazil)',
    'pt-PT': 'Portuguese (Portugal)',
    'fr': 'French',
    'fr-FR': 'French (France)',
    'fr-CA': 'French (Canada)',
    'de': 'German',
    'zh': 'Chinese',
    'zh-CN': 'Chinese (Simplified)',
    'zh-Hans': 'Chinese (Simplified)',
    'hi': 'Hindi',
    'iw': 'Hebrew',
    'he': 'Hebrew',
    'ta': 'Tamil',
    'te': 'Telugu',
    'ar': 'Arabic',
    'th': 'Thai',
    'ur': 'Urdu',
    'id': 'Indonesian',
    'fil': 'Filipino',
    'zxx': 'No linguistic content',
    'ko': 'Korean',
    'mr': 'Marathi',
    'ja': 'Japanese',
    'vi': 'Vietnamese',
    'el': 'Greek',
    'kn': 'Kannada',
    'pl': 'Polish',
    'dz': 'Dzongkha',
    'hy': 'Armenian',
    'arc': 'Aramaic'
}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def transform_videos_silver(df, lang_map):
    """
    Transforms the silver layer videos dataset by:
    1. Normalizing the language column using the lang_map dictionary.
    2. Extracting only the date from the releaseDate column.
    3. Creating a 'duration_mins' column from the ISO 8601 duration.
    4. Dropping unnecessary columns.
    """
    # Normalize language
    if 'language' in df.columns:
        df['language_clean'] = df['language'].replace(lang_map)

    # Convert releaseDate to just date
    if 'releaseDate' in df.columns:
        df['releaseDate'] = pd.to_datetime(df['releaseDate']).dt.date

    # Convert ISO duration to minutes (rounded up)
    if 'duration' in df.columns:
        df['duration_mins'] = df['duration'].apply(
            lambda x: math.ceil(isodate.parse_duration(x).total_seconds() / 60) if pd.notnull(x) and isinstance(x, str) and x.startswith('PT') else 0
        )

    # Drop unwanted columns
    cols_to_drop = ['videoDescripton', 'Unnamed: 0']
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')

    # Add load date 
    df['loadDate'] = datetime.now().date()
    #df['loadDate'] = datetime.now().date() - timedelta(days=1)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
