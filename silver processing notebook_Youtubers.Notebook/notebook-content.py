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

# MARKDOWN ********************

# ## Load the csv files 


# CELL ********************

from datetime import datetime , timezone , timedelta
import pandas as pd
import isodate
import math
from deltalake import write_deltalake

# Load data into pandas DataFrame from "/lakehouse/default/Files/channels.csv"
channels_df = pd.read_csv("/lakehouse/default/Files/channels.csv")
print(channels_df.shape)
videos_df = pd.read_csv("/lakehouse/default/Files/videos.csv")
print(videos_df.shape)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# datetime.now().date() - timedelta(days=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## explorations
# * lets see if there are videos in other languages in the videos dataset - there are 42 different language short codes in the column that i had to normalize  
# * lets see the values in the videoType column and their  distribution - we had 'none' and 'upcoming' video types 
#     * looks like for videos where the video type is upcoming , those videos are not available to the public as opposed to them being a scheduled live stream ... so to make sure these ones are not part of the gold layer, i will filter out rows where video type is 'upcoming' and scheduledStartTime is 'Not Live Streamed'
#  

# MARKDOWN ********************

# ## Videos dataset transformations 
# #### Channels dataset
# * extract only date from the 'channel_created_at' column
# * drop columns ['playlist_ID', 'Unnamed: 0']
# 
# #### Videos dataset
# * extract only the date value from the releaseDate column 
# * normalize the language column in the 
# * create a duration_minutes column in the videos table 
# * drop columns ['videoDescripton', 'Unnamed: 0']

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

channels_df=transform_channels_silver(channels_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

channels_df.dtypes

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
    5. Standardizing the videoType column into categories:
       - 'none' → 'None'
       - 'upcoming' with null scheduledStartTime → 'Live stream offline'
       - 'upcoming' with scheduledStartTime in the past → 'Pending live stream'
       - 'upcoming' with scheduledStartTime in the future → 'Upcoming'
    """
    # 1. Normalize language
    if 'language' in df.columns:
        df['language_clean'] = df['language'].replace(lang_map)

    # 2. Convert releaseDate to just date
    if 'releaseDate' in df.columns:
        df['releaseDate'] = pd.to_datetime(df['releaseDate'], errors='coerce').dt.date

    # 3. Convert ISO duration to minutes (rounded up)
    if 'duration' in df.columns:
        df['duration_mins'] = df['duration'].apply(
            lambda x: math.ceil(isodate.parse_duration(x).total_seconds() / 60) 
            if isinstance(x, str) and x != 'Unknown' and pd.notnull(x) 
            else 0
        )

    # 4. Drop unwanted columns
    cols_to_drop = ['videoDescripton', 'Unnamed: 0']
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')

    
    
    # 5. Standardize videoType
    if 'videoType' in df.columns:
        # ensure scheduledStartTime is datetime
        if 'scheduledStartTime' in df.columns:
            df['scheduledStartTime'] = pd.to_datetime(df['scheduledStartTime'], errors='coerce')
        now = pd.Timestamp.now(tz="UTC")

        def categorize(row):
            if row['videoType'] == 'none':
                return 'None'
            if row['videoType'] == 'upcoming':
                if pd.isnull(row['scheduledStartTime']):
                    return 'Live stream offline'
                elif row['scheduledStartTime'] < now:
                    return 'Pending live stream'
                else:
                    return 'Upcoming'
            return 'Other'

        df['videoType'] = df.apply(categorize, axis=1)

    # 6. Add load date 
    df['loadDate'] = datetime.now(tz=timezone.utc).date()
    #df['loadDate'] = datetime.now().date() - timedelta(days=1)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

videos_df = transform_videos_silver(videos_df,lang_map)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load Tables

# CELL ********************

## Write channel details table
delta_table_path = f"/lakehouse/default/Tables/silver_channel_details"
write_deltalake(delta_table_path, channels_df, mode='overwrite', schema_mode='merge', engine='rust', storage_options={"allow_unsafe_rename": "true"})
print("channel details table written successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

## Write video details table
delta_table_path = f"/lakehouse/default/Tables/silver_video_details"
write_deltalake(delta_table_path, videos_df, mode='overwrite', schema_mode='merge', engine='rust', storage_options={"allow_unsafe_rename": "true"})
print("video details table written successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
