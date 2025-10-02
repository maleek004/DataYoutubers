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

# ## LINKS!
# * [Python Data Processing in Microsoft Fabric — End-to-End Transformation and Visualization](https://medium.com/@mariusz_kujawski/python-data-processing-in-microsoft-fabric-end-to-end-transformation-and-visualization-187d1994b555)
# * [How To Mount A Lakehouse and Identify The Mounted Lakehouse in Fabric Notebook](https://fabric.guru/how-to-mount-a-lakehouse-and-identify-the-mounted-lakehouse-in-fabric-notebook)
# *

# CELL ********************

import pandas as pd
from datetime import datetime , timezone , timedelta
import pandas as pd
import isodate
import math
from deltalake import write_deltalake
import builtin.Youtube_creator_utils.silver_utils as silver_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Testing the Youtube trabscribe API 

# CELL ********************

%pip install youtube-transcript-api
from youtube_transcript_api import YouTubeTranscriptApi

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


videos_df[
        (videos_df['videoType'] == 'None') &
        (videos_df['language_clean'].isin([
            'English', 'English (US)', 'English (UK)',
            'English (India)', 'English (Canada)',
            'English (Australia)', 'English (Ireland)'
        ]))
    ][['videoTitle', 'videoID']].sample(10).set_index('videoTitle')['videoID'].to_dict()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import pandas as pd
videos_df = pd.read_csv("/lakehouse/default/Files/videos.csv")
display(videos_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

videos_df.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Standardizing the videoType column
# #### The video type is either 'none' or 'upcoming' from the API
# #### A fraction (22/79) of the 'upcoming' videos have ***'null' scheduled start time***
# #### 'upcoming' videos with ***'null' scheduled start time*** are cases of **'Live stream offline'**
# #### cases where a video is upcoming and the ***scheduled start time is in the past*** , then that's a ***'Pending live stream'***
# #### cases where a video is upcoming and the ***scheduled start time is in the future*** then that's the real upcoming video

# CELL ********************

display(videos_df['videoType'].value_counts().reset_index())
# display(videos_df[''])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

display(videos_df[(videos_df['videoType'] == 'upcoming')])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

display(videos_df[(~videos_df["duration"].fillna("").str.startswith("PT")) & (videos_df.scheduledStartTime.notnull())])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

len(videos_df[videos_df["scheduledStartTime"] == "Not Live Streamed"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

videos_df[(videos_df["scheduledStartTime"] == "Not Live Streamed") & (videos_df.videoType == "upcoming")].reset_index(drop=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Standardizing the duration column 
# #### Almost all  the duration (99.8%) starts with 'PT', the rest either starts with 'PxD' for videos that are days long or 'P0D' for upcoming videos or 'Unknown' when the video key is not found in the API's response

# CELL ********************

import isodate
import math
math.ceil(isodate.parse_duration('P0D').total_seconds() / 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

videos_df[(~videos_df["duration"].fillna("").str.startswith("PT")) & (videos_df["duration"] != 'P0D')]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import duckdb
import pandas as pd
import pyarrow

arrow_df = duckdb.sql("""
    SELECT 
    *
    FROM 
    delta_scan('/lakehouse/default/Tables/silver_video_details') a

""").arrow()

df = arrow_df.to_pandas()
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

video_dict = dict(zip(df["videoID"], df["videoTitle"]))
video_dict


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

len(videoIDs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


videoIDs = df.videoID.to_list()
videoIDs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

video_id = 'YGb9GeRMBg4'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

ytt_api = YouTubeTranscriptApi()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

transcript_list = ytt_api.list(video_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************



raw_transcript = ytt_api.fetch(video_id)
for snippet in raw_transcript:
    print(snippet.text) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Reading delta table from non default lakehouse into python dataframe (without mounting ?) 
# * to_pyarrow_dataset() is necessary when using the Delta Lake Python API because it only exposes data through Arrow datasets. Unlike DuckDB, which can read data and optionally convert to Arrow with .arrow(), the Delta Lake API doesn’t provide direct pandas access — you must first go through Arrow, then convert to pandas or other formats.

# CELL ********************

from deltalake import DeltaTable, write_deltalake
table_path = 'abfss://c06c478b-1e1d-4cf3-9d95-1504e0966197@onelake.dfs.fabric.microsoft.com/cd386b65-9dde-4e31-a0bf-8ae04d99b5a4/Tables/github/repository_activity' 
storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}
dt = DeltaTable(table_path, storage_options=storage_options)
limited_data = dt.to_pyarrow_dataset().to_pandas()
display(limited_data)

# Write data frame to Lakehouse
# write_deltalake(table_path, limited_data, mode='overwrite')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Read delta table (from default lakehouse) into python notebook using DuckDB
# * The .arrow() step lets DuckDB stream results into an Arrow table, which is more efficient for large datasets because Arrow uses a zero-copy columnar format. From there, you can convert to pandas if needed, but this way you avoid unnecessary memory overhead compared to exporting directly to pandas. This step is **not compulsory but good for optimization**

# CELL ********************

import duckdb
import pyarrow

arrow_df = duckdb.sql("""
    SELECT 
    *
    FROM 
    delta_scan('/lakehouse/default/Tables/silver_channel_details') a
    where
    no_of_videos > 250
""").arrow()

df = arrow_df.to_pandas()
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Reading files from default lakehouse into pandas dataframe

# CELL ********************

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

# MARKDOWN ********************

# ### Write pandas dataframe as delta table in lakehouse (mounted or not?)


# CELL ********************

from deltalake import DeltaTable, write_deltalake
table_path = 'abfss://ec80c81a-c58e-4508-817c-17f6c253f83a@onelake.dfs.fabric.microsoft.com/5c8c217e-16f4-4499-b7c3-244fd38f8694/Tables/silver_channel_details' 
storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}
dt = DeltaTable(table_path, storage_options=storage_options)
limited_data = dt.to_pyarrow_dataset().head(1000).to_pandas()
display(limited_data)

# Write data frame to Lakehouse
# write_deltalake(table_path, limited_data, mode='overwrite')

# If the table is too large and might cause an Out of Memory (OOM) error,
# you can try using the code below. However, please note that delta_scan with default lakehouse is currently in preview.
# import duckdb
# display(duckdb.sql("select * from delta_scan('/lakehouse/default/Tables/dbo/bigdeltatable') limit 1000 ").df())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Write a pandas dataframe as parquet file in default lakehouse 

# CELL ********************

df.to_parquet("/lakehouse/default/Files/nbp/flat_price.parquet", index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Read Parquet file as pandas dataframe 

# CELL ********************

df = pd.read_parquet(f"/lakehouse/default/Files/nbp/flat_price.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Save dataframe as file in non default lakehouse

# MARKDOWN ********************

# ### Read delta table into python notebook using polars 

# CELL ********************

import polars as pl
# lazy read for tables above 16GB

# -- abfs_path = "abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables"
# -- pl.scan_delta(f"{abfs_path}/<tablename>" , version=0)


# eager read for smaller tables 

#-- pl.read_delta(f"{abfs_path}/<tablename>" , columns=['col1' , 'col2']).head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Writing a Polars dataframe as delta table in default lakehouse 
# * When you’re using `mode="overwrite"`, then `delta_write_options={"schema_mode": "overwrite"}` is effectively not needed.

# CELL ********************

repository_activity.write_delta(f"{abfs_path}/github/repository_activity", mode="overwrite",delta_write_options={"schema_mode": "overwrite"})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Mounting a lakehouse to a notebook

# CELL ********************

REST_abfss_path = 'abfss://c06c478b-1e1d-4cf3-9d95-1504e0966197@onelake.dfs.fabric.microsoft.com/cd386b65-9dde-4e31-a0bf-8ae04d99b5a4/'
mount_point = "/mnt/REST_BronzeLH/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

notebookutils.fs.mount(REST_abfss_path,mount_point)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Listing all lakehouses mounted (on notebook?)

# CELL ********************

notebookutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Reading data from a mounted lakehouse using local path and pandas 

# CELL ********************

import pandas as pd 
local_path = next((mp["localPath"] for mp in notebookutils.fs.mounts() if mp["mountPoint"] == mount_point), None)
print(local_path)
df = pd.read_csv(f'{local_path}Files/reviewer_data.csv')
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

print(os.path.exists(local_path)) #check if location exists
print(os.listdir(local_path + "/Files")) # for files
print(os.listdir(local_path ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

[x.source for x in notebookutils.fs.mounts()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

[x.source for x in notebookutils.fs.mounts() if x.mountPoint == '/default'].pop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

filesPath = 'abfss://ec80c81a-c58e-4508-817c-17f6c253f83a@onelake.dfs.fabric.microsoft.com/5c8c217e-16f4-4499-b7c3-244fd38f8694/Files'
filesPath.rsplit('/', 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

next((x.source for x in notebookutils.fs.mounts() if x.mountPoint == '/default'), None)


#(x.source for x in notebookutils.fs.mounts() if x.mountPoint == '/default')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Testing sample library 

# CELL ********************

import builtin.Youtube_creator_utils.silver_utils as silver_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/channels.csv"
channels_df = pd.read_csv("/lakehouse/default/Files/channels.csv")
channels_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


channels_cleaned = silver_utils.transform_channels_silver(channels_df)
channels_cleaned.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

videos_df = pd.read_csv("/lakehouse/default/Files/channels.csv")
videos_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

videos_cleaned = silver_utils.transform_videos_silver(videos_df , silver_utils.lang_map)
videos_cleaned.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
