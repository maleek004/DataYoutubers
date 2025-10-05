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

!pip install google-api-python-client > /dev/null 2>&1
import pandas as pd
from googleapiclient.discovery import build 
from IPython.display import JSON 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## youtube API client set up 

# CELL ********************


api_key = notebookutils.credentials.getSecret('https://youtuberskvault.vault.azure.net/','YoutubeAPIv3')
api_service_name = "youtube"
api_version = "v3"    
# Get credentials and create an API client
youtube = build(
        api_service_name, api_version, developerKey=api_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Get channel handles

# CELL ********************

conn = notebookutils.data.connect_to_artifact("Youtubers_WH")
channel_ids = conn.query("SELECT DISTINCT channel_ID FROM metadata.metadata;").channel_ID.tolist()
print(len(channel_ids))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def get_channel_details(youtube, channel_ids):
    """
    This function takes a Youtube API client and a list of channel IDs
    and returns a dataframe containing the channel details.

    Parameters
    ----------
    youtube : object
        The Youtube API client.
    channel_ids : list
        A list of channel IDs.

    Returns
    -------
    channels_df : pandas.DataFrame
        A dataframe containing the channel details.
    """
    channels_dictionary = {
        'channel_name': [],
        'channel_ID': [],
        'channel_description': [],
        'channel_created_at': [],
        'subscribers_count': [],
        'no_of_views': [],
        'no_of_videos': [],
        'thumbnail_URL': [],
        'playlist_ID': [],
        'channel_country':[]
        
    }

    for channelid in channel_ids:
        try:
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channelid
            )
            response = request.execute()

            if response.get("items"):
                item = response["items"][0]

                channels_dictionary['channel_name'].append(item['snippet']['title'])
                channels_dictionary['channel_ID'].append(item['id'])
                channels_dictionary['channel_description'].append(item['snippet'].get('description', ''))
                channels_dictionary['channel_created_at'].append(item['snippet']['publishedAt'])
                channels_dictionary['subscribers_count'].append(int(item['statistics'].get('subscriberCount', 0)))
                channels_dictionary['no_of_views'].append(int(item['statistics'].get('viewCount', 0)))
                channels_dictionary['no_of_videos'].append(int(item['statistics'].get('videoCount', 0)))
                channels_dictionary['thumbnail_URL'].append(item['snippet']['thumbnails']['high']['url'])
                channels_dictionary['playlist_ID'].append(item['contentDetails']['relatedPlaylists']['uploads'])
                channels_dictionary['channel_country'].append(item['snippet'].get('country'))

            else:
                print(f"No channel found for {channelid}")

        except Exception as e:
            print(f"An error occurred for {channelid}: {e}")
    
    channels_df = pd.DataFrame(channels_dictionary)
    return channels_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def get_video_ids(youtube, playlistIDs):
    video_IDs = []

    for playlist_id in playlistIDs:
        next_page_token = None

        while True:
            try:
                request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()

                for item in response.get('items', []):
                    video_id = item['contentDetails']['videoId']
                    if video_id not in video_IDs:
                        video_IDs.append(video_id)

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            except Exception as e:
                print(f"Error fetching videos for playlist {playlist_id}: {e}")
                break  # Exit the loop for this playlist and move on

    print(f"{len(video_IDs)} videos IDs have been added to the list.")
    return video_IDs


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def get_video_details(youtube, videoID):
    """
    This function takes a Youtube API client and a list of video IDs
    and returns a dataframe containing the video details.

    Parameters
    ----------
    youtube : object
        The Youtube API client.
    videoID : list
        A list of video IDs.

    Returns
    -------
    df : pandas.DataFrame
        A dataframe containing the video details.
    """
    if not videoID:
        print("No video IDs provided.")
        return pd.DataFrame()

    video_details_dictionary = {
        'videoID': [],
        'channelID': [],
        'videoTitle': [],
        'videoDescription': [],
        'releasedDate': [],
        'language': [],
        'duration': [],
        'noOfViews': [],
        'noOfLikes': [],
        'noOfComments': [],
        'thumbnailURL': [],
        'videoType': [],
        'scheduledStartTime':[]
    }

    default_thumbnail_url = "https://media.istockphoto.com/id/1409329028/vector/no-picture-available-placeholder-thumbnail-icon-illustration-design.jpg?s=612x612&w=0&k=20&c=_zOuJu755g2eEUioiOUdz_mHKJQJn-tDgIAhQzyeKUQ="

    for i in range(0, len(videoID), 50):
        batch = videoID[i:i+50]  
        video_id_str = ",".join(batch)  

        try:
            request = youtube.videos().list(
                    part="snippet,contentDetails,statistics,liveStreamingDetails",
                    id=video_id_str
                )
            response = request.execute()

            for video in response.get('items', []):  
                video_details_dictionary['videoID'].append(video['id'])
                video_details_dictionary['channelID'].append(video['snippet']['channelId'])
                video_details_dictionary['videoTitle'].append(video['snippet']['title'])
                video_details_dictionary['videoDescription'].append(video['snippet'].get('description', 'No description'))
                video_details_dictionary['releasedDate'].append(video['snippet'].get('publishedAt', 'Unknown'))
                video_details_dictionary['language'].append(video['snippet'].get('defaultAudioLanguage', 'Unknown'))

                    # ✅ FIXED: Handle missing 'duration' field safely
                video_details_dictionary['duration'].append(video.get('contentDetails', {}).get('duration', 'Unknown'))

                video_details_dictionary['noOfViews'].append(video['statistics'].get('viewCount', 0))
                video_details_dictionary['noOfLikes'].append(video['statistics'].get('likeCount', 0))
                video_details_dictionary['noOfComments'].append(video['statistics'].get('commentCount', 0))

                    # Handle missing thumbnails safely
                thumbnail = video['snippet'].get('thumbnails', {}).get('standard', {}).get('url', default_thumbnail_url)
                video_details_dictionary['thumbnailURL'].append(thumbnail)

                video_details_dictionary['videoType'].append(video['snippet'].get('liveBroadcastContent', 'Not Live Streamed'))
                video_details_dictionary['scheduledStartTime'].append(video.get('liveStreamingDetails', {}).get('scheduledStartTime', ''))

            print(f"Processed {len(batch)} videos from batch {i//50 + 1}")

        except Exception as e:
           print(f"An error occurred in batch {i//50 + 1}: {e}")

    # 🔹 Ensure all lists have the same length before creating DataFrame
    min_length = min(len(lst) for lst in video_details_dictionary.values())
    for key in video_details_dictionary:
        video_details_dictionary[key] = video_details_dictionary[key][:min_length]  

    df = pd.DataFrame(video_details_dictionary)
    print(f"Total videos retrieved: {len(df)}")
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

channels_df= get_channel_details(youtube,channel_ids)
channels_df.head()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

playlistIDs = list(channels_df.playlist_ID)
print(len(playlistIDs))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

video_IDs = get_video_ids(youtube, playlistIDs)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

videos_df = get_video_details(youtube,video_IDs)
videos_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Save datasets to the raw layer: Lakehouse file section

# CELL ********************

channels_df.to_csv('/lakehouse/default/Files/channels.csv')
videos_df.to_csv('/lakehouse/default/Files/videos.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
