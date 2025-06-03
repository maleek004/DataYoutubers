---- Performs a full load of the videos table everytime it runs 

CREATE   PROCEDURE sp_load_videos_gold
AS
BEGIN
    -- Step 1: Truncate the gold table
    TRUNCATE TABLE dbo.videoDetails_gold;

    -- Step 2: Insert refreshed data from silver layer
    INSERT INTO dbo.videoDetails_gold (
        [Video Key],
        [Video ID],
        [Channel ID],
        [Release Date],
        [Scheduled Start Time],
        [Title],
        [Duration(mins)],
        [Views],
        [Likes],
        [Comments],
        [URL],
        [Thumbnail]
    )
    
SELECT ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) ,
   	videoID ,
	   channelID  ,
	   releasedDate  ,
	   scheduledStartTime , 
	   videoTitle ,  
	   duration_mins  ,
	   noOfViews  ,
	   noOfLikes  ,
	   noOfComments  ,
	   CONCAT('https://www.youtube.com/watch?v=' , videoID) [URL],
	   thumbnailURL  

FROM [youtubers_LH].[dbo].[silver_video_details]
END;