CREATE   PROCEDURE sp_loadChannelSnapshot AS
BEGIN
INSERT INTO [Youtubers_WH].[dbo].[channelSnapshot_gold] (
    [Snapshot Date], [Channel ID], [Likes], [Views], [Subscribers], [Videos]
)
SELECT 
    c.loadDate,
    c.channel_ID,
    SUM(v.noOfLikes),
    c.no_of_views,
    c.subscribers_count,
    c.no_of_videos
FROM [youtubers_LH].[dbo].[silver_video_details] v
JOIN [youtubers_LH].[dbo].[silver_channel_details] c
    ON v.channelID = c.channel_ID
WHERE NOT EXISTS (
    SELECT 1 FROM [Youtubers_WH].[dbo].[channelSnapshot_gold] snap
    WHERE snap.[Channel ID] = c.channel_ID AND snap.[Snapshot Date] = c.loadDate
)
GROUP BY 
    c.loadDate,
    c.channel_ID,
    c.no_of_views,
    c.subscribers_count,
    c.no_of_videos;
    END;