/*SELECT   cd.[Channel ID]  					AS id
		,cd.[Channel Name] 					AS name
		,cd.[Thumbnail URL] 				AS thumbnail_url
		,DATEDIFF(month, cd.[Created At], CONVERT(date, GETDATE())) AS age_months
		,cs.Videos 							AS total_videos 
		,cs.Subscribers 					AS subscribers
		,cs.Views 							AS  total_views
		,cs.Likes 							AS likes
		,CASE WHEN DATEDIFF(month, cd.[Created At], CONVERT(date, GETDATE()))   < 12 THEN 'Less than a year old'
			 WHEN DATEDIFF(month, cd.[Created At], CONVERT(date, GETDATE()))   <= 24 THEN 'Between 1 and 2 years old'
			 WHEN DATEDIFF(month, cd.[Created At], CONVERT(date, GETDATE()))	<= 36 THEN 'Between 2 and 3 years old' 
			 WHEN DATEDIFF(month, cd.[Created At], CONVERT(date, GETDATE()))	<= 48 THEN 'Between 3 and 4 years old'
			 WHEN DATEDIFF(month, cd.[Created At], CONVERT(date, GETDATE()))	<= 36 THEN 'Between 4 and 5 years old'
			 ELSE 'Above 5 years old' 		
			 END AS category

FROM  [Youtubers_WH].[dbo].[channelDetails_gold] cd 
LEFT JOIN [Youtubers_WH].[dbo].[channelSnapshot_gold] cs
	ON cd.[Channel ID] = cs.[Channel ID]
WHERE cd.[Is Current] = 1 AND cs.[Snapshot Date] = CONVERT(date, GETDATE())
*/
 -- This is my first attempt at computing the cummulative data as a view , but i couldnt 
 -- use a variable to store the current date so i opted for stored procedure + table


CREATE OR ALTER VIEW [dbo].[vw_ChannelPerformance] AS
WITH CurrentChannels AS (
    SELECT 
        [Channel ID] AS channel_id,
        [Channel Name] AS channel_name,
        [Thumbnail URL] AS thumbnail_url,
        [Created At] AS created_at,
        COALESCE([Channel Description], 'NO Channel DESCRIPTION' )AS channel_description
    FROM [Youtubers_WH].[dbo].[channelDetails_gold]
    WHERE [Is Current] = 1
),
LatestSnapshot AS (
    SELECT *
    FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]
    WHERE [Snapshot Date] = CONVERT(date, GETDATE())
),
Snapshot_Yesterday AS (
    SELECT *
    FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]
    WHERE [Snapshot Date] = CONVERT(date, DATEADD(DAY, -1, GETDATE()))
),
Snapshot_7Days AS (
    SELECT *
    FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]
    WHERE [Snapshot Date] = CONVERT(date, DATEADD(DAY, -7, GETDATE()))
),
Snapshot_30Days AS (
    SELECT *
    FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]
    WHERE [Snapshot Date] = CONVERT(date, DATEADD(DAY, -30, GETDATE()))
),
Final AS (
    SELECT
        c.channel_id,
        c.channel_name,
        c.channel_description,
        c.thumbnail_url,

        DATEDIFF(MONTH, c.created_at, GETDATE()) AS age_months,

        -- Latest snapshot metrics
        l.Videos AS total_videos,
        l.Subscribers AS subscribers,
        l.Views AS total_views,
        l.Likes AS likes,

        -- Yesterday snapshot
        y.Videos AS total_videos_yesterday,
        y.Subscribers AS subscribers_yesterday,
        y.Views AS total_views_yesterday,
        y.Likes AS likes_yesterday,

        -- 7-day snapshot
        d7.Videos AS total_videos_7days,
        d7.Subscribers AS subscribers_7days,
        d7.Views AS total_views_7days,
        d7.Likes AS likes_7days,

        -- 30-day snapshot
        d30.Videos AS total_videos_30days,
        d30.Subscribers AS subscribers_30days,
        d30.Views AS total_views_30days,
        d30.Likes AS likes_30days,

        -- Deltas
        l.Videos - y.Videos AS delta_videos_yesterday,
        l.Subscribers - y.Subscribers AS delta_subscribers_yesterday,
        l.Views - y.Views AS delta_views_yesterday,
        l.Likes - y.Likes AS delta_likes_yesterday,

        l.Videos - d7.Videos AS delta_videos_7days,
        l.Subscribers - d7.Subscribers AS delta_subscribers_7days,
        l.Views - d7.Views AS delta_views_7days,
        l.Likes - d7.Likes AS delta_likes_7days,

        l.Videos - d30.Videos AS delta_videos_30days,
        l.Subscribers - d30.Subscribers AS delta_subscribers_30days,
        l.Views - d30.Views AS delta_views_30days,
        l.Likes - d30.Likes AS delta_likes_30days,

        -- Growth Rates (Safe division with NULL checks)
        CASE WHEN y.Subscribers > 0 THEN CAST((l.Subscribers - y.Subscribers) * 1.0 / y.Subscribers AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_subscribers_yesterday,
        CASE WHEN y.Views > 0 THEN CAST((l.Views - y.Views) * 1.0 / y.Views AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_views_yesterday,
        CASE WHEN y.Likes > 0 THEN CAST((l.Likes - y.Likes) * 1.0 / y.Likes AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_likes_yesterday,
        CASE WHEN y.Videos > 0 THEN CAST((l.Videos - y.Videos) * 1.0 / y.Videos AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_videos_yesterday,

        CASE WHEN d7.Subscribers > 0 THEN CAST((l.Subscribers - d7.Subscribers) * 1.0 / d7.Subscribers AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_subscribers_7days,
        CASE WHEN d7.Views > 0 THEN CAST((l.Views - d7.Views) * 1.0 / d7.Views AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_views_7days,
        CASE WHEN d7.Likes > 0 THEN CAST((l.Likes - d7.Likes) * 1.0 / d7.Likes AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_likes_7days,
        CASE WHEN d7.Videos > 0 THEN CAST((l.Videos - d7.Videos) * 1.0 / d7.Videos AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_videos_7days,

        CASE WHEN d30.Subscribers > 0 THEN CAST((l.Subscribers - d30.Subscribers) * 1.0 / d30.Subscribers AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_subscribers_30days,
        CASE WHEN d30.Views > 0 THEN CAST((l.Views - d30.Views) * 1.0 / d30.Views AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_views_30days,
        CASE WHEN d30.Likes > 0 THEN CAST((l.Likes - d30.Likes) * 1.0 / d30.Likes AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_likes_30days,
        CASE WHEN d30.Videos > 0 THEN CAST((l.Videos - d30.Videos) * 1.0 / d30.Videos AS DECIMAL(10,4)) ELSE NULL END AS growth_rate_videos_30days,

        -- Age Category (Detailed)
        CASE 
            WHEN DATEDIFF(month, c.created_at, CONVERT(date, GETDATE())) < 12 THEN 'Less than a year old'
            WHEN DATEDIFF(month, c.created_at, CONVERT(date, GETDATE())) <= 24 THEN 'Between 1 and 2 years old'
            WHEN DATEDIFF(month, c.created_at, CONVERT(date, GETDATE())) <= 36 THEN 'Between 2 and 3 years old' 
            WHEN DATEDIFF(month, c.created_at, CONVERT(date, GETDATE())) <= 48 THEN 'Between 3 and 4 years old'
            WHEN DATEDIFF(month, c.created_at, CONVERT(date, GETDATE())) <= 60 THEN 'Between 4 and 5 years old'
            ELSE 'Above 5 years old' 		
        END AS age_category

    FROM CurrentChannels c
    LEFT JOIN LatestSnapshot l ON c.channel_id = l.[Channel ID]
    LEFT JOIN Snapshot_Yesterday y ON c.channel_id = y.[Channel ID]
    LEFT JOIN Snapshot_7Days d7 ON c.channel_id = d7.[Channel ID]
    LEFT JOIN Snapshot_30Days d30 ON c.channel_id = d30.[Channel ID]
)

SELECT * FROM Final;
