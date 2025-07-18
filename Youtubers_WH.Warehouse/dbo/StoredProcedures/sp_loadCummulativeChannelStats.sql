CREATE   PROCEDURE sp_loadCummulativeChannelStats AS

BEGIN
    DECLARE @maxdate DATE;

    SET @maxdate = (SELECT MAX([Snapshot Date]) FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]);


    TRUNCATE TABLE dbo.channels_cumm_stats;

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
        WHERE [Snapshot Date] = @maxdate
    ),
    Snapshot_prevday AS (
        SELECT *
        FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]
        WHERE [Snapshot Date] = CONVERT(date, DATEADD(DAY, -1, @maxdate))
    ),
    Snapshot_7Days AS (
        SELECT *
        FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]
        WHERE [Snapshot Date] = CONVERT(date, DATEADD(DAY, -7, @maxdate))
    ),
    Snapshot_30Days AS (
        SELECT *
        FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]
        WHERE [Snapshot Date] = CONVERT(date, DATEADD(DAY, -30, @maxdate))
    ),
    Final AS (
        SELECT
            @maxdate AS snapshot_date,
            c.channel_id,
            c.channel_name,
            c.channel_description,
            c.thumbnail_url,

            DATEDIFF(MONTH, c.created_at, @maxdate) AS age_months,

            -- Latest snapshot metrics(as at yesterday)
            l.Videos AS latest_videos,
            l.Subscribers AS latest_subscribers,
            l.Views AS latest_views,
            l.Likes AS latest_likes,

            -- snapshot 2 days ago
            y.Videos AS videos_prev_day,
            y.Subscribers AS subscribers_prev_day,
            y.Views AS views_prev_day,
            y.Likes AS likes_prev_day,

            -- snapshot 7 days ago
            d7.Videos AS videos_7days,
            d7.Subscribers AS subscribers_7days,
            d7.Views AS views_7days,
            d7.Likes AS likes_7days,

            -- snapshot 30 days ago
            d30.Videos AS videos_30days,
            d30.Subscribers AS subscribers_30days,
            d30.Views AS views_30days,
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
                WHEN DATEDIFF(month, c.created_at, @maxdate) < 12 THEN 'Less than a year old'
                WHEN DATEDIFF(month, c.created_at, @maxdate) <= 24 THEN 'Between 1 and 2 years old'
                WHEN DATEDIFF(month, c.created_at, @maxdate) <= 36 THEN 'Between 2 and 3 years old' 
                WHEN DATEDIFF(month, c.created_at, @maxdate) <= 48 THEN 'Between 3 and 4 years old'
                WHEN DATEDIFF(month, c.created_at, @maxdate) <= 60 THEN 'Between 4 and 5 years old'
                ELSE 'Above 5 years old' 		
            END AS age_category

        FROM CurrentChannels c
        LEFT JOIN LatestSnapshot l ON c.channel_id = l.[Channel ID]
        LEFT JOIN Snapshot_prevday y ON c.channel_id = y.[Channel ID]
        LEFT JOIN Snapshot_7Days d7 ON c.channel_id = d7.[Channel ID]
        LEFT JOIN Snapshot_30Days d30 ON c.channel_id = d30.[Channel ID]
    )

    INSERT INTO dbo.channels_cumm_stats
    SELECT * FROM Final;
END