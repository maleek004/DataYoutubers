CREATE OR ALTER PROCEDURE sp_loadCummulativeVideoStats AS
BEGIN
    DECLARE @currentloaddate DATE;
    SET @currentloaddate = (SELECT MAX([Snapshot Date]) FROM [Youtubers_WH].[dbo].[videoSnapshots_gold]);

    TRUNCATE TABLE video_cumm_stats;


    WITH latestVideoDetails AS (
        SELECT  @currentloaddate AS LastestSnapshot 
            ,[Video ID]
            ,[Channel ID]
            ,[Release Date]
            ,[Scheduled Start Time]
            ,Title
            ,[Duration(mins)]
            ,Thumbnail
            ,[URL]
            ,[Views]				AS views_latest
            ,[Likes]					AS likes_latest
            ,[Comments]					AS comments_latest
        FROM	[Youtubers_WH].[dbo].[videoDetails_gold]
    ),
    videostatsPrevDay AS (
        SELECT  [Video ID]
                ,[Views]                    
                ,Likes						
                ,Comments					
        FROM    [Youtubers_WH].[dbo].[videoSnapshots_gold]
        WHERE   [Snapshot Date] = CONVERT(date, DATEADD(DAY, -1, @currentloaddate))
    ),
    videostats7Days AS (
        SELECT  [Video ID]
                ,[Views]                    
                ,Likes						
                ,Comments					
        FROM    [Youtubers_WH].[dbo].[videoSnapshots_gold]
        WHERE   [Snapshot Date] = CONVERT(date, DATEADD(DAY, -7, @currentloaddate))
    ),
    videostats30Days AS (
        SELECT  [Video ID]
                ,[Views]                    
                ,Likes						
                ,Comments					
        FROM    [Youtubers_WH].[dbo].[videoSnapshots_gold]
        WHERE   [Snapshot Date] = CONVERT(date, DATEADD(DAY, -30, @currentloaddate))
    )
    INSERT INTO video_cumm_stats
    SELECT lv.*
            ,vp.[Views]								AS viewsPrevDay    
            ,vp.Likes								AS likesPrevDay
            ,vp.Comments							AS commentsPrevDay
            ,v7.[Views]								AS views7Days    
            ,v7.Likes								AS likes7Days
            ,v7.Comments							AS comments7Days
            ,v30.[Views]							AS views30Days   
            ,v30.Likes								AS likes30Days
            ,v30.Comments							AS comments30Days
            ---- DElTAS 
            ,lv.views_latest - vp.[Views]			AS delta_views_prevDay
            ,lv.likes_latest - vp.[Likes]			AS delta_likes_prevDay
            ,lv.comments_latest - vp.[Comments]     AS delta_comments_prevDay
            ,lv.views_latest - v7.[Views]			AS delta_views_7Days
            ,lv.likes_latest - v7.[Likes]			AS delta_likes_7Days
            ,lv.comments_latest - v7.[Comments]     AS delta_comments_7Days
            ,lv.views_latest - v30.[Views]			AS delta_views_30Days
            ,lv.likes_latest - v30.[Likes]			AS delta_likes_30Days
            ,lv.comments_latest - v30.[Comments]    AS delta_comments_30Days
            -- Growth Rates (Prev Day)
            ,CASE WHEN vp.Views > 0 
                THEN CAST((lv.views_latest - vp.Views) * 1.0 / vp.Views AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_views_prevDay
            ,CASE WHEN vp.Likes > 0 
                THEN CAST((lv.likes_latest - vp.Likes) * 1.0 / vp.Likes AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_likes_prevDay
            ,CASE WHEN vp.Comments > 0 
                THEN CAST((lv.comments_latest - vp.Comments) * 1.0 / vp.Comments AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_comments_prevDay
        -- Growth Rates (7 Days)
            ,CASE WHEN v7.Views > 0 
                THEN CAST((lv.views_latest - v7.Views) * 1.0 / v7.Views AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_views_7Days
            ,CASE WHEN v7.Likes > 0 
                THEN CAST((lv.likes_latest - v7.Likes) * 1.0 / v7.Likes AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_likes_7Days
            ,CASE WHEN v7.Comments > 0 
                THEN CAST((lv.comments_latest - v7.Comments) * 1.0 / v7.Comments AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_comments_7Days

        -- Growth Rates (30 Days)
            ,CASE WHEN v30.Views > 0 
                THEN CAST((lv.views_latest - v30.Views) * 1.0 / v30.Views AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_views_30Days

            ,CASE WHEN v30.Likes > 0 
                THEN CAST((lv.likes_latest - v30.Likes) * 1.0 / v30.Likes AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_likes_30Days

            ,CASE WHEN v30.Comments > 0 
                THEN CAST((lv.comments_latest - v30.Comments) * 1.0 / v30.Comments AS DECIMAL(10,4)) 
                ELSE NULL 
            END AS growth_rate_comments_30Days

    FROM latestVideoDetails lv
    LEFT JOIN videostatsPrevDay vp
        ON lv.[Video ID] = vp.[Video ID]
    LEFT JOIN videostats7Days v7
        ON lv.[Video ID] = v7.[Video ID]
    LEFT JOIN videostats30Days v30
        ON lv.[Video ID] = v30.[Video ID]

END


