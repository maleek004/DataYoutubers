WITH yesterday AS (
SELECT * FROM [Youtubers_WH].[dbo].[channelSnapshot_gold] WHERE [Snapshot Date] = '2025-05-25'
),
today AS (
SELECT * FROM [Youtubers_WH].[dbo].[channelSnapshot_gold] WHERE [Snapshot Date] = '2025-05-26'
)

SELECT t.[Channel ID], t.Likes , y.Likes, t.Views,  y.Views, t.Subscribers , y.Subscribers , t.Videos , y.Videos
FROM today t 
JOIN yesterday y 
    ON t.[Channel ID] = y.[Channel ID]
WHERE t.Views <> y.Views OR t.Likes <> y.Likes OR t.Subscribers <> y.Subscribers OR t.Videos <> y.Videos
