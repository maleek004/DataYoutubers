--- my attempt at getting a proper nested cummulative channels table 
SELECT TOP 1 
  c.[Channel ID],
  (
    SELECT 
      s.[Snapshot Date],
      s.[Likes],
      s.[Views],
      s.[Subscribers],
      s.[Videos]
    FROM [Youtubers_WH].[dbo].[channelSnapshot_gold] AS s
    WHERE s.[Channel ID] = c.[Channel ID]
    ORDER BY s.[Snapshot Date]
    FOR JSON PATH
  ) AS snapshot_history
FROM [Youtubers_WH].[dbo].[channelSnapshot_gold] AS c;

SELECT 
      s.[Snapshot Date],
      s.[Likes],
      s.[Views],
      s.[Subscribers],
      s.[Videos]
    FROM [Youtubers_WH].[dbo].[channelSnapshot_gold] AS s
    ORDER BY s.[Snapshot Date]
    FOR JSON PATH