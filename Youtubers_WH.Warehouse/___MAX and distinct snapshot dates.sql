------ MAX and distinct snapshot dates
SELECT max([Snapshot Date]), COUNT(DISTINCT [Snapshot Date]) FROM [Youtubers_WH].[dbo].[channelSnapshot_gold];

SELECT DISTINCT [Snapshot Date] FROM [Youtubers_WH].[dbo].[channelSnapshot_gold] ORDER BY 1 desc