--Video stats ordered by snapshot date

SELECT [Video ID], [Snapshot Date], Views , Likes, Comments FROM [Youtubers_WH].[dbo].[videoSnapshots_gold] 
ORDER BY [Video ID], [Snapshot Date] DESC

--SELECT COUNT(*) FROM [Youtubers_WH].[dbo].[videoDetails_gold]
--UNION ALL
--SELECT COUNT(*) FROM [Youtubers_WH].[dbo].[videoSnapshots_gold] 