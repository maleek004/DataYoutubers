-- Most viewed video yesterday
SELECT delta_views_prevDay , Title, URL , [Duration(mins)],[Release Date]
FROM [Youtubers_WH].[dbo].[video_cumm_stats] 
order by delta_views_prevDay DESC

/*
SELECT [Release Date], [Scheduled Start Time],Title, URL ,[Duration(mins)], views_latest, delta_views_prevDay   
FROM [Youtubers_WH].[dbo].[video_cumm_stats] 
WHERE [Duration(mins)] = 0 AND [Scheduled Start Time]  ='Not Live Streamed' 
ORDER BY [Release Date] DESC

*/