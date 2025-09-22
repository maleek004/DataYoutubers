-- getting  title and  url to all video released on latest snapshot date
SELECT Title VideosReleasedToday,
        URL ,
        Views,
        [Release Date],
        [Duration(mins)]
FROM [Youtubers_WH].[dbo].[videoDetails_gold]
WHERE DATETRUNC(DAY,[Release Date]) = (SELECT MAX(DATETRUNC(DAY,[Release Date])) FROM [Youtubers_WH].[dbo].[videoDetails_gold])
ORDER BY Views desc

