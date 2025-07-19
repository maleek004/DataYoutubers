--Channel stats ordered by snapshot date
SELECT [Snapshot Date],
			[Channel ID],
			[Likes],
			[Views],
			[Subscribers],
			[Videos]
FROM [Youtubers_WH].[dbo].[channelSnapshot_gold]
ORDER BY 2,1