WITH changedChannels AS (SELECT * FROM [Youtubers_WH].[dbo].[channelDetails_gold] WHERE [Is Current] = 0),
changedChannelsnew as (
SELECT * FROM [Youtubers_WH].[dbo].[channelDetails_gold]
WHERE [Channel ID] IN (
                        SELECT [Channel ID] FROM changedChannels    )
                        AND [Is Current] = 1 )

SELECT * FROM changedChannels
UNION ALL 
SELECT * FROM changedChannelsnew
ORDER BY [Channel ID]