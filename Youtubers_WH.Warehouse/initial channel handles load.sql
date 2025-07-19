--COPY INTO metadata.metadata
--FROM 'https://sacctdatayoutubers.blob.core.windows.net/youtube-handles/channelHandles.txt'
--WITH(FILE_TYPE='csv')

INSERT INTO metadata.metadata 
SELECT [Channel ID]
FROM [Youtubers_WH].[dbo].[channelDetails_gold]