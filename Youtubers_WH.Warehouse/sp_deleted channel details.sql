/*CREATE OR ALTER PROCEDURE sp_deleteChannelDetails AS
BEGIN 
DROP TABLE IF EXISTS Youtubers_WH.temp.deletedChannelRecords

-- create the temp table and gather the deleted records into it 
CREATE TABLE Youtubers_WH.temp.deletedChannelRecords AS 
SELECT  slv.channel_ID,
        slv.channel_name,
        slv.channel_description,
        slv.channel_created_at,
        slv.thumbnail_URL,
        --slv.loadDate
        CAST( '2025-05-24' AS Date ) loadDate
FROM [youtubers_LH].[dbo].[silver_channel_details] slv
LEFT JOIN [Youtubers_WH].[dbo].[channelDetails_gold] gld
    ON slv.channel_ID = gld.[Channel ID]
WHERE gld.[Channel ID] IS NULL;

END