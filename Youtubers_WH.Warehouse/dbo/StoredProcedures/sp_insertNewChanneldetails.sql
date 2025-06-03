CREATE   PROCEDURE sp_insertNewChanneldetails AS 
BEGIN 

DROP TABLE IF EXISTS Youtubers_WH.temp.newChannelRecords

-- create the temp table and gather the new records into it 
CREATE TABLE Youtubers_WH.temp.newChannelRecords AS 
SELECT  slv.channel_ID,
        slv.channel_name,
        slv.channel_description,
        slv.channel_created_at,
        slv.thumbnail_URL,
        slv.loadDate
        --CAST( '2025-05-24' AS Date ) loadDate
FROM [youtubers_LH].[dbo].[silver_channel_details] slv
LEFT JOIN [Youtubers_WH].[dbo].[channelDetails_gold] gld
    ON slv.channel_ID = gld.[Channel ID]
WHERE gld.[Channel ID] IS NULL;

-- If there are records in the temp table , then you need to insert them into the gold layer table  
IF EXISTS (SELECT 1 FROM Youtubers_WH.temp.newChannelRecords)
    DECLARE @MaxChannelKey AS INT;

    IF EXISTS(SELECT * FROM Youtubers_WH.dbo.channelDetails_gold)
        SET @MaxChannelKey = (SELECT MAX([Channel Key]) FROM Youtubers_WH.dbo.channelDetails_gold)
    ELSE
        SET @MaxChannelKey = 0

    INSERT INTO Youtubers_WH.dbo.channelDetails_gold
    SELECT @MaxChannelKey + ROW_NUMBER() OVER(ORDER BY(SELECT NULL)),
            channel_ID,
            channel_name,
            channel_description,
            channel_created_at,
            thumbnail_URL,
            loadDate,
            --CAST( '2025-05-24' AS Date ) loadDate,
            CAST('9999-12-31'AS DATE),
            1,
            0
    FROM Youtubers_WH.temp.newChannelRecords
DROP TABLE Youtubers_WH.temp.newChannelRecords
END