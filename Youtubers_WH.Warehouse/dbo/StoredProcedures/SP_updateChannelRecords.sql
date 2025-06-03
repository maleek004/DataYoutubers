CREATE   PROCEDURE SP_updateChannelRecords AS

BEGIN

DROP TABLE IF EXISTS Youtubers_WH.temp.updatedChannelRecords

-- create the temp table and gather the updated records into it 
CREATE TABLE Youtubers_WH.temp.updatedChannelRecords AS 
SELECT  slv.channel_name,
        slv.channel_ID,
        slv.channel_description,
        slv.channel_created_at,
        slv.thumbnail_URL,
        slv.loadDate
        --CAST( '2025-05-24' AS Date ) loadDate
FROM [youtubers_LH].[dbo].[silver_channel_details] slv
INNER JOIN [Youtubers_WH].[dbo].[channelDetails_gold] gld
ON slv.channel_ID = gld.[Channel ID]
WHERE gld.[Is Current] = 1
    AND gld.[Channel Name] <> slv.[channel_name];

-- If there are records in the temp table , then you need to update the gold layer table appropriately 
IF EXISTS (SELECT 1 FROM temp.updatedChannelRecords)
    BEGIN
    -- for the old version of the updated records in the gold table, the columns [Valid To] and [Is Current] needs to be updated
    UPDATE gld
    SET gld.[Valid To] = tmp.loadDate,
        gld.[Is Current] = 0
    FROM [Youtubers_WH].[dbo].[channelDetails_gold] gld
    INNER JOIN temp.updatedChannelRecords tmp
        ON gld.[Channel ID] = tmp.channel_ID
    WHERE gld.[Is Current] = 1;

    -- then the current version of the updated records will be inserted
    DECLARE @MaxChannelKey AS INT;

    IF EXISTS(SELECT * FROM Youtubers_WH.dbo.channelDetails_gold)
        SET @MaxChannelKey = (SELECT MAX([Channel Key]) FROM Youtubers_WH.dbo.channelDetails_gold)
    ELSE
        SET @MaxChannelKey = 0

    INSERT INTO Youtubers_WH.dbo.channelDetails_gold
    SELECT 
        @MaxChannelKey + ROW_NUMBER() OVER(ORDER BY(SELECT NULL)),
        channel_ID,
        channel_name,
        channel_description,
        channel_created_at,
        thumbnail_URL,
        loadDate,
        '9999-12-31 23:59:59',
        1,
        0
    FROM Youtubers_WH.temp.updatedChannelRecords
    END
DROP TABLE Youtubers_WH.temp.updatedChannelRecords

END