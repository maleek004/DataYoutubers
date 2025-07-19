CREATE OR ALTER PROCEDURE [dbo].[InsertDailyVideoSnapshots]
AS
BEGIN
    DECLARE @currentloaddate DATE;
    SET @currentloaddate = (SELECT MAX(CAST(REPLACE(loadDate, ',', '-') AS DATE)) FROM [youtubers_LH].[dbo].[silver_video_details]);

    INSERT INTO [Youtubers_WH].[dbo].[videoSnapshots_gold] (
        [Video Key], [Video ID], [Views], [Likes], [Comments], [Snapshot Date]
    )
    SELECT 
        [Video Key],
        [Video ID],
        [Views],
        [Likes],
        [Comments],
        @currentloaddate AS [Snapshot Date]
    FROM [Youtubers_WH].[dbo].[videoDetails_gold]
    WHERE NOT EXISTS (
        SELECT 1 
        FROM [Youtubers_WH].[dbo].[videoSnapshots_gold]
        WHERE [Snapshot Date] = CONVERT(date, @currentloaddate)
    );
END;
