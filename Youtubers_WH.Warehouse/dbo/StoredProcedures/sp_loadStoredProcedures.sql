CREATE   PROCEDURE sp_loadStoredProcedures
AS
BEGIN


   -- BEGIN TRY
        EXEC [Youtubers_WH].[dbo].[sp_loadChannelSnapshot];
        EXEC [Youtubers_WH].[dbo].[sp_insertNewChanneldetails];
        EXEC [Youtubers_WH].[dbo].[sp_load_videos_gold];
        EXEC [Youtubers_WH].[dbo].[SP_updateChannelRecords];
        EXEC [Youtubers_WH].[dbo].[InsertDailyVideoSnapshots];
        EXEC [Youtubers_WH].[dbo].[sp_loadCummulativeChannelStats];
        EXEC [Youtubers_WH].[dbo].[sp_loadCummulativeVideoStats];
   -- END TRY
   -- BEGIN CATCH
        -- Handle error: log, rethrow, or return error info
   --     DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
   --     RAISERROR('Error occurred: %s', 16, 1, @ErrorMessage);
   --     RETURN;
   -- END CATCH
END