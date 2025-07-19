-- Create a schema and channel handles table 
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name= 'metadata')
BEGIN 
	EXEC('CREATE SCHEMA metadata');
END
GO

DROP TABLE IF EXISTS Youtubers_WH.metadata.metadata
CREATE TABLE Youtubers_WH.metadata.metadata
(
channel_ID VARCHAR(30) NOT NULL -- the max length for youtube handles is 24 - 30 ish 
)

