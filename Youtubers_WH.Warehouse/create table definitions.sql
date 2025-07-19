-- Create table definition for the gold layer channels tables
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name='channelDetails_gold')
BEGIN 

CREATE TABLE dbo.channelDetails_gold(
	[Channel Key] INT,
	[Channel ID] VARCHAR(200) ,
	[Channel Name] Varchar(300), -- channel titles have 100 character limit
	[Channel Description] varchar(7000), -- channel descriptions have a limit of 5000 characters
	[Created At] DATE,
	[Thumbnail URL] VARCHAR(300),
	[Valid From] DATE,
    [Valid To] DATE,
    [Is Current] BIT,
    [Is Deleted] BIT


);
END
GO


IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name='channelSnapshot_gold')
BEGIN 

CREATE TABLE dbo.channelSnapshot_gold(
	[Snapshot Date] DATETIME2(0),
	[Channel ID] VARCHAR(100) NOT NULL,
	[Likes] INT,
	[Views] INT,
	[Subscribers] INT,
	[Videos] INT

);
END
GO



-- create table definition for the gold layer videos table 

IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name='videoDetails_gold')
BEGIN 

CREATE TABLE dbo.videoDetails_gold(
	[Video Key] INT,
	[Video ID] VARCHAR(200),
	[Channel ID] VARCHAR(200),
	[Release Date] DATE,
	[Scheduled Start Time] VARCHAR(300), -- the column contains descriptive string values 
	[Title] VARCHAR(300), -- video titles have a limit  of 100 characters 
	[Duration(mins)] INT,
	[Views] INT,
	[Likes] INT,
	[Comments] INT,
	[URL] VARCHAR(300),
	[Thumbnail] VARCHAR(300)
)
END
GO