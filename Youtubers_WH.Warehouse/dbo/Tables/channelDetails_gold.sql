CREATE TABLE [dbo].[channelDetails_gold] (

	[Channel Key] int NULL, 
	[Channel ID] varchar(200) NULL, 
	[Channel Name] varchar(300) NULL, 
	[Channel Description] varchar(7000) NULL, 
	[Created At] date NULL, 
	[Thumbnail URL] varchar(300) NULL, 
	[Valid From] date NULL, 
	[Valid To] date NULL, 
	[Is Current] bit NULL, 
	[Is Deleted] bit NULL
);