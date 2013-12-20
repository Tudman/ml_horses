/*******************************************

Create dw database and tabes for horses ETL 
Script in T_SQL for SQL Server.
Will do a PostgreSQL version too.

Creates: 
	a user for the ETL scripts to use;
	the 'dw'database;
	the 'stage' schema;
	staging tables for the horses data import

*** Versions ***
TG		19-Dec-13	Initial Version
*******************************************/


-- create db
IF NOT (EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = 'dw'))
CREATE DATABASE dw;
GO

USE dw;
GO

-- create login
IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'etl')
DROP USER etl;
GO
IF EXISTS (SELECT * FROM sys.server_principals WHERE name = N'etl')
DROP LOGIN etl
GO
CREATE LOGIN etl WITH PASSWORD=N'etlpass'
	, DEFAULT_DATABASE = dw
	, DEFAULT_LANGUAGE = us_english
	, CHECK_EXPIRATION = OFF
	, CHECK_POLICY = OFF
GO
CREATE USER etl FOR LOGIN etl WITH DEFAULT_SCHEMA = dbo
GO



-- create schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'Stage') 
	EXEC ('CREATE SCHEMA Stage AUTHORIZATION dbo')
GO




-- create tables
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('Stage.FK_Race_RaceMeeting') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE Stage.Race DROP CONSTRAINT FK_Race_RaceMeeting
;
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('Stage.FK_Runner_Race') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE Stage.Runner DROP CONSTRAINT FK_Runner_Race
;
IF  EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'Stage.RaceMeeting'))
DROP TABLE Stage.RaceMeeting
IF  EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'Stage.Race'))
DROP TABLE Stage.Race
IF  EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'Stage.Runner'))
DROP TABLE Stage.Runner
GO

CREATE TABLE Stage.RaceMeeting (
  RaceMeetingID int identity(1, 1) NOT NULL,
  Location varchar(100),
  [State] varchar(3),
  MeetingDate date,
  IsTrial bit,
  RailPosition varchar(100),
  TrackCondition varchar(100),
  TrackType varchar(100),
  Weather varchar(100),
  Penetrometer varchar(100),
  ResultsLastPublished varchar(100),
  Comments varchar(1000),
  CONSTRAINT PK_RaceMeeting PRIMARY KEY CLUSTERED (RaceMeetingID)
 )
 ;
 
CREATE TABLE Stage.Race ( 
	RaceID int identity(1, 1) NOT NULL,
	RaceMeetingID int NOT NULL,
	RaceNumber varchar(10),
	RaceTime varchar(10),
	RaceName varchar(100),
	RaceDistance varchar(20),
	RaceDetails varchar(200),
	TrackCondition varchar(50),
	WinningTime varchar(50),
	LastSplitTime varchar(50),
	OfficialComments varchar(200),
	Created datetime2(0) NOT NULL,
	CONSTRAINT PK_Race PRIMARY KEY CLUSTERED (RaceID)
)
;

CREATE TABLE Stage.Runner (
  RunnerID int identity(1, 1) NOT NULL,
  RaceID int NOT NULL,
  Horse varchar(100),
  Trainer varchar(100),
  Jockey varchar(100),
  Number int,
  Barrier int,
  Result int,
  Margin varchar(20),
  [Weight] varchar(20),
  Penalty varchar(20),
  StartingPrice varchar(20),
  CONSTRAINT PK_Runner PRIMARY KEY CLUSTERED (RunnerID)
 )
 ;

--  Create Foreign Key Constraints 
ALTER TABLE Stage.Race ADD CONSTRAINT FK_Race_RaceMeeting
	FOREIGN KEY (RaceMeetingID) REFERENCES Stage.RaceMeeting (RaceMeetingID)
;
ALTER TABLE Stage.Runner ADD CONSTRAINT FK_Runner_Race
	FOREIGN KEY (RaceID) REFERENCES Stage.Race (RaceID)
;
