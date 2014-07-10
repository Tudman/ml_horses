/*******************************************

Create dw database and tables for horses ETL 
Script in T_SQL for SQL Server.
Will do a PostgreSQL version too.

Creates: 
	a user for the ETL scripts to use;
	the 'dw'database;
	the 'stage' schema;
	the 'audit' schema
	staging tables for the horses data import
	audit tables for general ETL logging

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
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'Audit') 
	EXEC ('CREATE SCHEMA Audit AUTHORIZATION dbo')
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'Stage') 
	EXEC ('CREATE SCHEMA Stage AUTHORIZATION dbo')
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'dw') 
	EXEC ('CREATE SCHEMA dw AUTHORIZATION dbo')
GO


-- grant permissions
GRANT SELECT ON SCHEMA::Audit TO etl
GRANT INSERT ON SCHEMA::Audit TO etl
GRANT UPDATE ON SCHEMA::Audit TO etl

GRANT SELECT ON SCHEMA::Stage TO etl
GRANT INSERT ON SCHEMA::Stage TO etl
GRANT UPDATE ON SCHEMA::Stage TO etl

GRANT SELECT ON SCHEMA::dw TO etl
GRANT INSERT ON SCHEMA::dw TO etl
GRANT UPDATE ON SCHEMA::dw TO etl


-- create tables
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('Audit.FK_ETLRunLog_ETLRun') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE Audit.ETLRunLog DROP CONSTRAINT FK_ETLRunLog_ETLRun
;
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('Audit.FK_ETLError_ETLRun') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE Audit.ETLError DROP CONSTRAINT FK_ETLError_ETLRun
;
IF  EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'Audit.ETLError'))
DROP TABLE Audit.ETLError
IF  EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'Audit.ETLRun'))
DROP TABLE Audit.ETLRun
IF  EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'Audit.ETLRunLog'))
DROP TABLE Audit.ETLRunLog
GO


CREATE TABLE Audit.ETLError (
	ETLErrorID int IDENTITY(1,1) NOT NULL,
	ETLRunID int NOT NULL,
	ETLState varchar(5000) NULL,
	Error varchar(5000) NULL,
	Logged datetime2(7) NULL,
	CONSTRAINT PK_ETLError PRIMARY KEY CLUSTERED (ETLErrorID)
 )
 ;

 CREATE TABLE Audit.ETLRun (
	ETLRunID int IDENTITY(1,1) NOT NULL,
	ETLName varchar(50) NULL,
	Start datetime2(7) NULL,
	Finish datetime2(7) NULL,
	CONSTRAINT PK_ETLRun PRIMARY KEY CLUSTERED (ETLRunID)
 )
 ;

 CREATE TABLE Audit.ETLRunLog(
	ETLRunLogID int IDENTITY(1,1) NOT NULL,
	ETLRunID int NOT NULL,
	LogName varchar(50) NULL,
	LogDescription varchar(2000) NULL,
	RecordsLoaded int NULL,
	RecordsCreated int NULL,
	RecordsUpdated int NULL,
	Start datetime2(7) NULL,
	Finish datetime2(7) NULL,
	Logged datetime2(7) NULL,
 	CONSTRAINT PK_ETLRunLog PRIMARY KEY CLUSTERED (ETLRunLogID)
)
;


--  Create Foreign Key Constraints 
ALTER TABLE Audit.ETLError ADD CONSTRAINT FK_ETLError_ETLRun
	FOREIGN KEY (ETLRunID) REFERENCES Audit.ETLRun (ETLRunID)
;
ALTER TABLE Audit.ETLRunLog ADD CONSTRAINT FK_ETLRunLog_ETLRun
	FOREIGN KEY (ETLRunID) REFERENCES Audit.ETLRun (ETLRunID)
;







-- create specific Races staging tables. This could be done in separate scripts for new data sources
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
  [Source] varchar(20),
  ETLRunID int NOT NULL,
  Created datetime2(7) NOT NULL,
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
	[Source] varchar(20),
	ETLRunID int NOT NULL,
	Created datetime2(7) NOT NULL,
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
	Result varchar(10),
	Margin varchar(20),
	[Weight] varchar(20),
	Penalty varchar(20),
	StartingPrice varchar(20),
	RaceTime varchar(10),
	DateOfBirth date,
	HorsePageURL varchar(400),
	[Source] varchar(20),
	ETLRunID int NOT NULL,
	Created datetime2(7) NOT NULL,
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





-- first drop foreign Keys if exist
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('FK_Horse_DateOfBirthCalendarKey') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE dw.Horse DROP CONSTRAINT FK_Horse_DateOfBirthCalendarKey;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('FK_Race_RaceCalendarKey') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE dw.Race DROP CONSTRAINT FK_Race_RaceCalendarKey;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('FK_RaceResult_AgeOnRaceDayDays') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE dw.RaceResult DROP CONSTRAINT FK_RaceResult_AgeOnRaceDayDays;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('FK_RaceResult_HorseKey') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE dw.RaceResult DROP CONSTRAINT FK_RaceResult_HorseKey;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('FK_RaceResult_RaceCalendarKey') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE dw.RaceResult DROP CONSTRAINT FK_RaceCalendarKey;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('FK_RaceResult_RaceKey') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE dw.RaceResult DROP CONSTRAINT FK_RaceResult_RaceKey;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('FK_RaceResult_Jockey') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE dw.RaceResult DROP CONSTRAINT FK_RaceResult_Jockey;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('FK_RaceResult_FinishPosition') AND OBJECTPROPERTY(id, 'IsForeignKey') = 1)
ALTER TABLE dw.RaceResult DROP CONSTRAINT FK_RaceResult_FinishPosition;
GO



-- drop tables if exist
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('dw.RaceResult') AND  OBJECTPROPERTY(id, 'IsUserTable') = 1)
DROP TABLE dw.RaceResult;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('dw.Age') AND  OBJECTPROPERTY(id, 'IsUserTable') = 1)
DROP TABLE dw.Age;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('dw.Horse') AND  OBJECTPROPERTY(id, 'IsUserTable') = 1)
DROP TABLE dw.Horse;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('dw.Race') AND  OBJECTPROPERTY(id, 'IsUserTable') = 1)
DROP TABLE dw.Race;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('dw.Calendar') AND  OBJECTPROPERTY(id, 'IsUserTable') = 1)
DROP TABLE dw.Calendar;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('dw.Jockey') AND  OBJECTPROPERTY(id, 'IsUserTable') = 1)
DROP TABLE dw.Jockey;
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id('dw.FinishPosition') AND  OBJECTPROPERTY(id, 'IsUserTable') = 1)
DROP TABLE dw.FinishPosition;
GO


-- now create tables
CREATE TABLE dw.Age ( 
	AgeInDays int NOT NULL,
	AgeInYears int NOT NULL,
	CONSTRAINT PK_Age PRIMARY KEY CLUSTERED (AgeInDays)
);

CREATE TABLE dw.FinishPosition (
	FinishPositionKey int NOT NULL,
	FinishPosition varchar(10) NOT NULL,
	DidNotFinish bit NOT NULL,
	DidNotStart bit NOT NULL,
	CONSTRAINT PK_FinishPosition PRIMARY KEY CLUSTERED (FinishPositionKey)
);

CREATE TABLE dw.Calendar ( 
	CalendarKey int NOT NULL,
	Date date NOT NULL,
	DateName varchar(11) NOT NULL,
	DayOfWeekId int NOT NULL,
	DayOfWeekName varchar(9) NOT NULL,
	MonthId int NOT NULL,
	MonthName varchar(20) NOT NULL,
	MonthOfYearId int NOT NULL,
	MonthOfYearName varchar(20) NOT NULL,
	SeasonId int NOT NULL,
	SeasonName varchar(20) NOT NULL,
	SeasonOfYearId int NOT NULL,
	SeasonOfYearName varchar(20) NOT NULL,
	QuarterId int NOT NULL,
	QuarterName varchar(20) NOT NULL,
	QuarterOfYearId int NOT NULL,
	QuarterOfYearName varchar(20) NOT NULL,
	YearId int NOT NULL,
	YearName varchar(4) NOT NULL,
	CONSTRAINT PK_Calendar PRIMARY KEY CLUSTERED (CalendarKey)
);

CREATE TABLE dw.Horse ( 
	HorseKey int IDENTITY(1,1) NOT NULL,
	DateOfBirthCalendarKey int NOT NULL,
	HorseName varchar(50) NOT NULL,
	TrainerName varchar(50) NOT NULL,
	DateOfBirth datetime2(7) NOT NULL,
	[Source] varchar(20) NOT NULL,
	EffectiveFrom datetime2(7) NOT NULL,
	EffectiveFromETLRunKey bigint NOT NULL,
	EffectiveTo datetime2(7) NOT NULL,
	EffectiveToETLRunKey bigint NOT NULL,
	IsCurrent bit NOT NULL,
	CONSTRAINT PK_Horse PRIMARY KEY CLUSTERED (HorseKey)
);

CREATE TABLE dw.Race ( 
	RaceKey int IDENTITY(1,1) NOT NULL,
	RaceCalendarKey int NOT NULL,
	RaceDate date NOT NULL,
	StartTime time(7) NOT NULL,
	[State] varchar(20) NOT NULL,
	Track varchar(50) NOT NULL,
	RaceNumber varchar(10) NOT NULL,
	RaceName varchar(50) NOT NULL,
	RaceDistanceMeters int NOT NULL,
	IsTrial bit NOT NULL,
	RailPosition varchar(100) NOT NULL,
	TrackCondition varchar(100) NOT NULL,
	TrackType varchar(100) NOT NULL,
	Weather varchar(100) NOT NULL, 
	Penetrometer varchar(100) NOT NULL,
	WinningTimeSeconds decimal(6,2) NOT NULL,
	LastSplitTime decimal(6,2) NOT NULL,
	ResultsLastPublished varchar(100) NOT NULL,
	RaceComments varchar(5000) NOT NULL,
	[Source] varchar(20) NOT NULL,
	PrizeMoney varchar(500) NOT NULL,
	Created datetime2(7) NOT NULL,
	CreatedETLRunKey bigint NOT NULL,
	Updated datetime2(7) NOT NULL,
	UpdatedETLRunKey bigint NOT NULL,
	CONSTRAINT PK_Race PRIMARY KEY CLUSTERED (RaceKey)
);

CREATE TABLE dw.RaceResult ( 
	RaceResultKey bigint IDENTITY(1,1) NOT NULL,
	RaceKey int NOT NULL,
	JockeyKey int NOT NULL,
	RaceCalendarKey int NOT NULL,
	HorseKey int NOT NULL,
	AgeOnRaceDayDays int NOT NULL,
	FinishPositionKey int NOT NULL,
	RaceTimeSeconds decimal(6,2) NOT NULL,
	StartingBarrier int NOT NULL,
	StartingPrice money NOT NULL,
	WeightKg int NOT NULL,
	JockeysAllowance int NOT NULL,
	Penalty varchar(20) NOT NULL, 
	[Source] varchar(20) NOT NULL, 
	Created datetime2(7) NOT NULL,
	CreatedETLRunKey bigint NOT NULL,
	Updated datetime2(7) NOT NULL,
	UpdatedETLRunKey bigint NOT NULL,
	CONSTRAINT PK_RaceResult PRIMARY KEY CLUSTERED (RaceResultKey)
);

CREATE TABLE dw.Jockey (
	JockeyKey int IDENTITY(1,1) NOT NULL,
	JockeyName varchar(100) NOT NULL,
	Created datetime2(7) NOT NULL,
	CreatedETLRunKey bigint NOT NULL,
	CONSTRAINT PK_Jockey PRIMARY KEY CLUSTERED (JockeyKey)
);

-- put on foreign keys
ALTER TABLE dw.RaceResult
	ADD CONSTRAINT FK_RaceResult_AgeOnRaceDayDays FOREIGN KEY (AgeOnRaceDayDays)
	REFERENCES dw.Age (AgeInDays)
GO

ALTER TABLE dw.RaceResult
	ADD CONSTRAINT FK_RaceResult_HorseKey FOREIGN KEY (HorseKey)
	REFERENCES dw.Horse (HorseKey)
GO

ALTER TABLE dw.RaceResult
	ADD CONSTRAINT FK_RaceResult_RaceCalendarKey FOREIGN KEY (RaceCalendarKey)
	REFERENCES dw.Calendar (CalendarKey)
GO

ALTER TABLE dw.RaceResult
	ADD CONSTRAINT FK_RaceResult_RaceKey FOREIGN KEY (RaceKey)
	REFERENCES dw.Race (RaceKey)
GO

ALTER TABLE dw.RaceResult
	ADD CONSTRAINT FK_RaceResult_Jockey FOREIGN KEY (JockeyKey)
	REFERENCES dw.Jockey (JockeyKey)
GO

ALTER TABLE dw.RaceResult
	ADD CONSTRAINT FK_RaceResult_FinishPosition FOREIGN KEY (FinishPositionKey)
	REFERENCES dw.FinishPosition (FinishPositionKey)
GO


ALTER TABLE dw.Horse
	ADD CONSTRAINT FK_Horse_DateOfBirthCalendarKey FOREIGN KEY (DateOfBirthCalendarKey)
	REFERENCES dw.Calendar (CalendarKey)
GO
ALTER TABLE dw.Race
	ADD CONSTRAINT FK_Race_RaceCalendarKey FOREIGN KEY (RaceCalendarKey)
	REFERENCES dw.Calendar (CalendarKey)
GO

-- don't check the foreign keys though, speed things up
ALTER TABLE dw.RaceResult NOCHECK CONSTRAINT ALL
GO
ALTER TABLE dw.Horse NOCHECK CONSTRAINT ALL
GO
ALTER TABLE dw.Race NOCHECK CONSTRAINT ALL
GO
	

-- Create Indexes
CREATE NONCLUSTERED INDEX IX_RaceResult_RaceKey ON dw.RaceResult (RaceKey ASC) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX IX_RaceResult_RaceCalendarKey ON dw.RaceResult (RaceCalendarKey ASC) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX IX_RaceResult_HorseKey ON dw.RaceResult (HorseKey ASC) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX IX_RaceResult_AgeOnRaceDayDays ON dw.RaceResult (AgeOnRaceDayDays ASC) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX IX_Age_AgeInYears ON dw.Age (AgeInYears ASC) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX IX_FinishPosition_FinishPosition ON dw.FinishPosition (FinishPosition ASC) ON [PRIMARY]
GO



-- Populate dw.FinishPosition
INSERT INTO dw.FinishPosition (
	FinishPositionKey, FinishPosition, DidNotFinish, DidNotStart)
SELECT * FROM (VALUES
	(1, '1', 0, 0), (2, '2', 0, 0), (3, '3', 0, 0), (4, '4', 0, 0), (5, '5', 0, 0), (6, '6', 0, 0), (7, '7', 0, 0), (8, '8', 0, 0), (9, '9', 0, 0), (10, '10', 0, 0), 
	(11, '11', 0, 0), (12, '12', 0, 0), (13, '13', 0, 0), (14, '14', 0, 0), (15, '15', 0, 0), (16, '16', 0, 0), (17, '17', 0, 0), (18, '18', 0, 0), (19, '19', 0, 0), (20, '20', 0, 0), 
	(21, '21', 0, 0), (22, '22', 0, 0), (23, '23', 0, 0), (24, '24', 0, 0), (25, '25', 0, 0), (26, '26', 0, 0), (27, '27', 0, 0), (28, '28', 0, 0), (29, '29', 0, 0), (30, '30', 0, 0), 
	(100, 'BD', 1, 0), (101, 'DQ', 0, 0), (102, 'FF', 1, 0), (103, 'FL', 1, 0), (104, 'LR', 0, 0), (105, 'NP', 0, 0),
	(-1, 'Unknown', 0, 0)) 
	AS a(FinishPositionKey, FinishPosition, DidNotFinish, DidNotStart)
;





-- Populate dw.Calendar

WITH dates AS (
	SELECT TOP (DATEDIFF(day,'19000101','20991231')) 
	DATEADD(day,row_Number() OVER (ORDER BY (SELECT 1))-1,'19000101') AS Date
	FROM master.dbo.spt_values AS t1
	CROSS JOIN master.dbo.spt_values AS t2
)
,
Calendar AS (
	SELECT *
	FROM dates AS d
	CROSS APPLY (SELECT year(d.Date)) AS dyr(YearId)
	CROSS APPLY (SELECT CAST(dyr.YearId AS char(4))) AS dyr1(YearName)
	CROSS APPLY (SELECT year(d.Date) * 10000 + month(d.Date) * 100 + day(d.Date)) as d1 (DateKey)
	CROSS APPLY (SELECT datediff(day,'19000101',d.Date) % 7 + 1) as d2(DayOfWeekId)
	CROSS APPLY (SELECT CASE d2.DayOfWeekId WHEN 1 THEN 'Monday' 
					WHEN 2 THEN 'Tuesday' 
					WHEN 3 THEN 'Wednesday'
					WHEN 4 THEN 'Thursday'
					WHEN 5 THEN 'Friday'
					WHEN 6 THEN 'Saturday'
					WHEN 7 THEN 'Sunday'
					END) AS d3(DayOfWeekName)
	CROSS APPLY (SELECT LEFT(d3.DayOfWeekName,3)) AS d4(ShortDayOfWeekName)
	CROSS APPLY (SELECT month(d.Date)) AS dmnth(MonthID)
	CROSS APPLY (SELECT CASE dmnth.MonthID WHEN 1 THEN 'January' 
					WHEN 2 THEN 'February'
					WHEN 3 THEN 'March'
					WHEN 4 THEN 'April'
					WHEN 5 THEN 'May'
					WHEN 6 THEN 'June'
					WHEN 7 THEN 'July'
					WHEN 8 THEN 'August'
					WHEN 9 THEN 'September'
					WHEN 10 THEN 'October'
					WHEN 11 THEN 'November'
					WHEN 12 THEN 'December'
					END) AS dmnth1(MonthName)
	CROSS APPLY (SELECT LEFT(dmnth1.MonthName,3)) AS dmnth2(ShortMonthName)
	CROSS APPLY (SELECT dyr.YearId * 100 +  dmnth.MonthID) AS dmnth3(MonthOfYearId)
	CROSS APPLY (SELECT dmnth2.ShortMonthName + ' ' + dyr1.YearName) AS dmnth4(MonthOfYearName)
	CROSS APPLY (SELECT CASE dmnth.MonthID WHEN 1 THEN 1 
					WHEN 2 THEN 1
					WHEN 3 THEN 2
					WHEN 4 THEN 2
					WHEN 5 THEN 2
					WHEN 6 THEN 3
					WHEN 7 THEN 3
					WHEN 8 THEN 3
					WHEN 9 THEN 4
					WHEN 10 THEN 4
					WHEN 11 THEN 4
					WHEN 12 THEN 1
					END) AS dsea(SeasonId)
	CROSS APPLY (SELECT CASE dsea.SeasonId WHEN 1 THEN 'Summer'
					WHEN 2 THEN 'Autumn'
					WHEN 3 THEN 'Winter'
					WHEN 4 THEN 'Spring'
					END) AS dsea1(SeasonName)
	CROSS APPLY (SELECT dyr.YearId * 10 + dsea.SeasonId) AS dsea2(SeasonOfYearId)
	CROSS APPLY (SELECT dsea1.SeasonName + ' ' + dyr1.YearName) AS dsea3(SeasonOfYearName)
	CROSS APPLY (SELECT CASE dmnth.MonthID WHEN 1 THEN 1 
					WHEN 2 THEN 1
					WHEN 3 THEN 1
					WHEN 4 THEN 2
					WHEN 5 THEN 2
					WHEN 6 THEN 2
					WHEN 7 THEN 3
					WHEN 8 THEN 3
					WHEN 9 THEN 3
					WHEN 10 THEN 4
					WHEN 11 THEN 4
					WHEN 12 THEN 4
					END) AS dqtr(QuarterId)
	CROSS APPLY (SELECT CASE dqtr.QuarterId WHEN 1 THEN 'Qtr 1'
					WHEN 2 THEN 'Qtr 2'
					WHEN 3 THEN 'Qtr 3'
					WHEN 4 THEN 'Qtr 4'
					END) AS dqtr1(QuarterName)
	CROSS APPLY (SELECT dyr.YearId * 10 + dqtr.QuarterId) AS dqtr2(QuarterOfYearId)
	CROSS APPLY (SELECT dqtr1.QuarterName + ' ' + dyr1.YearName) AS dqtr3(QuarterOfYearName)
	CROSS APPLY (SELECT CAST(day(d.Date) AS char(2)) + ' ' + dmnth2.ShortMonthName + ' ' + dyr1.YearName) AS d5(DateName)
)

INSERT INTO dw.Calendar (
	CalendarKey, Date, DateName, DayOfWeekId, DayOfWeekName, MonthId, MonthName,
	MonthOfYearId, MonthOfYearName, SeasonId, SeasonName, SeasonOfYearId, SeasonOfYearName, 
	QuarterId, QuarterName, QuarterOfYearId, QuarterOfYearName, YearId, YearName)
SELECT 
	DateKey, Date, DateName, DayOfWeekId, DayOfWeekName, MonthId, MonthName,
	MonthOfYearId, MonthOfYearName, SeasonId, SeasonName, SeasonOfYearId, SeasonOfYearName,
	QuarterId, QuarterName, QuarterOfYearID, QuarterOfYearName, YearId, YearName
FROM Calendar
;


-- Populate dw.Age
WITH days AS (
	SELECT TOP (365 * 20)
		row_Number() OVER (ORDER BY (SELECT 1)) AS daynum
	FROM master.dbo.spt_values AS t1
	CROSS JOIN master.dbo.spt_values AS t2
)
INSERT INTO dw.Age (
	AgeInDays, AgeInYears)
SELECT daynum AS DayNum, daynum / 365 AS YearNum
FROM days
