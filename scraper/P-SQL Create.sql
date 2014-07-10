/*************
Create Postgresql tables

firstly, create a role by running this separately:
CREATE ROLE etl LOGIN
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;

then create a database by running this separately:
CREATE DATABASE dw
  WITH OWNER = etl
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       LC_COLLATE = 'en_AU.UTF-8'
       LC_CTYPE = 'en_AU.UTF-8'
       CONNECTION LIMIT = -1;

then in that database create a scema using this:
CREATE SCHEMA Audit
  AUTHORIZATION etl;

CREATE SCHEMA Stage
  AUTHORIZATION etl;

CREATE SCHEMA dw
  AUTHORIZATION etl;
  
Now you can run the rest of the script in that database.
It can also be re-run if you'd like to drop and re-create the tables from scratch

TG 15-Jan-14
*************/



-- Audit Tables
 DROP TABLE IF EXISTS Audit.ETLError;
 DROP TABLE IF EXISTS Audit.ETLRunLog;
 DROP TABLE IF EXISTS Audit.ETLRun;

 CREATE TABLE Audit.ETLRun
(
	ETLRunID serial NOT NULL,
	ETLName character varying(50) NULL,
	Start timestamp without time zone NULL,
	Finish timestamp without time zone NULL,
	CONSTRAINT PK_ETLRun PRIMARY KEY (ETLRunID)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE Audit.ETLRun
  OWNER TO etl;

 CREATE TABLE Audit.ETLError
(
	ETLErrorID serial NOT NULL,
	ETLRunID bigint NOT NULL,
	ETLState character varying(5000) NULL,
	Error character varying(5000) NULL,
	Logged timestamp without time zone NULL,
	CONSTRAINT PK_ETLError PRIMARY KEY (ETLErrorID),
	CONSTRAINT ETLError_ETLRunID_FKey FOREIGN KEY (ETLRunID)
    REFERENCES Audit.ETLRun (ETLRunID) MATCH SIMPLE
    ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE Audit.ETLError
  OWNER TO etl;


 CREATE TABLE Audit.ETLRunLog
(
	ETLRunLogID serial NOT NULL,
	ETLRunID int NOT NULL,
	LogName character varying(50) NULL,
	LogDescription character varying(2000) NULL,
	RecordsLoaded int NULL,
	RecordsCreated int NULL,
	RecordsUpdated int NULL,
	Start timestamp without time zone NULL,
	Finish timestamp without time zone NULL,
	Logged timestamp without time zone NULL,
 	CONSTRAINT PK_ETLRunLog PRIMARY KEY (ETLRunLogID),
	CONSTRAINT ETLRunLog_ETLRunID_FKey FOREIGN KEY (ETLRunID)
    REFERENCES Audit.ETLRun (ETLRunID) MATCH SIMPLE
    ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE Audit.ETLRunLog
  OWNER TO etl;




  

-- Horses staging tables

 DROP TABLE IF EXISTS Stage.Runner;
 DROP TABLE IF EXISTS Stage.Race;
 DROP TABLE IF EXISTS Stage.RaceMeeting;
  
CREATE TABLE stage.racemeeting
(
  racemeetingid serial NOT NULL,
  location character varying(100),
  state character varying(3),
  meetingdate date,
  istrial bit(1),
  railposition character varying(100),
  trackcondition character varying(100),
  tracktype character varying(100),
  weather character varying(100),
  penetrometer character varying(100),
  resultslastpublished character varying(100),
  comments character varying(1000),
  source character varying(20),
  etlrunid int NOT NULL,
  created timestamp without time zone NOT NULL,
  CONSTRAINT pk_racemeeting PRIMARY KEY (racemeetingid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE stage.racemeeting
  OWNER TO etl;

CREATE TABLE stage.race
(
  raceid serial NOT NULL,
  racemeetingid bigint,
  racenumber character varying(10),
  racetime character varying(10),
  racename character varying(100),
  racedistance character varying(20),
  racedetails character varying(200),
  trackcondition character varying(50),
  winningtime character varying(50),
  lastsplittime character varying(50),
  officialcomments character varying(200),
  source character varying(20),
  etlrunid int NOT NULL,
  created timestamp without time zone NOT NULL,
  CONSTRAINT pk_race PRIMARY KEY (raceid),
  CONSTRAINT race_racemeetingid_fkey FOREIGN KEY (racemeetingid)
      REFERENCES stage.racemeeting (racemeetingid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE Stage.Race OWNER TO etl;

CREATE TABLE stage.runner
(
  runnerid serial NOT NULL,
  raceid bigint,
  horse character varying(100),
  trainer character varying(100),
  jockey character varying(100),
  "number" integer,
  barrier integer,
  result character varying(10),
  margin character varying(20),
  weight character varying(20),
  penalty character varying(20),
  startingprice character varying(20),
  racetime character varying(10),
  dateofbirth date,
  horsepageurl character varying(400),
  source character varying(20),
  etlrunid int NOT NULL,
  created timestamp without time zone NOT NULL,
  CONSTRAINT pk_runner PRIMARY KEY (runnerid),
  CONSTRAINT runner_raceid_fkey FOREIGN KEY (raceid)
      REFERENCES stage.race (raceid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE stage.runner
  OWNER TO etl;



-- horses data warehouse tables
DROP TABLE IF EXISTS dw.RaceResult;
DROP TABLE IF EXISTS dw.Age;
DROP TABLE IF EXISTS dw.Horse;
DROP TABLE IF EXISTS dw.Race;
DROP TABLE IF EXISTS dw.Calendar;
DROP TABLE IF EXISTS dw.Jockey;
DROP TABLE IF EXISTS dw.FinishPosition;



-- now create tables
CREATE TABLE dw.Age ( 
	AgeInDays integer NOT NULL,
	AgeInYears integer NOT NULL,
	CONSTRAINT PK_Age PRIMARY KEY (AgeInDays)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE dw.Age
  OWNER TO etl;

CREATE TABLE dw.FinishPosition (
	FinishPositionKey integer NOT NULL,
	FinishPosition character varying(10) NOT NULL,
	DidNotFinish boolean NOT NULL,
	DidNotStart boolean NOT NULL,
	CONSTRAINT PK_FinishPosition PRIMARY KEY (FinishPositionKey)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE dw.FinishPosition
  OWNER TO etl;

CREATE TABLE dw.Calendar ( 
	CalendarKey integer NOT NULL,
	Date date NOT NULL,
	DateName character varying(11) NOT NULL,
	DayOfWeekId integer NOT NULL,
	DayOfWeekName character varying(9) NOT NULL,
	MonthId integer NOT NULL,
	MonthName character varying(20) NOT NULL,
	MonthOfYearId integer NOT NULL,
	MonthOfYearName character varying(20) NOT NULL,
	SeasonId integer NOT NULL,
	SeasonName character varying(20) NOT NULL,
	SeasonOfYearId integer NOT NULL,
	SeasonOfYearName character varying(20) NOT NULL,
	QuarterId integer NOT NULL,
	QuarterName character varying(20) NOT NULL,
	QuarterOfYearId integer NOT NULL,
	QuarterOfYearName character varying(20) NOT NULL,
	YearId integer NOT NULL,
	YearName character varying(4) NOT NULL,
	CONSTRAINT PK_Calendar PRIMARY KEY (CalendarKey)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE dw.Calendar
  OWNER TO etl;

CREATE TABLE dw.Horse ( 
	HorseKey serial NOT NULL,
	DateOfBirthCalendarKey integer NOT NULL,
	HorseName character varying(50) NOT NULL,
	TrainerName character varying(50) NOT NULL,
	DateOfBirth date NOT NULL,
	"Source" character varying(20) NOT NULL,
	EffectiveFrom timestamp without time zone NOT NULL,
	EffectiveFromETLRunKey bigint NOT NULL,
	EffectiveTo timestamp without time zone NOT NULL,
	EffectiveToETLRunKey bigint NOT NULL,
	IsCurrent boolean NOT NULL,
	CONSTRAINT PK_Horse PRIMARY KEY (HorseKey)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE dw.Horse
  OWNER TO etl;

CREATE TABLE dw.Race ( 
	RaceKey serial NOT NULL,
	RaceCalendarKey integer NOT NULL,
	RaceDate date NOT NULL,
	StartTime time NOT NULL,
	"State" character varying(20) NOT NULL,
	Track character varying(50) NOT NULL,
	RaceNumber character varying(10) NOT NULL,
	RaceName character varying(50) NOT NULL,
	RaceDistanceMeters integer NOT NULL,
	IsTrial boolean NOT NULL,
	RailPosition character varying(100) NOT NULL,
	TrackCondition character varying(100) NOT NULL,
	TrackType character varying(100) NOT NULL,
	Weather character varying(100) NOT NULL, 
	Penetrometer character varying(100) NOT NULL,
	WinningTimeSeconds numeric(6,2) NOT NULL,
	LastSplitTime numeric(6,2) NOT NULL,
	ResultsLastPublished character varying(100) NOT NULL,
	RaceComments character varying(5000) NOT NULL,
	"Source" character varying(20) NOT NULL,
	PrizeMoney character varying(500) NOT NULL,
	Created timestamp without time zone NOT NULL,
	CreatedETLRunKey bigint NOT NULL,
	Updated timestamp without time zone NOT NULL,
	UpdatedETLRunKey bigint NOT NULL,
	CONSTRAINT PK_Race PRIMARY KEY (RaceKey),
	CONSTRAINT FK_Race_Calendar FOREIGN KEY (RaceCalendarKey)
		REFERENCES dw.Calendar (CalendarKey) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE dw.Race
  OWNER TO etl;

CREATE TABLE dw.Jockey (
	JockeyKey serial NOT NULL,
	JockeyName character varying(100) NOT NULL,
	Created timestamp without time zone NOT NULL,
	CreatedETLRunKey bigint NOT NULL,
	CONSTRAINT PK_Jockey PRIMARY KEY (JockeyKey)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE dw.Jockey
  OWNER TO etl;

CREATE TABLE dw.RaceResult ( 
	RaceResultKey serial NOT NULL,
	RaceKey integer NOT NULL,
	JockeyKey integer NOT NULL,
	RaceCalendarKey integer NOT NULL,
	HorseKey integer NOT NULL,
	AgeOnRaceDayDays integer NOT NULL,
	FinishPositionKey integer NOT NULL,
	RaceTimeSeconds numeric(6,2) NOT NULL,
	StartingBarrier integer NOT NULL,
	StartingPrice money NOT NULL,
	WeightKg integer NOT NULL,
	JockeysAllowance integer NOT NULL,
	Penalty character varying(20) NOT NULL, 
	"Source" character varying(20) NOT NULL, 
	Created timestamp without time zone NOT NULL,
	CreatedETLRunKey bigint NOT NULL,
	Updated timestamp without time zone NOT NULL,
	UpdatedETLRunKey bigint NOT NULL,
	CONSTRAINT PK_RaceResult PRIMARY KEY (RaceResultKey),
	CONSTRAINT FK_RaceResult_Race FOREIGN KEY (RaceKey)
		REFERENCES dw.Race (RaceKey) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE NO ACTION,
	CONSTRAINT FK_RaceResult_Jockey FOREIGN KEY (JockeyKey)
		REFERENCES dw.Jockey (JockeyKey) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE NO ACTION,
	CONSTRAINT FK_RaceResult_Calendar FOREIGN KEY (RaceCalendarKey)
		REFERENCES dw.Calendar (CalendarKey) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE NO ACTION,
	CONSTRAINT FK_RaceResult_Horse FOREIGN KEY (HorseKey)
		REFERENCES dw.Horse (HorseKey) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE NO ACTION,
	CONSTRAINT FK_RaceResult_Age FOREIGN KEY (AgeOnRaceDayDays)
		REFERENCES dw.Age (AgeInDays) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE NO ACTION,
	CONSTRAINT FK_RaceResult_FinishPosition FOREIGN KEY (FinishPositionKey)
		REFERENCES dw.FinishPosition (FinishPositionKey) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE dw.RaceResult
  OWNER TO etl;





-- Create Indexes
CREATE INDEX IX_RaceResult_RaceKey ON dw.RaceResult (RaceKey ASC);
CREATE INDEX IX_RaceResult_RaceCalendarKey ON dw.RaceResult (RaceCalendarKey ASC);
CREATE INDEX IX_RaceResult_HorseKey ON dw.RaceResult (HorseKey ASC);
CREATE INDEX IX_RaceResult_AgeOnRaceDayDays ON dw.RaceResult (AgeOnRaceDayDays ASC);
CREATE INDEX IX_Age_AgeInYears ON dw.Age (AgeInYears ASC);
CREATE INDEX IX_FinishPosition_FinishPosition ON dw.FinishPosition (FinishPosition ASC);




-- Populate DW bits

-- Populate dw.FinishPosition
INSERT INTO dw.FinishPosition (
	FinishPositionKey, FinishPosition, DidNotFinish, DidNotStart)
SELECT * FROM (VALUES
	(1, '1', FALSE, FALSE), (2, '2', FALSE, FALSE), (3, '3', FALSE, FALSE), (4, '4', FALSE, FALSE), (5, '5', FALSE, FALSE), (6, '6', FALSE, FALSE), (7, '7', FALSE, FALSE), (8, '8', FALSE, FALSE), (9, '9', FALSE, FALSE), (10, '10', FALSE, FALSE), 
	(11, '11', FALSE, FALSE), (12, '12', FALSE, FALSE), (13, '13', FALSE, FALSE), (14, '14', FALSE, FALSE), (15, '15', FALSE, FALSE), (16, '16', FALSE, FALSE), (17, '17', FALSE, FALSE), (18, '18', FALSE, FALSE), (19, '19', FALSE, FALSE), (20, '20', FALSE, FALSE), 
	(21, '21', FALSE, FALSE), (22, '22', FALSE, FALSE), (23, '23', FALSE, FALSE), (24, '24', FALSE, FALSE), (25, '25', FALSE, FALSE), (26, '26', FALSE, FALSE), (27, '27', FALSE, FALSE), (28, '28', FALSE, FALSE), (29, '29', FALSE, FALSE), (30, '30', FALSE, FALSE), 
	(100, 'BD', TRUE, FALSE), (101, 'DQ', FALSE, FALSE), (102, 'FF', TRUE, FALSE), (103, 'FL', TRUE, FALSE), (104, 'LR', FALSE, FALSE), (105, 'NP', FALSE, FALSE),
	(-1, 'Unknown', FALSE, FALSE)) 
	AS a(FinishPositionKey, FinishPosition, DidNotFinish, DidNotStart)
;

-- Populate dw.Age
WITH Days (DayNum) AS (
	SELECT * 
	FROM generate_series(1, 365 * 20)
)
INSERT INTO dw.Age (
	AgeInDays, AgeInYears)
SELECT DayNum, Daynum / 365 AS YearNum
FROM Days
;




-- Populate dw.Calendar
WITH dates (Date) AS (
	SELECT CAST('19000101' AS Date) + interval '1' day * generate_series AS Date
	FROM generate_series(0, 100000)
)
, date_ids (DateKey, Date, YearId, DayOfMonth, DayOfWeekId, MonthId, SeasonId, QuarterId) AS (
	SELECT DateKey, d.Date, YearId, DayOfMonth, DayOfWeekId, MonthId, SeasonId, QuarterId
	FROM dates d,
		LATERAL (SELECT CAST(DATE_PART('year', d.Date) * 10000 
			+ DATE_PART('month', d.Date) * 100 + DATE_PART('day', d.Date) AS integer))  AS d1 (DateKey),
		LATERAL CAST(EXTRACT(YEAR FROM d.Date) AS integer) AS dyr(YearId),
		LATERAL (SELECT CAST(DATE_PART('day', d.Date) AS char(2))) AS domnth(DayOfMonth),
		LATERAL (SELECT CAST((DATE_PART('day', CAST(d.Date AS timestamp) - timestamp '19000101')) AS integer) % 7 + 1) as d2(DayOfWeekId),
		LATERAL (SELECT CAST(DATE_PART('month', d.Date) AS integer)) AS dmnth(MonthID),
		LATERAL (SELECT CASE dmnth.MonthID WHEN 1 THEN 1 
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
						END) AS dsea(SeasonId),
		LATERAL (SELECT CASE dmnth.MonthID WHEN 1 THEN 1 
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
)

, date_nms (DateKey, Date, YearId, DayOfMonth, DayOfWeekId, MonthId, SeasonId, QuarterId,
	YearName, DayOfWeekName, ShortDayOfWeekName, MonthName, ShortMonthName, SeasonName,
	QuarterName, MonthOfYearId, SeasonOfYearId, QuarterOfYearId) AS (
	SELECT DateKey, Date, YearId, DayOfMonth, DayOfWeekId, MonthId, SeasonId, QuarterId,
		CAST(YearId AS char(4)) AS YearName, DayOfWeekName, ShortDayOfWeekName,
		MonthName, ShortMonthName, SeasonName, QuarterName, 
		YearId * 100 + MonthId AS MonthOfYearId,
		YearId * 10 + SeasonId AS SeasonOfYearId,
		YearId * 10 + QuarterId AS QuarterOfYearId
	FROM date_ids,
		LATERAL (SELECT CASE DayOfWeekId WHEN 1 THEN 'Monday' 
				WHEN 2 THEN 'Tuesday' 
				WHEN 3 THEN 'Wednesday'
				WHEN 4 THEN 'Thursday'
				WHEN 5 THEN 'Friday'
				WHEN 6 THEN 'Saturday'
				WHEN 7 THEN 'Sunday'
				END) AS dwk(DayOfWeekName),
		LATERAL (SELECT LEFT(dwk.DayOfWeekName,3)) AS dwk1(ShortDayOfWeekName),
		LATERAL (SELECT CASE MonthID WHEN 1 THEN 'January' 
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
				END) AS mthn(MonthName),
		LATERAL (SELECT LEFT(mthn.MonthName,3)) AS mthn1(ShortMonthName),
		LATERAL (SELECT CASE SeasonId WHEN 1 THEN 'Summer'
						WHEN 2 THEN 'Autumn'
						WHEN 3 THEN 'Winter'
						WHEN 4 THEN 'Spring'
						END) AS sean(SeasonName),
		LATERAL (SELECT CASE QuarterId WHEN 1 THEN 'Q1'
						WHEN 2 THEN 'Q2'
						WHEN 3 THEN 'Q3'
						WHEN 4 THEN 'Q4'
						END) AS qtr(QuarterName)
	)

INSERT INTO dw.Calendar (
	CalendarKey, Date, DateName, DayOfWeekId, DayOfWeekName, 
	MonthId, MonthName, MonthOfYearId, MonthOfYearName, 
	SeasonId, SeasonName, SeasonOfYearId, SeasonOfYearName, 
	QuarterId, QuarterName, QuarterOfYearId, QuarterOfYearName, 
	YearId, YearName)
SELECT DateKey, Date, 
	DayOfMonth || ' ' || ShortMonthName || ' ' || YearName AS DateName,
	DayOfWeekId, DayOfWeekName, 
	MonthId, MonthName, MonthOfYearId, 
	ShortMonthName || ' ' || YearName MonthOfYearName,
	SeasonId, SeasonName, SeasonOfYearId,
	SeasonName || ' ' || YearName SeasonOfYearName,
	QuarterId, QuarterName, QuarterOfYearId, 
	QuarterName || ' ' || YearName QuarterOfYearName,
	YearId, YearName
FROM date_nms


