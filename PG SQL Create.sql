/*******************************************

Create dw database and tabes for horses ETL 
Script in PG SQL for PostgreSQL.

Creates: 
	a user for the ETL scripts to use;
	the 'dw'database;
	the 'stage' schema;
	staging tables for the horses data import

*** Versions ***
TG		19-Dec-13	Initial Version
*******************************************/






-- Role: etl

-- DROP ROLE etl;

CREATE ROLE etl LOGIN
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;



-- Database: dw

-- DROP SCHEMA stage;
-- DROP DATABASE dw;

CREATE DATABASE dw
  WITH OWNER = etl
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       LC_COLLATE = 'en_AU.UTF-8'
       LC_CTYPE = 'en_AU.UTF-8'
       CONNECTION LIMIT = -1;

USE dw;

CREATE SCHEMA Stage
  AUTHORIZATION etl;



-- create tables

-- DROP TABLE Stage.RaceMeeting;
-- DROP TABLE Stage.Race;
-- DROP TABLE Stage.Runner;

CREATE TABLE Stage.RaceMeeting (
  RaceMeetingID serial NOT NULL,
  Location character varying(100),
  "State" character varying(3),
  MeetingDate date,
  IsTrial bit,
  RailPosition character varying(100),
  TrackCondition character varying(100),
  TrackType character varying(100),
  Weather character varying(100),
  Penetrometer character varying(100),
  ResultsLastPublished character varying(100),
  Comments character varying(1000),
  CONSTRAINT PK_RaceMeeting PRIMARY KEY (RaceMeetingID)
)
WITH (OIDS=FALSE);
ALTER TABLE Stage.RaceMeeting OWNER TO etl;

CREATE TABLE Stage.Race (
  RaceID serial NOT NULL,
  RaceMeetingID bigint REFERENCES Stage.RaceMeeting (RaceMeetingID),
  RaceNumber character varying(10),
  RaceTime character varying(10),
  RaceName character varying(100),
  RaceDistance character varying(20),
  RaceDetails character varying(200),
  TrackCondition character varying(50),
  WinningTime character varying(50),
  LastBitTime character varying(50),
  OfficialComments character varying(200),
  CONSTRAINT PK_Race PRIMARY KEY (RaceID)
)
WITH (OIDS=FALSE);
ALTER TABLE Stage.Race OWNER TO etl;

CREATE TABLE Stage."Runner"
(
  RunnerID serial NOT NULL,
  RaceID bigint REFERENCES Stage.Race (RaceID),
  Horse character varying(100),
  Trainer character varying(100),
  Jockey character varying(100),
  Number integer,
  Barrier integer,
  Result integer,
  Margin character varying(20),
  "Weight" character varying(20),
  Penalty character varying(20),
  StartingPrice character varying(20),
  CONSTRAINT PK_Runner PRIMARY KEY (RunnerID)
)
WITH (OIDS=FALSE);
ALTER TABLE Stage.Runner OWNER TO etl;




