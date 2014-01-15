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
CREATE SCHEMA Stage
  AUTHORIZATION etl;

Now you can run the rest of the script in that database.
It can also be re-run if you'd like to drop and re-create the tables from scratch

TG 15-Jan-14
*************/

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
  source character varying(20),
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
