#drop and rebuild tables

def rebuild_stage_db(con):
    
    cur = con.cursor()
    
    cur.execute("DROP TABLE IF EXISTS stage.runner;")
    cur.execute("DROP TABLE IF EXISTS stage.race;")
    cur.execute("DROP TABLE IF EXISTS stage.raceMeeting;")
    
    cur.execute("CREATE TABLE stage.test (testid serial)")
    
    cur.execute("CREATE TABLE stage.racemeeting" \
                    "(racemeetingid serial NOT NULL," \
                    "location character varying(100)," \
                    "state character varying(3)," \
                    "meetingdate date," \
                    "istrial bit(1), " \
                    "railposition character varying(100), " \
                    "trackcondition character varying(100), " \
                    "tracktype character varying(100), " \
                    "weather character varying(100), " \
                    "penetrometer character varying(100), " \
                    "resultslastpublished character varying(100), " \
                    "comments character varying(1000), " \
                    "source character varying(20), " \
                    "created timestamp without time zone NOT NULL, " \
                    "CONSTRAINT pk_racemeeting PRIMARY KEY (racemeetingid)) " \
                    "WITH (OIDS=FALSE);")
    
    cur.execute("ALTER TABLE stage.racemeeting OWNER TO etl;")

    cur.execute("CREATE TABLE stage.race(" \
                    "raceid serial NOT NULL," \
                    "racemeetingid bigint," \
                    "racenumber character varying(10)," \
                    "racetime character varying(10)," \
                    "racename character varying(100)," \
                    "racedistance character varying(20)," \
                    "racedetails character varying(200)," \
                    "trackcondition character varying(50)," \
                    "winningtime numeric(10,4)," \
                    "lastsplittime character varying(50)," \
                    "officialcomments character varying(200)," \
                    "source character varying(20)," \
                    "created timestamp without time zone NOT NULL," \
                    "CONSTRAINT pk_race PRIMARY KEY (raceid)," \
                    "CONSTRAINT race_racemeetingid_fkey FOREIGN KEY (racemeetingid)" \
                    "REFERENCES stage.racemeeting (racemeetingid) MATCH SIMPLE " \
                    "ON UPDATE NO ACTION ON DELETE NO ACTION)" \
                    "WITH (OIDS=FALSE);")
    
    cur.execute("ALTER TABLE stage.race OWNER TO etl;")

    cur.execute("CREATE TABLE stage.runner" \
                "(runnerid serial NOT NULL," \
                "raceid bigint," \
                "horse character varying(100)," \
                "trainer character varying(100)," \
                "jockey character varying(100)," \
                "horse_number integer, " \
                "barrier integer, " \
                "result character varying(10), " \
                "margin character varying(20)," \
                "finish_time numeric," \
                "weight character varying(20)," \
                "penalty character varying(20)," \
                "startingprice character varying(20)," \
                "source character varying(20)," \
                "created timestamp without time zone NOT NULL," \
                "CONSTRAINT pk_runner PRIMARY KEY (runnerid)," \
                "CONSTRAINT runner_raceid_fkey FOREIGN KEY (raceid)" \
                "REFERENCES stage.race (raceid) MATCH SIMPLE " \
                "ON UPDATE NO ACTION ON DELETE NO ACTION) " \
                "WITH (OIDS=FALSE);")
    
    cur.execute("ALTER TABLE stage.runner OWNER TO etl;")
    
    con.commit
