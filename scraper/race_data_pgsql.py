# race_data_postgres

# provides PostgreSQL functions for race data etl
# db connection string is provided in race_data_main.py
# database function definitions. keep in separate file - have so can be used for different db engines
import psycopg2
from etl_audit_pgsql import *

def getConnection(dbconnstr):
    return psycopg2.connect(dbconnstr)

def rebuild_stage_db(con):
    # drop and rebuild the db tables.
    cur = con.cursor()
    
    file_name = "P-SQL Create.sql"
    
    with open (file_name, "r") as drop_create_db:
        str_sql = drop_create_db.read()
        
    cur.execute(str_sql)
    con.commit()
    
def checkMeetExists(meet_details, cnn):
    # return EXISTS meet
    cur = cnn.cursor()
    cur.execute("SELECT 1 " \
                "FROM Stage.RaceMeeting " \
                "WHERE LTRIM(RTRIM(location)) = '" + meet_details[0] + "' " \
                "AND state = '" + meet_details[1] + "' " \
                "AND meetingdate = '" + meet_details[2] + "' " \
                "LIMIT 1;")
    return cur.fetchone() is not None

def saveMeetDetails(meet_details, source, etl_run_id, cnn):
    # save the race meet details to the db and return the ID of the just added meet
    try:
        cur = cnn.cursor()
        strSQL = "INSERT INTO Stage.RaceMeeting ( " \
            "Location, State, MeetingDate, IsTrial, RailPosition, " \
            "TrackCondition, TrackType, Weather, Penetrometer, " \
            "ResultsLastPublished, Comments, Source, ETLRunID, Created) VALUES ( " \
            "'" + meet_details[4] + "', " \
            "'" + meet_details[3] + "', " \
            "'" + meet_details[2] + '-' + meet_details[1] + '-' + meet_details[0] + "', " \
            "'" + meet_details[5] + "', " \
            "'" + meet_details[6] + "', " \
            "'" + meet_details[7] + "', " \
            "'" + meet_details[8] + "', " \
            "'" + meet_details[9] + "', " \
            "'" + meet_details[10] + "'," \
            "'" + meet_details[11] + "', " \
            "'" + meet_details[12] + "', " \
            "'" + source + "', " \
            + etl_run_id + ", " \
            "now())"
        cur.execute(strSQL)
        cnn.commit()

        # return MeetID
        cur.execute("SELECT MAX(RaceMeetingID) AS id FROM Stage.RaceMeeting")
        meet_id = cur.fetchone()
        return meet_id[0]

    except Exception as exc:
        logError(etl_run_id, "saveMeetDetails: " + strSQL, exc, cnn)
        raise  # as we can't return a meet id

def saveRaceDetails(race_details, meet_id, source, etl_run_id, cnn):
    # save the race details to the db and return the ID of the race
    try:
        cur = cnn.cursor()
        strSQL = "INSERT INTO Stage.Race ( " \
            "RaceMeetingID, RaceNumber, RaceTime, RaceName, RaceDistance, " \
            "RaceDetails, TrackCondition, WinningTime, LastSplitTime, " \
            "OfficialComments, Source, ETLRunID, Created) VALUES ( " \
            "'" + str(meet_id) + "', " \
            "'" + race_details[0] + "', " \
            "'" + race_details[1] + "', " \
            "'" + race_details[2].replace("'", "''") + "', " \
            "'" + race_details[3] + "', " \
            "'" + race_details[4].replace("'", "''") + "', " \
            "'" + race_details[5] + "', " \
            "'" + race_details[6] + "', " \
            "'" + race_details[7].replace("'", "''") + "'," \
            "'" + race_details[8].replace("'", "''") + "', " \
            "'" + source + "', " \
            + etl_run_id + ", " \
            "now())"
        cur.execute(strSQL)
        cnn.commit()
        
        # return RaceID
        cur.execute("SELECT MAX(RaceID) AS id FROM Stage.Race")
        race_id = cur.fetchone()
        return race_id[0]

    except Exception as exc:
        logError(etl_run_id, "saveRaceDetails: " + strSQL, exc, cnn)
        raise  # as we can't return a race id

def saveRunnerDetails(runner_details, race_id, source, etl_run_id, cnn):
    # save the runner details to the db
    try:
        cur = cnn.cursor()
        strSQL = "INSERT INTO Stage.Runner ( " \
            "RaceID, Result, Number, Horse, Trainer, " \
            "Jockey, Margin, Barrier, Weight, " \
            "Penalty, StartingPrice, RaceTime, " \
            "DateOfBirth, HorsePageURL, " \
            "Source, ETLRunID, Created) VALUES ( """ \
            "'" + str(race_id) + "', " \
            "'" + runner_details[0] + "', " \
            "'" + runner_details[1] + "', " \
            "'" + runner_details[2].replace("'", "''") + "', " \
            "'" + runner_details[3].replace("'", "''") + "', " \
            "'" + runner_details[4].replace("'", "''") + "', " \
            "'" + runner_details[5] + "', " \
            "'" + runner_details[6] + "', " \
            "'" + runner_details[7] + "'," \
            "'" + runner_details[8] + "', " \
            "'" + runner_details[9] + "', " \
            "'" + runner_details[10] + "', " \
            "'" + runner_details[11] + "', " \
            "'" + runner_details[12].replace("'", "''") + "', " \
            "'" + source + "', " \
            + etl_run_id + ", " \
            "now())"
        cur.execute(strSQL)
        cnn.commit()
        return 1
    
    except Exception as exc:
        logError(etl_run_id, "saveRunnerDetails: " + strSQL, exc, cnn)

def checkRunnerExists(horse_page_url, cnn):
    # return EXISTS horse with that page URL
    try:
        cur = cnn.cursor()
        strSQL = "SELECT 1 " \
            "FROM Stage.Runner " \
            "WHERE HorsePageURL = '" + horse_page_url.replace("'", "''") + "'"
        cur.execute(strSQL)
        return cur.fetchone() is not None
    
    except Exception as exc:
        logError(etl_run_id, "checkRunnerExists: " + strSQL, exc, cnn)

def getRunnerURLs(cnn):
    # return set of distinct Runner page URLs
    try:
        cur = cnn.cursor()
        strSQL = "SELECT DISTINCT HorsePageURL " \
            "FROM Stage.Runner"
        cur.execute(strSQL)
        return cur.fetchall()
    
    except Exception as exc:
        logError(etl_run_id, "saveRunnerOtherDetails: " + strSQL, exc, cnn)

def saveRunnerOtherDetails(dob, runnerURL, cnn):
    # save the runner details to the db
    try:
        cursor = cnn.cursor()
        strSQL = "UPDATE Stage.Runner SET " \
            "DateOfBirth = '" + dob + "' " \
            "WHERE HorsePageURL = '" + runnerURL + "'"
        cursor.execute(strSQL)
        cnn.commit()
        return 1
    
    except Exception as exc:
        logError(etl_run_id, "saveRunnerOtherDetails: " + strSQL, exc, cnn)
