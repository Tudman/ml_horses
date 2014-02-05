# race_data_postgres

# provides PostgreSQL functions for race data etl
# db connection string is provided in race_data_main.py
# database function definitions. keep in separate file - have so can be used for different db engines
import psycopg2

def getConnection(dbconnstr):
    return psycopg2.connect(dbconnstr)

def checkMeetExists(meet_details, con):#dbconnstr):
    # return EXISTS meet
    cur = con.cursor()
    cur.execute("SELECT 1 " \
                "FROM ""Stage"".racemeeting"" " \
                "WHERE LTRIM(RTRIM(location)) = '" + meet_details[0] + "' " \
                "AND state = '" + meet_details[1] + "' " \
                "AND meetingdate = '" + meet_details[2] + "' " \
                "LIMIT 1;")
    return cur.fetchone() is not None

def saveMeetDetails(meet_details, source, con):#dbconnstr):
    # save the race meet details to the db and return the ID of the just added meet
	

##    print('Year: ' + meet_details[0])
##    print('Month: ' + meet_details[1])
##    print('Day: ' + meet_details[2])
##    print('State: ' + meet_details[3])
##    print('Location: ' + meet_details[4])
##    print('IsTrial?: ' + meet_details[5])
##    print('Rail Position: ' + meet_details[6])
##    print('Track Condition: ' + meet_details[7])
##    print('Track Type: ' + meet_details[8])
##    print('Weather: ' + meet_details[9])
##    print('Penetrometer: ' + meet_details[10])
##    print('Results Last Published: ' + meet_details[11])
##    print('Comments: ' + meet_details[12])

    cur = con.cursor()
    cur.execute("INSERT INTO ""Stage"".racemeeting ( " \
        "location, state, meetingdate, istrial, railposition, " \
        "TrackCondition, TrackType, Weather, Penetrometer, " \
        "ResultsLastPublished, Comments, Source, Created) VALUES ( " \
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
        "now())")
    con.commit()

    # return MeetID
    cur.execute("SELECT MAX(RaceMeetingID) AS id FROM ""Stage"".racemeeting")
    meet_id = cur.fetchone()
    return meet_id[0]

def saveRaceDetails(race_details, meet_id, source, con):#dbconnstr):
    # save the race details to the db and return the ID of the race
    
##    print('Race Number: ' + race_details[0])
##    print('Race Time: ' + race_details[1])
##    print('Race Name: ' + race_details[2])
##    print('Race Distance: ' + race_details[3])
##    print('Race Details: ' + race_details[4])
##    print('Track Condition: ' + race_details[5])
##    print('Winning Time: ' + race_details[6])
##    print ('Last Split Time: ' + race_details[7])
##    print ('Official Comments: ' + race_details[8])
 
    cur = con.cursor()
    cur.execute("INSERT INTO ""Stage"".race ( " \
        "racemeetingID, racenumber, racetime, racename, racedistance, " \
        "raceDetails, trackCondition, winningtime, lastsplittime, " \
        "OfficialComments, Source, Created) VALUES ( " \
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
        "now())")
    con.commit()
	
   
    # return RaceID
    cur.execute("SELECT MAX(RaceID) AS id FROM Stage.Race")
    race_id = cur.fetchone()
    return race_id[0]

def saveRunnerDetails(runner_details, race_id, source, con):#dbconnstr):
    # save the runner details to the db
    
##    print('Finish Position: ' + runner_details[0])
##    print('Number: ' + runner_details[1])
##    print('Runner Name: ' + runner_details[2])
##    print('Trainer Name: ' + runner_details[3])
##    print('Jockey Name: ' + runner_details[4])
##    print('Margin to Winner: ' + runner_details[5])
##    print('Barrier: ' + runner_details[6])
##    print('Weight: ' + runner_details[7])
##    print('Penalty: ' + runner_details[8])
##    print('Starting Price: ' + runner_details[9])

    cur = con.cursor()
    cur.execute("INSERT INTO Stage.Runner ( " \
        "RaceID, Result, Number, Horse, Trainer, " \
        "Jockey, Margin, Time, Barrier, Weight, " \
        "Penalty, StartingPrice, Source, Created) VALUES ( """ \
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
        "'" + source + "', " \
        "now())")
    con.commit()
	
    return 1
