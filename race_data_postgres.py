# race_data_postgres

# provides PostgreSQL functions for race data etl
# db connection string is provided in race_data_main.py
# database function definitions. keep in separate file - have so can be used for different db engines
import psycopg2

def checkMeetExists(meet_details, dbconnstr):
    # return EXISTS meet
    return 1

def saveMeetDetails(meet_details, dbconnstr):
    # save the race meet details to the db and return the ID of the just added meet
    print('Year: ' + meet_details[0])
    print('Month: ' + meet_details[1])
    print('Day: ' + meet_details[2])
    print('State: ' + meet_details[3])
    print('Location: ' + meet_details[4])
    print('IsTrial?: ' + meet_details[5])
    print('Rail Position: ' + meet_details[6])
    print('Track Condition: ' + meet_details[7])
    print('Track Type: ' + meet_details[8])
    print('Weather: ' + meet_details[9])
    print('Penetrometer: ' + meet_details[10])
    print('Results Last Published: ' + meet_details[11])
    print('Comments: ' + meet_details[12])
    return 1

def saveRaceDetails(race_details, meet_id, dbconnstr):
    # save the race details to the db and return the ID of the race
    print('Race Number: ' + race_details[0])
    print('Race Time: ' + race_details[1])
    print('Race Name: ' + race_details[2])
    print('Race Distance: ' + race_details[3])
    print('Race Details: ' + race_details[4])
    print('Track Condition: ' + race_details[5])
    print('Winning Time: ' + race_details[6])
    print ('Last Split Time: ' + race_details[7])
    print ('Official Comments: ' + race_details[8])
    return 1

def saveRunnerDetails(runner_details, race_id, dbconnstr):
    # save the runner details to the db
    print('Finish Position: ' + runner_details[0])
    print('Number: ' + runner_details[1])
    print('Runner Name: ' + runner_details[2])
    print('Trainer Name: ' + runner_details[3])
    print('Jockey Name: ' + runner_details[4])
    print('Margin to Winner: ' + runner_details[5])
    print('Barrier: ' + runner_details[6])
    print('Weight: ' + runner_details[7])
    print('Penalty: ' + runner_details[8])
    print('Starting Price: ' + runner_details[9])
    return 1
