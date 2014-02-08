
import psycopg2

def getConnection(dbconnstr):
    return psycopg2.connect(dbconnstr)

dbconnstr = "host ='localhost' dbname='dw' user='etl' password='etl'"

con = getConnection(dbconnstr)

cur = con.cursor()

meet_details = (['2014', 'January', '31', 'NSW', 'Albury ', '0', '', '', '', '', '', '', ''])

def checkMeetExists(meet_details, con):#dbconnstr):
    # return EXISTS meet
    cur = con.cursor()
    cur.execute("SELECT 1 " \
                "FROM stage.racemeeting " \
                "WHERE LTRIM(RTRIM(location)) = '" + meet_details[0] + "' " \
                "AND state = '" + meet_details[1] + "' " \
                "AND meetingdate = '" + meet_details[2] + "' " \
                "LIMIT 1;")
    return cur.fetchone() is not None

meet_details = ('Albury','NSW','31-January-2014')

x = checkMeetExists(meet_details, con)

meet_details = (['2014', 'January', '31', 'NSW', 'Albury ', '0', '', '', '', '', '', '', ''])

print(x)

cur.execute("INSERT INTO stage.racemeeting ( " \
        "location, state, meetingdate, istrial, railposition, " \
        "trackcondition, trackType, weather, penetrometer, " \
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
        "'" + 'racenet' + "', " \
        "now());")

con.commit()

cur.execute("INSERT InTO stage.racemeeting ( state ) VALUES ('NSW');")

con.commit()
print('make it here?')


