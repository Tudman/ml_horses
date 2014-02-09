#importing libraries

import psycopg2
import numpy
from sklearn import linear_model

#establishing database connection details

dbconnstr = "host ='localhost' dbname='dw' user='etl' password='etl'"

#just borrowing this from you for a second

def getConnection(dbconnstr):
    return psycopg2.connect(dbconnstr)

con = getConnection(dbconnstr)

'''To do'''
#write query defs...
#get top ten horses by count in table
#use as list and query the db, for name, time, distance, weight, track??? ... to get array
#transform for numpy, and other ml.
#get paramters and (create and) load them into a table

#sql = 'UPDATE stage.runner SET horse=lower(runner.horse);' # just making sure all the names are lowercase. comment this out if sure.

''' get list of distinct horse names, tucked away for a sec '''

#sql = 'select distinct runner.horse from stage.runner'


data_by_horse = []
regress_parameters = []

cur = con.cursor()

###''' test list '''###
sql = 'select runner.horse, count(runner.horse) as count from stage.runner GROUP BY runner.horse ORDER BY count DESC LIMIT 1' 

cur.execute(sql)

rows = cur.fetchall()

for row in rows:
    horse = (str(row[0]).strip('"')) # cleans strings for passing back into db.
    sql = "SELECT runner.finish_time, runner.weight, race.racedistance, race.trackcondition, racemeeting.location" \
    " FROM stage.runner, stage.race, stage.racemeeting WHERE runner.raceid = race.raceid" \
    " AND race.racemeetingid =  racemeeting.racemeetingid AND runner.horse = '" + horse.replace("'","''") + "';"
    cur.execute(sql)
    race_rows = cur.fetchall()
    data_race = []
    for race_row in race_rows:
       data_race.append(race_row)
    data_by_horse.append((horse,data_race))   

for i in range(len(data_by_horse)): #iterate throgh all the horses
    horse = data_by_horse[i][0] #get horses name
    x1 = []
    x2 = []
    y = []
    for j in range(len(data_by_horse[0][1])): #iterate through the race data
        y.append(float(data_by_horse[i][1][j][0]))
        x1.append(float(data_by_horse[i][1][j][1]))
        x2.append(int(data_by_horse[i][1][j][2][:-1:]))

    y = numpy.array(y)
    x1 = numpy.array(x1)
    x2 = numpy.array(x2)
    x = numpy.vstack([x1,x2]).T

    print(len(x1),len(x2),len(y))
    
    regr = linear_model.LinearRegression()
    regr.fit( x, y )
    print(regr.predict([59.5, 1600]))
       
    #do regression stuff here and store the parameters.

                       
