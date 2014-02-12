'''Instructions:
Make sure you have the libraries, and test thier importing.
This script connects to the postgre db with the horse racing data in it.
As a proof of concept, I select the horse the the most runs, and determine the influence race distance and handicapping
have on race time, and plug in two artificial scenarios.
'''

#importing libraries

import psycopg2
import numpy
import scipy
from sklearn import linear_model


#establishing database connection details

dbconnstr = "host ='localhost' dbname='dw' user='etl' password='etl'"

#just borrowing this from you for a second

def getConnection(dbconnstr):
    return psycopg2.connect(dbconnstr)

con = getConnection(dbconnstr)

'''To do'''
#get paramters and (create and) load them into a table
#bring other prediction tools.


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

    print("horse: ", horse)
    print("scikit_learn predictions")
    
    regr = linear_model.LinearRegression(True,)
    regr.fit( x, y )
    regr.get_params()
    print("e.g., predicted time for 2050 metres @ 57.5kg:", regr.predict([57.5, 2050]), ' seconds')
    print("e.g., predicted time for 1600 metres @ 59.5kg:", regr.predict([59.5, 1600]), ' seconds')

    print("getting coefficients")

    coefs = (numpy.linalg.lstsq( x, y ))

    print(str((float(coefs[0][1])*1600)+ (float(coefs[0][0])*59.5)))

      #do regression stuff here and store the parameters

''' doing some investigations here...'''

sql_rt = 'select finish_time from stage.runner'

cur.execute(sql_rt)

RT = cur.fetchall()
RT_str = [str(item) for item in RT]
RT_float = [float(str(item).strip("(''),")) for item in RT_str]
RT_float = numpy.asarray(RT_float)
RT_clean = RT_float[RT_float > 40] #selecting out races run in less than 40sec - some dodgy data coming down the web pipes. 

_max, _min = numpy.amax(RT_clean), numpy.amin(RT_clean)

hist_range = range(40,440,10)

import matplotlib.pyplot as plt
plt.hist(RT_clean, hist_range)
plt.show()

       
    #do more regression stuff, and parameter investigation here and calculate and apply parameters to horses in upcoming races.

                       
