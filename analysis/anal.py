#Importing library

import psycopg2

#establishing database connection details

dbconnstr = "host ='localhost' dbname='dw' user='etl' password='etl'"

#just borrowing this from you for a second

def getConnection(dbconnstr):
    return psycopg2.connect(dbconnstr)

con = getConnection(dbconnstr)


#write query defs...
#get top ten horses by count in table
#use as list and query the db, for name, time, distance, weight, track??? ... to get array
#transform for numpy, and other ml.
#get paramters and (create and) load them into a table

''' get list of distinct horse names '''

sql = 'select distinct runner.horse from stage.runner'

cur = con.cursor()

cur.execute(sql)

rows = cur.fetchall()

for row in rows:
    print(row)

#sql = "SELECT stage.
