
#establishing database connection

from race_data_postgres import *
dbconnstr = "host ='localhost' dbname='dw' user='etl' password='etl'"

con = getConnection(dbconnstr)


#write query defs...
#get top ten horses by count in table
#use as list and query the db, for name, time, distance, weight, track??? ... to get array
#transform for numpy, and other ml.
#get paramters and (create and) load them into a table