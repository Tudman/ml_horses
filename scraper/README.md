ml_horses - scrapers
=========

10-Jul TG
Uploaded some changes.
1) Now loading Horses' Birth Dates from the horse pages.
   Doing this as a separate process after having loaded all the race results.
2) Changed that handy db rebuilding functionality, it now runs the SQL in the 
   .sql files and the function def is in the race_data_pgsql.py file
3) Added some auditing functionality with error logging and audit logging
4) Added a star schema to the database, will have some sql to populate it from
   the staging data shortly. Should make reporting on the data easier.

So you should be able to run the historical load as-is and it'll rebuild the database
and then load from racenet. It'll take a few days to finish loading as getting
the individual horse detail pages takes a while - there's 100000 pages to get.


15-Jan TG
Uploaded new P-SQL Create.sql for creating the database & tables in postgres required for the initial staging load.
+ historical_race_data_main.py which does the actual load from the Racenet website.
+ race_data_postgres.py which populates the postgres tables.

run the create scripts in the sql file, then put the two .py files in the same directory and run the historical_race_data_main.py and it will populate the db with australian races going back to 2009 or so!

(4-Feb, Like your work buddy! - ST)