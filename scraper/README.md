ml_horses - scrapers
=========


15-Jan TG
Uploaded new P-SQL Create.sql for creating the database & tables in postgres required for the initial staging load.
+ historical_race_data_main.py which does the actual load from the Racenet website.
+ race_data_postgres.py which populates the postgres tables.

run the create scripts in the sql file, then put the two .py files in the same directory and run the historical_race_data_main.py and it will populate the db with australian races going back to 2009 or so!

(4-Feb, Like your work buddy! - ST)