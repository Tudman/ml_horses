## python 3.3
##
## ETL Audit framework
##
## Provides standard functions to be added to ETL
## to log errors, start and finish times for ETL packages
## and provide a unique ETL Run ID.
#import pyodbc

def etlAuditStart(etl_file_name, cnn):
    # create ETL Audit record in the database
    # return the ETLRunID created in the database
    cursor = cnn.cursor()
    cursor.execute("INSERT INTO Audit.ETLRun ( " \
                   "ETLName, Start) VALUES ( " \
                   "'" + etl_file_name + "', " \
                   "GETDATE())")
    cnn.commit()
    # return the ETLRunID
    run_id = cursor.execute("SELECT MAX(ETLRunID) AS id FROM Audit.ETLRun").fetchone()
    return str(run_id.id);

def etlAuditFinish(etl_run_id, cnn):
    # record the finish of the ETL run
    cursor = cnn.cursor()
    cursor.execute("UPDATE Audit.ETLRun " \
                   "SET Finish = GETDATE() " \
                   "WHERE ETLRunID = " + etl_run_id)
    cnn.commit()
    return 1;

# log an ETL activity
def etlAuditLog(etl_run_id, etl_log_args, cnn):
    # Add an Audit Log record

    # unpack the etl_log_args
    log_name = etl_log_args[0]
    log_desc = etl_log_args[1]
    rec_load = 0
    rec_crea = 0
    rec_updt = 0
    start = ""
    finish = ""
    if len(etl_log_args) > 2:
        rec_load = etl_log_args[2]
        if len(etl_log_args) > 3:
            rec_crea = etl_log_args[3]
            if len(etl_log_args) > 4:
                rec_updt = etl_log_args[4]
                if len(etl_log_args) > 5:
                    start = etl_log_args[5]
                    if len(etl_log_args) > 6:
                        finish = etl_log_args[6]
    
    cursor = cnn.cursor()
    cursor.execute("INSERT INTO Audit.ETLRunLog ( " \
                   "ETLRunID, LogName, LogDescription, RecordsLoaded, " \
                   "RecordsCreated, RecordsUpdated, Start, Finish, Logged) VALUES ( " \
                    + etl_run_id + ", " \
                   "'" + log_name + "', " \
                   "'" + log_desc + "', " \
                   + str(rec_load) + ", " \
                   + str(rec_crea) + ", " \
                   + str(rec_updt) + ", " \
                   "'" + start + "', " \
                   "'" + finish + "', " \
                   "GETDATE())")
    cnn.commit()

    return 1;

# Log an error
def logError(etl_run_id, etl_state, exc, cnn):
    # log error to the db with the passed ETL Run ID

    cursor = cnn.cursor()
    cursor.execute("INSERT INTO Audit.ETLError ( " \
                   "ETLRunID, ETLState, Error, Logged) VALUES ( " \
                   + etl_run_id + ", " \
                   "'" + etl_state + "', " \
                   "'" + str(exc).replace("'", "''") + "', " \
                   "GETDATE())")
    cnn.commit()
    
    return 1
