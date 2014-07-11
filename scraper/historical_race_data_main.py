### python 3.3.
# Load Historical Races Data
# Data source is racenet.com.au


import bs4 # this is the html parser, beautiful soup. Is a seperate module.
import urllib.request #this is python's in-distro web fetcher.
import re
import datetime
import os.path


# database connection string.
# change depending on using pgsql or mssql
#from race_data_mssql import *
#dbconnstr = 'DRIVER={SQL Server Native Client 11.0};SERVER=.\SQLExpress;DATABASE=dw;UID=etl;PWD=etlpass'

from race_data_pgsql import *
dbconnstr = 'dbname=dw user=etl password=etlpass'



# function definitions. prob put these in a separate library file at some stage

def getRunnerDOBetc(pageURL, cnn):
    # get the individual horse's page and get it's essential measurements
    try: 
        page_data = str(urllib.request.urlopen(pageURL).read())
        #soup = bs4.BeautifulSoup(page_data)
        # get age. Look for "YO. ("
        dob_index = page_data.find("YO. (", 0)
        dob = "1-Jan-1900"
        if (dob_index > -1):
            # get next 10 characters and strip off ")" if there
            dob = page_data[dob_index + 5:dob_index + 15].strip(")")
            dob_parts = dob.split("/")

            # prob should error handle here...
            dob_parts[1] = recode_month_lookup[dob_parts[1]]
            dob = dob_parts[0] + "-" + dob_parts[1] + "-" + dob_parts[2]
            
        # Update db with results.
        saveRunnerOtherDetails(dob, pageURL, cnn)
    
    except Exception as exc:
        logError(etl_run_id, "getRunnerDOBetc: " + pageURL, exc, cnn)

def getRunnersPageData(cnn):
    # get further runner data from their individual pages
    
    # get distinct set of URLs to runner pages
    rows = getRunnerURLs(cnn)
    # log the number of runner pages we're getting
    etlAuditLog(etl_run_id, ["Starting loading Runner Data", "Got unique runner pages", len(rows)], cnn)
    row_count = 0
    # iterate and get data from page
    for row in rows:
        getRunnerDOBetc(row[0], cnn)
        row_count = row_count + 1
        if row_count % 1000 == 0:
            etlAuditLog(etl_run_id, ["Loading Runner Data", "Loaded " + str(row_count) + " of " + str(len(rows)) + " runners.", row_count], cnn)
    
    
def decodeMarginToSeconds(margin_desc):
    # convert the descriptive margin passed to a number/fraction of seconds.
    margin_seconds = 0
    if margin_desc in recode_time_lookup:
        margin_seconds = recode_time_lookup[margin_desc]
    else:
        margin_seconds = float(margin_desc)
    
    return margin_seconds

def getRunnerDetails(row, race_id, winning_time, margin_to_2nd, cnn):
    # get horse details and save to db.
    try:
        td_tags = row.find_all('td')
        finish_position = td_tags[0].text.replace('\\r\\n', '').strip()
        margin_desc = td_tags[1].text.replace('\\r\\n', '').strip()
        name_elements = td_tags[2].text.replace('\\r\\n', '').replace("\\", "").strip().split('.')
        runner_number = name_elements[0].strip()
        runner_name = name_elements[1].strip()
        runner_url = td_tags[2].a['href'].strip()
        # strip out the "\'" from any URLs
        runner_url = runner_url.replace("\\'", "")
        runner_dob = '1-Jan-1900'
        # change routine for getting DOB
        #if not checkRunnerExists(runner_url, cnn):
            # only get dob if this is a runner we havn't seen before
            #runner_dob = getRunnerDOBetc(runner_url)
        
        trainer_name = td_tags[3].text.replace('\\r\\n', '').replace("\\", "").strip()
        jockey_name = td_tags[4].text.replace('\\r\\n', '').replace("\\", "").strip()
        starting_price = td_tags[5].text.replace('\\r\\n', '').strip()
        weight = td_tags[6].text.replace('\\r\\n', '').strip()

        # calculate race time:
        if finish_position == '1':
            race_time = "{0:0.2f}".format(winning_time)
        elif finish_position == '2':
            margin_to_2nd = decodeMarginToSeconds(margin_desc)
            race_time = "{0:0.2f}".format(float(winning_time) + margin_to_2nd)
        elif finish_position == '3':
            race_time = "{0:0.2f}".format(float(winning_time) + margin_to_2nd +
                            decodeMarginToSeconds(margin_desc))
        else:
            race_time = "{0:0.2f}".format(float(winning_time) +
                            decodeMarginToSeconds(margin_desc))
        
        # save to db
        saveRunnerDetails([finish_position, runner_number, runner_name, trainer_name,
                                      jockey_name, margin_desc, '-1', weight, '',
                                      starting_price, race_time, runner_dob, runner_url],
                                      race_id, 'Racenet', etl_run_id, cnn)
        # return the margin to 2nd place as it might be needed in the next calc!
        return margin_to_2nd

    except Exception as exc:
        logError(etl_run_id, "getRunnerDetails", exc, cnn)
        return margin_to_2nd

def getRaceDetails(race_header, meet_id, cnn):
    # save to db details of an individual race
    try:
        race_name = race_header.find('b').text
        race_name_elements = race_name.split(' - ')
        if len(race_name_elements) == 3:
            race_number = race_name_elements[0]
            race_name = race_name_elements[1]
            race_distance = race_name_elements[2]
        else:
            race_number = race_name_elements[0]
            race_name = race_name_elements[1] + ' - ' + race_name_elements[2]
            race_distance = race_name_elements[3]

        # move to next tr to get more race details
        race_header2 = race_header.next_sibling.next_sibling
        race_details = race_header2.text
        race_details = race_details.replace('Class:', '|')
        race_details = race_details.replace('Track:', '|')
        race_details = race_details.replace('Time:', '|')
        race_details = race_details.replace('Class:', '|')
        race_details = race_details.replace('\\r\\n', '|')
        race_details_elements = race_details.split('|')
        race_details = race_details_elements[2]
        track_condition = race_details_elements[3]
        race_time = race_details_elements[4].strip().rstrip('.')    # strip the trailing '.'
        race_comments = ''
        if len(race_details_elements) >= 8:
            race_comments = race_details_elements[7]

        # convert race time to seconds
        time_convert = datetime.datetime.strptime(race_time, "%M:%S.%f")
        winning_time = (time_convert.minute * 60) + time_convert.second + time_convert.microsecond / 1000000
        
        return [saveRaceDetails([race_number.strip(), '1-1-1900',
                                    race_name.strip(), race_distance.strip(),
                                    race_details.strip(), track_condition.strip(),
                                    race_time.strip(), '',
                                    race_comments.strip()], meet_id, 'Racenet',
                                    etl_run_id, cnn),
                winning_time]
    
    except Exception as exc:
        logError(etl_run_id, "getRaceDetails", exc, cnn)
        raise

def getRaceMeetDetails(page_title, state, cnn):
    # save to db details of the race meet;
    #Location, State and Date. Other details we don't have.
    title_words = page_title.split('Race Results')
    location = title_words[0]
    temp = title_words[1].split(' ')
    day = temp[2]
    month = temp[3]
    year = temp[4]

    # work out State to save in db
    short_state = recode_state_lookup[state]

    # Save these details in the db
    return saveMeetDetails([year, month, day, short_state, location,
                            '0', '', '', '', '', '','', ''], 'Racenet',
                           etl_run_id, cnn)

def needToGetMeet(pageURL, state, cnn):
    # get the location and date from pageURL
    url_bits = pageURL.split('/')
    location = url_bits[2]
    location = location.replace('-', ' ')
    date_thing = url_bits[3]
    
    # decode state...
    short_state = recode_state_lookup[state]

    if checkMeetExists([location, short_state, date_thing], cnn):
        return 0
    else:
        return 1

def getMeet(pageURL, state, cnn):
    # build URL, get print version
    if needToGetMeet(pageURL, state, cnn):
        pageURL = pageURL.replace('horse-racing-results', 'horse-racing-results-print')
        pageURL = "http://www.racenet.com.au" + pageURL
        # put a try-catch around this as sometimes there's an error
        try:
            page_data = str(urllib.request.urlopen(pageURL).read())
            page_data = page_data.replace('</b> </b>', '</b>')
            page_data = page_data.replace('        </b></td>', '        </td>')
            soup = bs4.BeautifulSoup(page_data)
            meet_id = getRaceMeetDetails(soup.title.text, state, cnn)
            count_races = 0
            # now get race details
            table_rows = soup.find_all('tr')
            for row in table_rows:
                # if the row's class == "again_bg_table" it's a runner row
                if row.th != None:
                    # start of race header. get Race Details
                    race_data = getRaceDetails(row, meet_id, cnn)
                    race_id = race_data[0]
                    winning_time = race_data[1]
                    margin_to_2nd = 0
                    count_races += 1 # increment counter for log
                elif row.has_attr('class'):
                    if row['class'][0] == 'again_bg_table':
                        # its a runner row. get Runner details
                        margin_to_2nd = getRunnerDetails(row, race_id, winning_time, margin_to_2nd, cnn)

            # log our successful import of the meet
            etlAuditLog(etl_run_id, ["loaded Race Meet", pageURL, count_races], cnn)

        except Exception as exc:
            #print(exc)
            #print(pageURL)
            logError(etl_run_id, pageURL, exc, cnn)
            #raise


def getLocationMeetDates(pageURL, state, cnn):
    # get the location results page and iterate through the meet dates
    # clean up the URL if necessary
    pageURL = pageURL.replace('\t', '') # get rid of TAB characters
    pageURL = "http://www.racenet.com.au" + pageURL
    page_data = urllib.request.urlopen(pageURL)
    soup = bs4.BeautifulSoup(page_data)
    results_links = soup.h1.parent.find_all('a')
    for link in results_links:
        getMeet(link.get('href'), state, cnn)




# actual script starts here

# some global recode things for common data
global recode_month_lookup
recode_month_lookup = {"01":"Jan", "02":"Feb", "03":"Mar", "04":"Apr",
                       "05":"May", "06":"Jun", "07":"Jul", "08":"Aug",
                       "09":"Sep", "10":"Oct", "11":"Nov", "12":"Dec"}
global recode_state_lookup
recode_state_lookup = {"nsw":"NSW", "victoria":"VIC", "queensland":"QLD",
                       "act":"ACT", "south-australian":"SA", 
                       "western-australian":"WA", "northern-territory":"NT",
                       "tasmanian":"TAS"}
global recode_time_lookup
recode_time_lookup = {'HH':0.1, 'NK':0.3, 'SHH':0.1, 'HD':0.2, 'NS':0.1,
                      'HN':0.2, 'LH':0.3, 'LN':0.4, 'SN':0.2, 'SH':0.2,
                      'LR':0.5, 'DH':0.2, 'DQ':0}

# get database connection
cnn = getConnection(dbconnstr)

# true/false switch to drop and recreate tables.
rebuild = True
if rebuild == True:
    rebuild_stage_db(cnn)

# Start Audit Log
global etl_run_id
etl_run_id = etlAuditStart(os.path.basename(__file__), cnn)
# get page with all the race locations in australia
states = ['nsw', 'victoria', 'queensland', 'act', 'south-australian', 'western-australian', 'northern-territory', 'tasmanian']
for state in states:
    # get list of race locations for each state
    page ="http://www.racenet.com.au/raceclub-category-pages/" + state + "-racing-clubs.asp"
    print(page)
    page_data = urllib.request.urlopen(page)
    soup = bs4.BeautifulSoup(page_data)

    # iterate through race location pages
    for location in soup.find_all('h2'):
        # get results page
        page = location.a.get('href')
        page_data = urllib.request.urlopen(page)
        soup1 = bs4.BeautifulSoup(page_data)
        menudiv = soup1.find('div', id = "left_menu_club")
        if menudiv == None:
            menudiv = soup1.find('div', id = "left_menu")
        # if still None, something is crap
        if menudiv == None:
            # Hawkesbury and "Toowoomba cushion raceclub" seem to have special pages. well fuck them
            print('STUPID SPECIAL PAGE ' + page)
        else:
            if menudiv.li == None:
                page = menudiv.LI.a.get('href')
            else:
                page = menudiv.li.a.get('href')
            getLocationMeetDates(page, state, cnn)

# log success
etlAuditLog(etl_run_id, ["Finished Loading Race Meets", "Finished Loading Race Meets; starting loading Runner Page data", 0], cnn)
# get other Runner data elements from the individual Runner pages
getRunnersPageData(cnn)

# close off Audit log
etlAuditFinish(etl_run_id, cnn)
