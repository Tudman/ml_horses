### python 3.3.
### This is the barebones of scraping data to populate a db for ml training and prediction.
### The basic idea here is:
### get the list of states/tracks, and blindly loop through days and days, testing for exceptions, and s
### If no excptions, parse the page and loop through the races collecting data
### Data can go into a python native DB: Sqlite3.
### ML stuff to come later.


import bs4 # this is the html parser, beautiful soup. Is a seperate module.
import urllib.request #this is python's in-distro web fetcher.
import re
import datetime



#using sample page
# this is naughty though - no approvals around accessing this data
# This data source also doesn't have race times, so this needs to be estimated, using a likely finish time distribution (probably need to be made from scratch) with some testing around the sensitivity to the ml results.
# example: page = "http://risa.com.au/FreeFields/Results.aspx?Key=2013Nov28,NSW,Wyong"


# database connection string.
# will probably need to change depending on what db engine we're using
#from race_data_mssql import *
#dbconnstr = 'DRIVER={SQL Server Native Client 10.0};SERVER=.\SQLExpress;DATABASE=dw;UID=etl;PWD=etlpass'

from race_data_postgres import *
dbconnstr = "host ='localhost' dbname='dw' user='etl' password='etl'"



# function definitions. prob put these in a separate library file at some stage

def getRunnerDetails(row, race_id, winning_time, con):


    #ST - some changes made here -
	### - added winning_time var as arg.
	### - dictionary to margin code into lengths.
	### - will convert lengths into seconds.
	### - added the start on rolling margin logic for first 3 horses.
	### will require some restructuring, as row's aren't entirely independent, but are treated as such and winning time is worked out later.
	### I'll maybe create and instantiate a class to hold some values a bit outside the program flow.
	
	'''
    rolling_margin = 0
	
	recode_time_lookup = dict('HH' : 0, 'NK' : 0, 'SHH' : 0, 'HD' : 0, 'NS' : 0, 'HN' : 0, 'LH' : 0, 'LN' : 0, 'SN' : 0, 'SH' :0, 'LR' : 0, 'DH' : 0, 'DQ' : 0)
    '''
	
    td_tags = row.find_all('td')
    finish_position = td_tags[0].text.replace('\\r\\n', '').strip()
		
    margin_to_winner = td_tags[1].text.replace('\\r\\n', '').strip()
    
	'''	
	if finish_position = 2 or 3:
	    margin_to_winner = rolling_margin + margin_to_winner		
	    rolling_margin = margin_to_winner
	'''
    #convert winning_time to integer based in seconds. Maybe better done earlier and passed as an arg into this function
	'''
	winning_time = winning_time[:-1:]
	time_convert = datetime.datetime.strptime(t, "%M:%S.%f")
	winning_time = (time_convert.minute * 60) + time_convert.second + time_convert.microsecond/1000000
	race_time = winning_time + (margin_to_winner * 0.14)
	'''
	
	
	name_elements = td_tags[2].text.replace('\\r\\n', '').strip().split('.')
    runner_number = name_elements[0].strip()
    runner_name = name_elements[1].strip()
    trainer_name = td_tags[3].text.replace('\\r\\n', '').strip()
    jockey_name = td_tags[4].text.replace('\\r\\n', '').strip()
    starting_price = td_tags[5].text.replace('\\r\\n', '').strip()
    weight = td_tags[6].text.replace('\\r\\n', '').strip()

    # save to db
    return saveRunnerDetails([finish_position, runner_number, runner_name, trainer_name,
                                  jockey_name, margin_to_winner, race_time, '-1', weight, '',
                                  starting_price], race_id, 'Racenet', con)

def getRaceDetails(race_header, meet_id, con):
    # save to db details of an individual race
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
    race_time = race_details_elements[4]
    race_comments = ''
    if len(race_details_elements) >= 8:
        race_comments = race_details_elements[7]
    
    return saveRaceDetails([race_number.strip(), '1-1-1900',
                                race_name.strip(), race_distance.strip(),
                                race_details.strip(), track_condition.strip(),
                                race_time.strip(), '',
                                race_comments.strip()], meet_id, 'Racenet', con)


def getRaceMeetDetails(page_title, state, con):
    # save to db details of the race meet;
    #Location, State and Date. Other details we don't have.
    title_words = page_title.split('Race Results')
    location = title_words[0]
    temp = title_words[1].split(' ')
    day = temp[2]
    month = temp[3]
    year = temp[4]

    # work out State to save in db
    if state == 'nsw':
        short_state = 'NSW'
    elif state == 'victoria':
        short_state = 'VIC'
    elif state == 'queensland':
        short_state = 'QLD'
    elif state == 'act':
        short_state = 'ACT'
    elif state == 'south-australian':
        short_state = 'SA'
    elif state == 'western-australian':
        short_state = 'WA'
    elif state == 'northern-territory':
        short_state = 'NT'
    elif state == 'tasmanian':
        short_state = 'TAS'

    # Save these details in the db
    return saveMeetDetails([year, month, day, short_state, location,
                            '0', '', '', '', '', '','', ''], 'Racenet', con)

def needToGetMeet(pageURL, state, con):
    # get the location and date from pageURL
    url_bits = pageURL.split('/')
    location = url_bits[2]
    location = location.replace('-', ' ')
    date_thing = url_bits[3]

    # decode State
    if state == 'nsw':
        short_state = 'NSW'
    elif state == 'victoria':
        short_state = 'VIC'
    elif state == 'queensland':
        short_state = 'QLD'
    elif state == 'act':
        short_state = 'ACT'
    elif state == 'south-australian':
        short_state = 'SA'
    elif state == 'western-australian':
        short_state = 'WA'
    elif state == 'northern-territory':
        short_state = 'NT'
    elif state == 'tasmanian':
        short_state = 'TAS'
    #print('date thing: ' + date_thing)

    if checkMeetExists([location, short_state, date_thing], con):
        #print('meet exists')
        return 0
    else:
        #print('meet does not exist')
        return 1
    return not checkMeetExists([location, short_state, date_thing], con)

def getMeet(pageURL, state, con):
    # build URL, get print version
    if needToGetMeet(pageURL, state, con):
        #print('getting meet')
        pageURL = pageURL.replace('horse-racing-results', 'horse-racing-results-print')
        pageURL = "http://www.racenet.com.au" + pageURL
        # put a try-catch around this as sometimes there's an error
        try:
            page_data = urllib.request.urlopen(pageURL)
            # (I think something here was breaking it ST) page_data = page_data.replace('</b> </b>', '</b>')
            # (I think something here was breaking it ST) page_data = page_data.replace('        </b></td>', '        </td>')
            soup = bs4.BeautifulSoup(page_data)
            meet_id = getRaceMeetDetails(soup.title.text, state, con)
            # now get race details
            table_rows = soup.find_all('tr')
            for row in table_rows:
                # if the row's class == "again_bg_table" it's a runner row
                if row.th != None:
                    # start of race header. get Race Details
                    race_id = getRaceDetails(row, meet_id, con)
                elif row.has_attr('class'):
                    if row['class'][0] == 'again_bg_table':
                        # its a runner row. get Runner details
                        getRunnerDetails(row, race_id, con)

        except Exception:
            print(str(Exception))
            print(pageURL)
            #raise
            #sys.exit('stopped.')
        #finally:
        #    return 1


def getLocationMeetDates(pageURL, state, con):
    # get the location results page and iterate through the meet dates
    # clean up the URL if necessary
    pageURL = pageURL.replace('\t', '') # get rid of TAB characters
    pageURL = "http://www.racenet.com.au" + pageURL
    page_data = urllib.request.urlopen(pageURL)
    soup = bs4.BeautifulSoup(page_data)
    results_links = soup.h1.parent.find_all('a')
    for link in results_links:
        getMeet(link.get('href'), state, con)




# actual script starts here

# get database connection
con = getConnection(dbconnstr)
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
            getLocationMeetDates(page, state, con)
