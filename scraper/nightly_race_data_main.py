### python 3.3.



import bs4 # this is the html parser, beautiful soup. Is a seperate module.
import urllib.request #this is python's in-distro web fetcher.
import re

#using sample page
# this is naughty though - no approvals around accessing this data
# This data source also doesn't have race times, so this needs to be estimated, using a likely finish time distribution (probably need to be made from scratch) with some testing around the sensitivity to the ml results.
# example: page = "http://risa.com.au/FreeFields/Results.aspx?Key=2013Nov28,NSW,Wyong"


# database connection string.
# will probably need to change depending on what db engine we're using
from race_data_mssql import *
dbconnstr = 'DRIVER={SQL Server Native Client 11.0};SERVER=.\SQLExpress;DATABASE=dw;UID=etl;PWD=etlpass'

#from race_data_pgsql import *
#dbconnstr = 'dbname=etltest user=etl password=etlpass'



# function definitions. prob put these in a separate library file at some stage

def getRaceLocationAndDate(pageURL, state):
    # extract the race meet location name and date from the URL string passed
    # example of pageURL: "http://risa.com.au/FreeFields/Results.aspx?Key=2013Nov03,SA,Morphettville Parks"
    meetYear = pageURL[47:51]
    meetMonth = pageURL[51:54]
    meetDay = pageURL[54:56]
    meetState = state
    meetLocation = pageURL[58 + len(state):100]
    meetTrial = 0
    # check if Trial, and remove from meetLocation if there
    match = re.search('Trial', meetLocation)
    if match:
        meetLocation = meetLocation[0:len(meetLocation) - 6]
        meetTrial = 1
    return [meetYear, meetMonth, meetDay, meetState, meetLocation, meetTrial]

def needToGetRaceMeet(pageURL, state, dbconnstr):
    # get details about the race meet
    # then check the database to see if we've got data for that meet yet
    meet_details = getRaceLocationAndDate(pageURL, state)
    # return 1 if we need to get it, else 0
    return checkMeetExists(meet_details, dbconnstr)

def getRaceMeetDetails(soup, meetDetails, dbconnstr):
    # save to db details of the race meet;
    # location, date, rail position, weather, penetrometer(!) etc
    #print(soup)
    header_bottom_div = soup.find('div', class_='race-venue-bottom')
    hdr_details = header_bottom_div.find('div', class_='col1').text
    print(hdr_details)
    hdr_details = hdr_details.replace('Rail Position:', '')
    hdr_details = hdr_details.replace('Track Condition:', '|')
    hdr_details = hdr_details.replace('Track Type:', '|')
    #hdr1_elements = hdr_details.split('|')
    #hdr_details = header_bottom_div.find('div', class_='col2').text
    hdr_details = hdr_details.replace('Weather:', '|')
    hdr_details = hdr_details.replace('Penetrometer:', '|')
    hdr1_elements = hdr_details.split('|')
    hdr_details = header_bottom_div.find('div', class_='col2').text
    hdr_details = hdr_details.replace('Results Last Published:', '')
    header_comments = soup.find('div', class_='comments').text

##    print(meetDetails[0])
##    print(meetDetails[1])
##    print(meetDetails[2])
##    print(meetDetails[3])
##    print(meetDetails[4])
##    print(meetDetails[5])
##    print(hdr1_elements)
##    print(hdr1_elements[0])
##    print(hdr1_elements[1])
##    print(hdr1_elements[2])
##    print(hdr1_elements[3])
##    print(hdr1_elements[4])
##    print(hdr_details)
    #print(hdr_details)
    #print(hdr2_elements[2])
    #print(header_comments)
    
    # Save these details in the db
    return saveMeetDetails([meetDetails[0], meetDetails[1], meetDetails[2], meetDetails[3],
                                meetDetails[4], str(meetDetails[5]), hdr1_elements[0].strip(),
                                hdr1_elements[1].strip(), hdr1_elements[2].strip(),
                                hdr1_elements[3].strip(), hdr1_elements[4].strip(),
                                hdr_details.strip(), header_comments.strip()], 'RISA', dbconnstr)



def race_name_tag(tag):
    return tag.has_attr('name') and tag.name == 'a'

def getRaceDetails(race_header, meet_id, dbconnstr):
    # save to db details of an individual race
    race_name = race_header.find(race_name_tag).text
    race_name = race_name.replace(' - ', '|', 1) # only replace 1st instance
    race_name = race_name.replace('AM ', 'AM|')
    race_name = race_name.replace('PM ', 'PM|')
    race_name = race_name.replace('(', '|')
    race_name = race_name.replace(')', '')
    race_name_elements = race_name.split('|')
    # get other details from the second <tr>
    race_details_tag = race_header.tr.next_sibling.next_sibling
    race_details = race_details_tag.td.text
    race_details = race_details.replace('Track Condition:', '|')
    race_details = race_details.replace('Time:', '|')
    race_details = race_details.replace('Timing Method:', '|')
    race_details = race_details.replace('Official Comments:', '|')
    race_details_elements = race_details.split('|')

    last_split_time = ''
    official_comments = ''
    if len(race_details_elements) > 3:
        last_split_time = race_details_elements[3].strip()
    if len(race_details_elements) > 4:
        official_comments = race_details_elements[4].strip()

    # save these details in db
    return saveRaceDetails([race_name_elements[0].strip(), race_name_elements[1].strip(),
                                race_name_elements[2].strip(), race_name_elements[3].strip(),
                                race_details_elements[0].strip(), race_details_elements[1].strip(),
                                race_details_elements[2].strip(), last_split_time,
                                official_comments], meet_id, 'RISA', dbconnstr)



def getRunnerDetails(race_runner, race_id, dbconnstr):
    # save to db details of an individual runner in a race
    # if we've got a 'class' attribute, we're looking at a runner row.
    if race_runner.has_attr('class'):
        td_tag = race_runner.td
        td_tag = td_tag.next_sibling.next_sibling
        finish_position = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling.next_sibling.next_sibling
        runner_number = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling
        runner_name = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling
        trainer_name = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling
        jockey_name = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling
        margin_to_winner = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling
        barrier = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling
        weight = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling
        penalty = td_tag.text
        td_tag = td_tag.next_sibling.next_sibling
        starting_price = td_tag.text

        # remove 'e' from runner number if there... denotes emergency?
        runner_number = runner_number.replace('e', '')
        
        return saveRunnerDetails([finish_position, runner_number, runner_name, trainer_name,
                                  jockey_name, margin_to_winner, barrier, weight, penalty,
                                  starting_price], race_id, 'RISA', dbconnstr)

    return 1

def getRaceRunners(race_runners, race_id, dbconnstr):
    # iterate through runners
    runners_soup = bs4.BeautifulSoup(str(race_runners))
    for runners_row in runners_soup.findAll('tr'):
        getRunnerDetails(runners_row, race_id, dbconnstr)

    return 1

def getRaceMeetResults(pageURL, state, dbconnstr):
    # get the race meet details
    # prints for temporary debugging purposes
    #print(pageURL)

    # get page specified in pageURL (replace spaces with %20 for correct url formatting)
    page_data = urllib.request.urlopen(pageURL.replace(' ', '%20') + '&BodyClass=PrintFriendly')
    soup = bs4.BeautifulSoup(page_data)

    # get race meet details data
    meetDetails = getRaceLocationAndDate(pageURL, state)
    meet_id = getRaceMeetDetails(soup, meetDetails, dbconnstr)

    flag_runner_table_next = 0
    race_id = ''

    # iterate through race meet races
    for table_tag in soup.findAll('table'):
        # see if this table_tagle flagged as runners
        if flag_runner_table_next == 1:
            # get the runners for the race
            getRaceRunners(table_tag, race_id, dbconnstr)
            # reset flag
            flag_runner_table_next = 0

        # see if the table is a race heading
        elif table_tag['class'] == ['race-title']:
            # get details of the race
            race_id = getRaceDetails(table_tag, meet_id, dbconnstr)
            # flag the next table tag is going to be the runners for this race
            flag_runner_table_next = 1

    return 1



# actual script starts here

states = ['SA']#, 'QLD']#, 'NSW', 'WA', 'VIC', 'TAS', 'ACT', 'NT']
for state in states:
    # get current results page for each state
    page = "http://risa.com.au/FreeFields/Calendar_Results.aspx?State=" + state
    page_data = urllib.request.urlopen(page)
    soup = bs4.BeautifulSoup(page_data)

    # iterate through results pages
    for link in soup.find_all('a', text='Available Now'):
        # ignore the risaform links
        if link.get('href') != 'http://www.risaform.com.au/Racing/default.aspx':
            # check if we've got this race meet's data yet
            if needToGetRaceMeet('http://risa.com.au' + link.get('href'), state, dbconnstr):
                getRaceMeetResults('http://risa.com.au' + link.get('href'), state, dbconnstr)











