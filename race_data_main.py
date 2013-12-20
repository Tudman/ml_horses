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

#using sample page
# this is naughty though - no approvals around accessing this data
# This data source also doesn't have race times, so this needs to be estimated, using a likely finish time distribution (probably need to be made from scratch) with some testing around the sensitivity to the ml results.
# example: page = "http://risa.com.au/FreeFields/Results.aspx?Key=2013Nov28,NSW,Wyong"


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

def needToGetRaceMeet(pageURL, state):
    # get details about the race meet
    # then check the database to see if we've got data for that meet yet
    meetDetails = getRaceLocationAndDate(pageURL, state)
    # TODO: check db to see if we've got this race meet yet
    # return 1 if we need to get it, else 0
    return 1

def getRaceMeetDetails(soup, meetDetails):
    # save to db details of the race meet;
    # location, date, rail position, weather, penetrometer(!) etc
    header_bottom_div = soup.find('div', class_='race-venue-bottom')
    hdr_details = header_bottom_div.find('div', class_='col1').text
    hdr_details = hdr_details.replace('Rail Position:', '')
    hdr_details = hdr_details.replace('Track Condition:', '|')
    hdr_details = hdr_details.replace('Track Type:', '|')
    hdr1_elements = hdr_details.split('|')
    hdr_details = header_bottom_div.find('div', class_='col2').text
    hdr_details = hdr_details.replace('Weather:', '')
    hdr_details = hdr_details.replace('Penetrometer:', '|')
    hdr_details = hdr_details.replace('Results Last Published:', '|')
    hdr2_elements = hdr_details.split('|')
    header_comments = soup.find('div', class_='comments').text

    # Save these details in the db
    print('Year: ' + meetDetails[0])
    print('Month: ' + meetDetails[1])
    print('Day: ' + meetDetails[2])
    print('State: ' + meetDetails[3])
    print('Location: ' + meetDetails[4])
    print('IsTrial?: ' + str(meetDetails[5]))
    print('Rail Position: ' + hdr1_elements[0].strip())
    print('Track Condition: ' + hdr1_elements[1].strip())
    print('Track Type: ' + hdr1_elements[2].strip())
    print('Weather: ' + hdr2_elements[0].strip())
    print('Penetrometer: ' + hdr2_elements[1].strip())
    print('Results Last Published: ' + hdr2_elements[2].strip())
    print('Comments: ' + header_comments.strip())
    return 1

def race_name_tag(tag):
    return tag.has_attr('name') and tag.name == 'a'

def getRaceDetails(race_header):
    # save to db details of an individual race
    race_name = race_header.find(race_name_tag).text
    race_name = race_name.replace(' - ', '|')
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
    race_details = race_details.replace('Last', '|Last')
    race_details = race_details.replace('Official Comments:', '|')
    race_details_elements = race_details.split('|')

    # save these details in db
    print('Race Number: ' + race_name_elements[0].strip())
    print('Race Time: ' + race_name_elements[1].strip())
    print('Race Name: ' + race_name_elements[2].strip())
    print('Race Distance: ' + race_name_elements[3].strip())
    print('Race Details: ' + race_details_elements[0].strip())
    print('Track Condition: ' + race_details_elements[1].strip())
    print('Winning Time: ' + race_details_elements[2].strip())
    if len(race_details_elements) > 3:
        print ('Last Split Time: ' + race_details_elements[3].strip())
    if len(race_details_elements) > 4:
        print ('Official Comments: ' + race_details_elements[4].strip())

    # return Race Number for reference when getting runners
    return race_name_elements[0].strip()

def getRaceRunnerDetails(race_runner):
    # save to db details of an individual runner in a race
    #print(type(race_runner))
    #if race_runner.tr['class'] == 'EvenRow' or race_runner.tr['class'] == 'OddRow':
    #    print('things')

    return 1

def getRaceRunners(race_runners):
    # iterate through runners
    runners_soup = bs4.BeautifulSoup(str(race_runners))
    for runners_row in runners_soup.findAll('tr'):
        getRaceRunnerDetails(runners_row)

    return 1

def getRaceMeetResults(pageURL, state):
    # get the race meet details
    # prints for temporary debugging purposes
    print(pageURL)

    # get page specified in pageURL (replace spaces with %20 for correct url formatting)
    page_data = urllib.request.urlopen(pageURL.replace(' ', '%20') + '&BodyClass=PrintFriendly')
    soup = bs4.BeautifulSoup(page_data)

    # get race meet details data
    meetDetails = getRaceLocationAndDate(pageURL, state)
    getRaceMeetDetails(soup, meetDetails)

    flag_runner_table_next = 0
    # iterate through race meet races
    for table_tag in soup.findAll('table'):
        # see if this table_tagle flagged as runners
        if flag_runner_table_next == 1:
            # get the runners for the race
            getRaceRunners(table_tag)
            # reset flag
            flag_runner_table_next = 0

        # see if the table is a race heading
        elif table_tag['class'] == ['race-title']:
            # get details of the race
            race_number = getRaceDetails(table_tag)
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
            if needToGetRaceMeet('http://risa.com.au' + link.get('href'), state):
                getRaceMeetResults('http://risa.com.au' + link.get('href'), state)








