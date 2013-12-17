### python 3.3.
### This is the barebones of scraping data to populate a db for ml training and prediction.
### The basic idea here is:
### get the list of states/tracks, and blindly loop through days and days, testing for exceptions, and s
### If no excptions, parse the page and loop through the races collecting data
### Data can go into a python native DB: Sqlite3.
### ML stuff to come later.


import bs4 # this is the html parser, beautiful soup. Is a seperate module.
import urllib.request #this is python's in-distro web fetcher.

#using sample page
# this is naughty though - no approvals around accessing this data
# This data source also doesn't have race times, so this needs to be estimated, using a likely finish time distribution (probably need to be made from scratch) with some testing around the sensitivity to the ml results. 

page = "http://risa.com.au/FreeFields/Results.aspx?Key=2013Nov28,NSW,Wyong"
page_data = urllib.request.urlopen(page)
soup = bs4.BeautifulSoup(page_data)

#number of races

number_of_races = len(soup.select('a[name]')) - 1

# output object of soup.select('a[name]')): <a name="Race2">Race 2 - 1:55PM ENWARE AUSTRALIA MAIDEN PLATE (1350 METRES)</a>

#write looping code here...

race_number = 
race_name = 
race_time =
race_length = 

