### python 3.3.
### This is the barebones of scraping data to populate a db for ml training and prediction.
### The basic idea here is:
### get the list of states/tracks, and blindly loop through days and days, testing for exceptions.
### If no excptions, parse the page and loop through the races collecting data
### Data can go into a python native DB: Sqlite3.
### ML stuff to come later.
# yay, us

import bs4 # this is the html parser, beautiful soup. Is a seperate module.
import urllib.request #this is pythons in-distro the web fetcher.

#building sample page cycle and test 

list_states_tracks = ['NSW'] # Just start with NSW for now          ,'VIC','QLD','SA','WA','NT','TAS','ACT']
list_months = ['Oct','Nov','Dec'] #just focus on these three months, for now.
list_tracks = ['Canterbury Park','Kembla Grange (Trial)','Muswellbrook (Trial)',
               'Wyong','Kensington','Orange (Trial)','Orange','Kembla Grange',
               'Moree','Moree (Trial)','Rosehill Gardens','Moruya','Moruya (Trial)',
               'Taree (Trial)','Taree','Lismore (Trial)','Lismore','Warwick Farm (Trial)',
               'Wyong (Trial)','Rosehill Gardens (Trial)','Tamworth','Tamworth (Trial)',
               'Wagga (Trial)','Wagga'] # NSW tracks.



page_start = "http://risa.com.au/FreeFields/Results.aspx?Key=2013" #Nov28,NSW,Wyong"

#making connection list to get a feel for the effectiveness of the dumb cycling.

response_table = ['']*len(list_states_tracks)*len(list_months)*len(list_tracks)*31

counter = 0

for i in range(1,len(list_months)):
    for j in range(1 ,32):
        for k in range(1,len(list_tracks)):

            address_suffix = list_months[i] + str(j) + ",NSW," + list_tracks[k]
            counter += 1
            page_address = "http://risa.com.au/FreeFields/Results.aspx?Key=2013" + address_suffix
          
            

#page_data = urllib.request.urlopen(page)
#soup = bs4.BeautifulSoup(page_data)

#number of races

#number_of_races = len(soup.select('a[name]')) - 1

# output: <a name="Race2">Race 2 - 1:55PM ENWARE AUSTRALIA MAIDEN PLATE (1350 METRES)</a>

#write looping code here...

#race_number = str(soup.select('a[name]')[1])[15 + 1:15 + 1 + 6] #needs chr length of looping integer to handle i 9 -> 10


