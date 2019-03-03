''' Script to loop through all the aiports in the flightradar24 list and print their data in a file.
Date: 26-02-2019
'''

import requests
import bs4
import time

start_time = time.time()

# Creating txt output file
file = open('Airports_list.csv','w', encoding='utf-8')
file_format = '{0},{1},{2},{3},{4},{5}\n'

# HTTP request to get the webpage that contains the list with all the covered cities
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0"}

main_resp = requests.get('https://www.flightradar24.com/data/airports',  headers = headers)
main_resp.raise_for_status()

# Parsing all the cities and looping though them to check the airports
cities_table = bs4.BeautifulSoup(main_resp.text, 'html.parser').select('#tbl-datatable td a')
i = 1   # Counter
                                
file.write(file_format.format('City', 'Airport', 'IATA Siglum', 'URL', 'Latitude', 'Longitude'))
for city in cities_table:
    
    url = city['href']
    
    if url == '#' or len(city.text) < 1: # Skip empty nodes
        continue
            
    resp = requests.get(url, headers = headers)
    resp.raise_for_status()
    
    airports_table = bs4.BeautifulSoup(resp.text, 'html.parser').select('#tbl-datatable td a')
    for airport in airports_table:
        
        if len(airport.text) > 0:
            airport_name = airport.text
            
            file.write(file_format.format(
                    city.text.strip(), airport_name[0:-11].strip(), airport_name[-10:-2].strip(), 
                    airport['href'], airport['data-lat'], airport['data-lon']))
            print('Request ' + str(i) + '\n')
            i = i + 1

file.close()
elapsed_time = time.time() - start_time
print('The total elapsed time was ' + str(elapsed_time) + 's.')