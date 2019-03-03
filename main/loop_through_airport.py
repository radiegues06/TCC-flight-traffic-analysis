# -*- coding: utf-8 -*-
"""
Created on Tue Feb 26 14:49:33 2019

@author: Rafael Amancio Diegues

Collect all arrivals and depart data for each airport
"""

import requests
import threading
import time

# Loop through all the airports

file = open('Airports_list.csv', 'r', encoding = 'utf-8')

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0"}

start_time = time.time()

parameters  = {'code': '', 'plugin[]': '', 'plugin-setting[schedule][mode]': 'arrivals', 
         'plugin-settings[schedule][timestamp]': '1551203235', 'page': '-4', 'limit': '100', 
         'token': 'OqXb3FuvDi5WJmBHxxrEo9g2eNzkmB_mZcosa0WxKg8'}

i = 1
for line in file:
    words = line.split(',')
    siglum = words[2][0:3]
    
    
    parameters['code'] = siglum
    resp = requests.get('https://api.flightradar24.com/common/v1/airport.json',  
                             headers = headers, params = parameters).json()
    
    try:
        json = resp['result']['response']['airport']['pluginData']['schedule']['arrivals']['data']
        
        print(
                str(i) + ' - ' + json[0]['flight']['status']['generic']['status']['text']
                )
        i = i + 1
    except:
        continue
    
elapsed_time = time.time() - start_time
print('The total elapsed time was ' + str(elapsed_time))