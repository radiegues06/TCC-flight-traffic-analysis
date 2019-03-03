# -*- coding: utf-8 -*-
"""
Created on Tue Feb 26 14:49:33 2019

@author: Rafael Amancio Diegues

Collect all arrivals and depart data for each airport, writing it all in a file
"""

import requests
import threading
import csv
import math
import time
import calendar

def resp_filter(siglum, page):
    resp = api_request()
    
    try:
        json_parse = resp.json()
    except:
        file_out.write('JSON reading error,' + str(resp.status_code) + ' , ' + resp.url + ' , ' +  siglum + ',' + str(page) + '\n')
        return
        
    json_node = json_parse['result']['response']['airport']['pluginData']['schedule'][parameters['plugin-setting[schedule][mode]']]['data']    
    
    for i in range(len(json_node)):
        flight = json_node[i]['flight']
        '''Filtering, excluing some data'''
        # Only the last day
        '''current_time = time.gmtime()
        yesterday_timestamp = calendar.timegm((current_time.tm_year, current_time.tm_mon, current_time.tm_mday - 1, 
                                               0, 0, 0, current_time.tm_wday -1, current_time.tm_yday, current_time.tm_isdst))
        today_timestamp = calendar.timegm((current_time.tm_year, current_time.tm_mon, current_time.tm_mday, 
                                               0, 0, 0, current_time.tm_wday, current_time.tm_yday, current_time.tm_isdst))'''
        
        # Printing flight if it has passed the filters
        
        print_flight(flight, json_parse['result']['request']['code'])
                

def airport_chunk(start,end):
    if test_mode == 1:
        parameters['page'] = 0
        parameters['plugin-setting[schedule][mode]'] = 'arrivals'
    
        for i in range(start,start+1):
            siglum = data_list[i][2][0:3].lower()
            parameters['code'] = siglum
            
            print('Airport number ' + str(i) )
            
            resp_filter(siglum, 0)
        
    else:
        for k in range(0,-11, -1):
            parameters['page'] = k
            
            for j in ['arrivals']:#, 'departures']:
                parameters['plugin-setting[schedule][mode]'] = j
                
                for i in range(start,end):
                    siglum = data_list[i][2][0:3].lower()
                    parameters['code'] = siglum
                    
                    print('Page number: ' + str(k) + ' Airport number ' + str(i) )
                    
                    resp_filter(siglum, k)

def print_flight(flight, siglum):
    
    # Mapping through the dict
    keys_list = [['identification','number','default'],
                        ['status','text'],
                        ['aircraft','model','text'],
                        ['aircraft','registration'],
                        ['owner','name'],
                        ['airline','name'],
                        ['airport','origin','code','iata'],
                        ['airport','origin','info','terminal'],
                        ['airport','origin','info','gate'],
                        ['airport','destination','code','iata'],
                        ['airport','destination','info','terminal'],
                        ['airport','destination','info','gate'],
                        ['time','scheduled','departure'],
                        ['time','scheduled','arrival'],
                        ['time','real','departure'],
                        ['time','real','arrival'],
                        ['time','other','duration']]
    
    print_parameters_list = []
    for keys in keys_list:
        try:
            print_par = nest_get(flight, keys)
        except:
            
            if 'iata' in keys:
                print_par = siglum
            else:
                print_par = 'Null'

        print_parameters_list.append(print_par)

    # Printing the file
    print_parameters_format = ''
    for i in range(len(keys_list)):
        print_parameters_format = print_parameters_format + '{d[' + str(i) + ']},'
    
    print_parameters_format = print_parameters_format[:-1] + '\r\n'
    
    file_out.write(print_parameters_format[:-1].format(d = print_parameters_list))

def nest_get(dic, Keys):
    result = dic
    for k in Keys:
        result = result[k]
    return result

def api_request():
    resp = requests.get('https://api.flightradar24.com/common/v1/airport.json', 
                        headers = headers, params = parameters)
    
    if resp.status_code != 200:
        resp = requests.get(str(resp.url), headers = headers)
    
    return resp
    

if __name__ == '__main__':

    start_time = time.time()
    
    test_mode = 0
    
    # Importing the csv file to a matrix
    file = open('Airports_list.csv', 'r')
    reader = csv.reader(file)
    data_list = list(reader)
    
    # Creating and setting new file where the dara will be written
    file_out = open('flight_data.txt','w', encoding='utf-8')
    format_out = '{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16}\n'
    file_out.write(format_out.format('Flight number', 'Status', 'Airplane Model', 'Airplane registration', 'Airplane owner', 'Airline',
                                     'Origin', 'Origin Terminal', 'Origin Gate', 'Destination', 'Destination Terminal', 'Destination Gate',
                                     'Scheduled departure', 'Scheduled arrival', 'Real departure', 'Real arrival', 'Duration'))
    
    # Setting internet parameters
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0"}
    
    current_timestamp = calendar.timegm(time.gmtime())
    
    parameters  = {'code': '', 'plugin[]': '', 'plugin-setting[schedule][mode]': 'arrivals', 
             'plugin-settings[schedule][timestamp]': str(current_timestamp), 'page': '0', 'limit': '100', 
             'token': 'OqXb3FuvDi5WJmBHxxrEo2VTLiHTwQ1p4wL6oifEDwo'}
    
    # Multithreating processig
    thread_count = 64
    jobs_count = len(data_list) - 1
    thread_list = []
    
    for i in range(thread_count):
        start = math.floor(i * jobs_count / thread_count) + 1
        end = math.floor((i + 1) * jobs_count / thread_count) + 1
        thread_list.append(threading.Thread(target=airport_chunk, args=(start, end)))
    
    for thread in thread_list:
        thread.start()
    
    for thread in thread_list:
        thread.join()
    
    
    # Ending
    file.close()
    file_out.close()
    
    elapsed_time = time.time() - start_time
    print('The total elapsed time was ' + str(elapsed_time))