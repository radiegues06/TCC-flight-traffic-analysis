# -*- coding: utf-8 -*-
"""
Created on Tue Feb 26 14:49:33 2019

@author: Rafael Amancio Diegues

Collect all arrivals and depart data for each airport, writing it all in a file
"""

import requests, json
import threading
import csv
import math
import time, calendar
import os

def flightsInAirportRequest(IATAcode, page):
    resp = APIrequest()
    jsonParse = request2JSON(resp)
    jsonFlightsNode = JSON2FlightNode(jsonParse)
    
    if jsonFlightsNode == '':
        FlightsFile.write('JSON reading error;' + str(resp.status_code) + ';' + resp.url + ';' +  IATAcode + ';' + str(page) + 
                       ';' + str(resp.content) + '\n')
        return
    
    for i in range(len(jsonFlightsNode)):
        flight = jsonFlightsNode[i]['flight']
        flightDataFilter(flight)
        
        if flightDataFilter:
            writeFlight2File(flight, jsonParse['result']['request']['code'])
                

def request2JSON(resp):
    try:
        jsonParse = json.loads(resp.text)
    except:
        jsonParse = ''
    return jsonParse

def JSON2FlightNode(JSON):
    try:
        flightNode = JSON['result']['response']['airport']['pluginData']['schedule'][parametersRequest['plugin-setting[schedule][mode]']]['data']
    except:
        flightNode = ''
    return flightNode

def flightDataFilter(flight):
    # Só quero de um dia, não quero os com status scheduled
    
    '''Filtering, excluing some data'''
    # Only the last day
        
    '''flight_timestamp = flight['time']['scheduled']['departure']
    
    current_time = time.gmtime()
    yesterday_timestamp = calendar.timegm((current_time.tm_year, current_time.tm_mon, current_time.tm_mday - 1, 
                                           0, 0, 0, current_time.tm_wday -1, current_time.tm_yday, current_time.tm_isdst))
    today_timestamp = calendar.timegm((current_time.tm_year, current_time.tm_mon, current_time.tm_mday, 
                                           0, 0, 0, current_time.tm_wday, current_time.tm_yday, current_time.tm_isdst))
    
    #if not( yesterday_timestamp < flight_timestamp and flight_timestamp < today_timestamp): continue'''
        
    return True

def airportsChunkTread(start, end, AirportsList):
    if testMode == 1:
        parametersRequest['page'] = 0
        parametersRequest['plugin-setting[schedule][mode]'] = 'arrivals'
    
        for airportRow in range(start,start+1):
            IATAcode = AirportsList[airportRow][2][0:3].lower()
            parametersRequest['code'] = IATAcode
            
            print('Airport number ' + str(airportRow) )
            
            flightsInAirportRequest(IATAcode, 0)
        
    else:
        for pageNumber in range(1,-10, -1):
            parametersRequest['page'] = pageNumber
            
            for j in ['arrivals']:#, 'departures']:
                parametersRequest['plugin-setting[schedule][mode]'] = j
                
                for airportRow in range(start, end, 1):
                    IATAcode = AirportsList[airportRow][2][0:3].lower()
                    parametersRequest['code'] = IATAcode
                    
                    #print('\033[H\033[J')
                    print('Page number: {0}   Airport number: {1:>4}  Airport code: {2}'.format(
                            str(pageNumber), str(airportRow), IATAcode))
                    
                    flightsInAirportRequest(IATAcode, pageNumber)

def writeFlight2File(flight, IATAcode):
    
    # Mapping through the dict
    flightMappingKeys = [['identification','number','default'],
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
    
    flightDataToPrint = []
    for keys in flightMappingKeys:
        try:
            flightParameter = dictNavigate(flight, keys)
        except:
            
            if 'iata' in keys:
                flightParameter = IATAcode
            else:
                flightParameter = 'Null'

        flightDataToPrint.append(flightParameter)

    # Printing the file
    flightPrintFormat = ''
    for i in range(len(flightMappingKeys)):
        flightPrintFormat = flightPrintFormat + '{d[' + str(i) + ']};'
    
    flightPrintFormat = flightPrintFormat[:-1] + '\r\n'
    
    FlightsFile.write(flightPrintFormat[:-1].format(d = flightDataToPrint))

def dictNavigate(dic, Keys):
    result = dic
    for k in Keys:
        result = result[k]
    return result

def APIrequest():
    resp = requests.get('https://api.flightradar24.com/common/v1/airport.json', 
                        headers = headersRequest, params = parametersRequest)
    
    if resp.status_code != 200:
        resp = requests.get(resp.url, headers = headersRequest)
    
    return resp
    

if __name__ == '__main__':

    startTime = time.time()
    
    testMode = 0
    
    # Importing the csv file to a matrix
    scriptDirectory = os.path.dirname(__file__)
    
    AirportsFile = open(os.path.join(scriptDirectory,'data/Airports_list.csv'), 'r')
    AirportsList = list(csv.reader(AirportsFile))
    
    if testMode == 1:
        AirportsList = [['', '', 'EZE', '', '', ''], ['', '', 'GRU', '', '', '']]
    
    # Creating and setting new file where the dara will be written
    FlightsFile = open(os.path.join(scriptDirectory,'data/flight_data.txt'),'w', encoding='utf-8')
    formatFlightsStr = '{0};{1};{2};{3};{4};{5};{6};{7};{8};{9};{10};{11};{12};{13};{14};{15};{16}\n'
    FlightsFile.write(formatFlightsStr.format('Flight number', 'Status', 'Airplane Model', 'Airplane registration', 'Airplane owner', 'Airline',
                                     'Origin', 'Origin Terminal', 'Origin Gate', 'Destination', 'Destination Terminal', 'Destination Gate',
                                     'Scheduled departure', 'Scheduled arrival', 'Real departure', 'Real arrival', 'Duration'))
    
    # Setting internet parameters
    headersRequest = {'Host': 'api.flightradar24.com', 
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0'}
    
    currentTimestamp = calendar.timegm(time.gmtime())
    
    parametersRequest  = {'code': '', 'plugin[]': '', 'plugin-setting[schedule][mode]': 'arrivals', 
             'plugin-settings[schedule][timestamp]': str(currentTimestamp), 'page': '0', 'limit': '100', 
             'token': 'OqXb3FuvDi5WJmBHxxrEo27LzBKwU1VWO7-ITM9ASec'}
    
    # Multithreading processig
    threadsCount = 64        # Basic formula: 2 * (number of cores)
    jobsCount = len(AirportsList)
    threadsList = []
    
    for i in range(threadsCount):
        start = math.floor(i * jobsCount / threadsCount)
        end = math.floor((i + 1) * jobsCount / threadsCount)
        threadsList.append(threading.Thread(target=airportsChunkTread, args=(start, end, AirportsList)))
    
    # Executing the threads
    for thread in threadsList:
        thread.start()
    
    for thread in threadsList:
        thread.join()
    
    # Ending
    AirportsFile.close()
    FlightsFile.close()
    
    elapsedTime = time.time() - startTime
    print('The total elapsed time was ' + str(elapsedTime))