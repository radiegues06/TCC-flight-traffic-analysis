# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 12:59:24 2019

@author: Júlia
"""

import flightradar24
fr = flightradar24.Api()

print(fr.apiUrl, fr.balanceJsonUrl, fr.balanceUrl, fr.baseUrl, fr.liveDataUrl, end = '\n')

airlines = fr.get_airlines()
i = 0
for airl in airlines['rows']:
    i += 1
print('Number of airports: ' + str(i))

code = 'GRU'
flights = fr.get_flights(code)