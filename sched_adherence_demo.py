# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:18:01 2016

@author: mu529
"""

import os
import pandas as pd
import numpy as np

import gtfs
import arrivals
os.chdir('/green-projects/project-bus_capstone_2016/workspace/share')

trips = gtfs.load_trips('gtfs/')
stops = gtfs.load_stops('gtfs/')
stop_times = gtfs.load_stop_times('gtfs/')
print 'Finished loading GTFS data.'

bustime = pd.read_csv('bustime_parsed.csv')
# just for now, use a truncated list
bustime_short = bustime.query('Line == "MTA NYCT_M5"')
del bustime
print 'Finished loading BusTime data and and slicing M5 line.'

num_recs = {}
for index, row in bustime_short.iterrows():
    lookup = row.Trip.replace('MTA NYCT_','')
    try:
        a = trips.loc[lookup]
        del a
        num_recs[index] = 1
    except:
        num_recs[index] = 0
p = np.asarray(num_recs.values()).mean()
print 'Percent of AVL records with matched trip id: ' + str(int(100*p)) + '%'

trip_id = 'MV_B6-Weekday-SDon-102900_M5_250'
print 'On trip_id ' + trip_id + ', scheduled arrival time at stop_id ' + str(400811) + ' is '
print stop_times.loc[(trip_id,400811)]['arrival_time']
print 'Results of query for nearby AVL records...'
print arrivals.nearby_pings(400811,trip_id,stop_times,stops,bustime_short)

