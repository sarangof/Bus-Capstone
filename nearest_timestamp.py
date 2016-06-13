# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 15:39:49 2016

@author: saf537

from the gtfs and bustime_parsed.csv data that shows, for each date and trip and stop, 


what is timestamp of the nearest AVL observation ("RecordedAtTime").  

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

