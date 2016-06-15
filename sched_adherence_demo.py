# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:18:01 2016

The script demonstrates a comparison of vehicle times from AVL data to the
scheduled arrival time at one stop for one example trip.

@author: mu529
"""

import os
import pandas as pd
import numpy as np

# these two modules are homemade
import gtfs
import arrivals
os.chdir('/green-projects/project-bus_capstone_2016/workspace/share')

# get all the schedule data. (subset can be created later)
trips = gtfs.load_trips('gtfs/')
stops = gtfs.load_stops('gtfs/')
stop_times = gtfs.load_stop_times('gtfs/')
print 'Finished loading GTFS data.'

# get the sample of parsed AVL data
dt_columns = ['RecordedAtTime','MonitoringTimeStamp','ResponseTimeStamp']
bustime = pd.read_csv('bustime_parsed.csv',parse_dates=dt_columns)
# for now, use a truncated data set.  just get data for one line (M5).
bustime_short = bustime.query('Line == "MTA NYCT_M5"')
del bustime # to free up memory
print 'Finished loading BusTime data and and slicing M5 line.'

# check to see how many records in the AVL data show a trip reference that
# matches one in the schedule data
num_recs = {}
for index, row in bustime_short.iterrows():
    lookup = row.Trip.replace('MTA NYCT_','') 
    # schedule data doesn't use the prefix
    try:
        a = trips.loc[lookup]
        del a
        num_recs[index] = 1 # if something is found, give it a 1
    except:
        num_recs[index] = 0 # if the selection doesnt work, give it a 0
p = np.asarray(num_recs.values()).mean()
print 'Percent of AVL records with matched trip id: ' + str(int(100*p)) + '%'

# use this trip_id to demonstrate the procedure
trip_id = 'MV_B6-Weekday-SDon-102900_M5_250'
# report the scheduled time for stop # 400811
print ('On trip_id ' + trip_id + ', scheduled arrival time at stop_id ' + 
    str(400811) + ' is ')
print stop_times.loc[(trip_id,400811)]['arrival_time']
# then use the arrivals module to get AVL data near that stop
print 'Results of query for nearby AVL records...'
print arrivals.nearby_pings(400811,trip_id,stop_times,stops,bustime_short)

fail =  0
stop_list = list(stop_times.loc['MV_B6-Weekday-SDon-102900_M5_250'].index)
# collect AVL data around each stop into a dict
stop_pings = {}
for stop_id in stop_list:
    try:
        stop_pings[stop_id] = arrivals.nearby_pings(stop_id,trip_id,stop_times,
                                                    stops,bustime_short)
    except:
        fail += 1
        print(fail)
        continue

# defining a convenience function to compare a row of AVL data to some stop
def dist_from_stop(row,stop_id):
    stop_data = stops.loc[stop_id]
    a = [stop_data.stop_lon,stop_data.stop_lat]
    b = [row.Longitude,row.Latitude]
    d = arrivals.spatial.distance.euclidean(a,b)
    return d

# make a summary table about the AVL data near each stop on a trip
summary_columns = ['count','min_stamp','max_stamp','timespan','mean_dist']
summary_df = pd.DataFrame(columns=summary_columns,index=stop_pings.keys())
for k, v in stop_pings.iteritems():
    # also want to know how many AVL pings were returned    
    newrow = {'count':len(v)}
    min_stamp = v['ResponseTimeStamp'].min()
    max_stamp = v['ResponseTimeStamp'].max()
    newrow['min_stamp'] = min_stamp
    newrow['max_stamp'] = max_stamp
    newrow['timespan'] = max_stamp - min_stamp
    # interested in the mean distance (measure of dispersion)
    mean_dist = v.apply(dist_from_stop,axis=1,args=[k]).mean()
    newrow['mean_dist'] = mean_dist
    summary_df.loc[k] = newrow
