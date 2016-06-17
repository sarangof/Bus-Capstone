# -*- coding: utf-8 -*-
"""
The script demonstrates a comparison of vehicle times from AVL data to the
scheduled arrival time at one stop for one example trip.
"""

import os
import pandas as pd
import numpy as np
import datetime
# from dateutil.zoneinfo import gettz
import pytz

# these two modules are homemade
import gtfs
import arrivals
os.chdir('/green-projects/project-bus_capstone_2016/workspace/share')

# get all the schedule data. (subset can be created later)
trips = gtfs.load_trips('gtfs/')
stops = gtfs.load_stops('gtfs/')
stop_times, tz_sched = gtfs.load_stop_times('gtfs/')
print 'Finished loading GTFS data.'

# get the sample of parsed AVL data.  Beware, takes a few minutes.
dt_columns = ['RecordedAtTime','ResponseTimeStamp']
bustime = pd.read_csv('newdata_parsed.csv',parse_dates=dt_columns)
# for now, use a truncated data set.  just get data for one line (M5).
qstr = 'Line == "MTA NYCT_M5" & TripDate == "2016-06-13"'
bustime_short = bustime.query(qstr)
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

# defining a convenience function to compare a row of AVL data to some stop
def dist_from_stop(row,stop_id):
    stop_data = stops.loc[stop_id]
    a = [stop_data.stop_lon,stop_data.stop_lat]
    b = [row.Longitude,row.Latitude]
    d = arrivals.spatial.distance.euclidean(a,b)
    return d

# This function returns some elements of interest for each trip and stop
def trip_summary(trip_id,avl_subset):    
    fail =  0
    stop_list = list(stop_times.loc[trip_id].index)
    # collect AVL data around each stop into a dict
    stop_pings = {}
    for stop_id in stop_list:
        try:
            # for now, hardcoding in a slicer to only measure one trip
            stop_pings[stop_id] = arrivals.nearby_pings(stop_id,trip_id,stop_times,
                                                        stops,avl_subset)
        except:
            fail += 1
            # print(fail)
            continue    
    # make a summary table about the AVL data near each stop on a trip
    summary_columns = ['N','arrival_actual_estimated','timespan']#,'mean_dist']
    summary_df = pd.DataFrame(columns=summary_columns,index=stop_pings.keys())
    for k, v in stop_pings.iteritems():
        # we want to know how many AVL pings were returned  nearby   
        newrow = {'N':len(v)}
        # we also want to know the span of time it was nearby
        # for now we can pretend that's dwell time
        min_stamp = v['ResponseTimeStamp'].min()
        max_stamp = v['ResponseTimeStamp'].max()
        newrow['arrival_actual_estimated'] = min_stamp
        newrow['timespan'] = max_stamp - min_stamp
        # interested in the mean distance from stop. excluding for now
        # mean_dist = v.apply(dist_from_stop,axis=1,args=[k]).mean()
        # newrow['mean_dist'] = mean_dist
        summary_df.loc[k] = newrow
    return summary_df

# now collect a dataframe with the summary data about all trips in the subset
day_summary = pd.DataFrame()
for t in bustime_short.Trip.unique():
    try:
        summary_df = trip_summary(t[9:],bustime_short)
        summary_df['trip_id'] = t[9:]
        summary_df.reset_index(inplace=True)
        summary_df.rename(columns={'index':'stop_id'},inplace=True)
        day_summary = day_summary.append(summary_df)
    except:
        print t
day_summary.set_index(['trip_id','stop_id'],inplace=True)
day_summary = day_summary.join(stop_times['arrival_time'])

# this is the one I finally found to do the job
# http://stackoverflow.com/a/4628148
import re
def parseTimeDelta(s):
    """Create timedelta object representing time delta
       expressed in a string
   
    Takes a string in the format produced by calling str() on
    a python timedelta object and returns a timedelta instance
    that would produce that string.
   
    Acceptable formats are: "X days, HH:MM:SS" or "HH:MM:SS".
    """
    if s is None:
        return None
    d = re.match(
            r'((?P<days>\d+) days, )?(?P<hours>\d+):'
            r'(?P<minutes>\d+):(?P<seconds>\d+)',
            str(s)).groupdict(0)
    return datetime.timedelta(**dict(( (key, int(value))
                              for key, value in d.items() )))

tds = pd.to_datetime('2016-06-13') + day_summary['arrival_time'].apply(parseTimeDelta)
utcoffset = pytz.timezone(tz_sched).localize(datetime.datetime(2016,6,13)).strftime('%z')
utcoffset = datetime.timedelta(hours=int(utcoffset[:3]), minutes=int(utcoffset[4:]))
day_summary['arrival_scheduled'] = tds - utcoffset
del day_summary['arrival_time']

day_summary.to_pickle('WIP.pkl')
day_summary.to_csv('day_summary.csv')