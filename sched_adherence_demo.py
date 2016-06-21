# -*- coding: utf-8 -*-
"""
The script demonstrates a comparison of vehicle times from AVL data to the
scheduled arrival time at one stop for one example trip.
"""

import os
import pandas as pd

# these two modules are homemade
import gtfs
import arrivals
os.chdir('/gpfs2/projects/project-bus_capstone_2016/workspace/share')

# get all the schedule data. (subset can be created later)
trips = gtfs.load_trips('gtfs/')
stops = gtfs.load_stops('gtfs/')
stop_times, tz_sched = gtfs.load_stop_times('gtfs/')
print 'Finished loading GTFS data.'

# get the sample of parsed AVL data.  Beware, takes a few minutes.
# dt_columns = ['RecordedAtTime','ResponseTimeStamp']
bustime = pd.read_csv('newdata_parsed.csv')#,parse_dates=dt_columns)
# for now, use a truncated data set.  just get data for one line (M5).
tripDateLookup = "2016-06-13"
qstr = 'Line == "MTA NYCT_M5" & TripDate == @tripDateLookup'
bustime_short = bustime.query(qstr)
del bustime # to free up memory
print 'Finished loading BusTime data and and slicing M5 line.'

# This function returns some elements of interest for each trip and stop
def trip_summary(avl_subset):    
    fail =  0
    trips = avl_subset.Trip.unique()
    if len(trips) != 1:
        raise ValueError('Need exactly one unique trip_id in input data.')
    trip_id = trips[0][9:]
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
            continue    
    # make a summary table about the AVL data near each stop on a trip
    summary_columns = ['N','arrival_actual_estimated']#,'timespan']#,'mean_dist']
    summary_df = pd.DataFrame(columns=summary_columns,index=stop_pings.keys())
    for k, v in stop_pings.iteritems():
        # we want to know how many AVL pings were returned  nearby   
        newrow = {'N':len(v)}
        # we also want to know the span of time it was nearby
        # for now we can pretend that's dwell time
        min_stamp = v['ResponseTimeStamp'].min()
        # max_stamp = v['ResponseTimeStamp'].max()
        newrow['arrival_actual_estimated'] = min_stamp
        # newrow['timespan'] = max_stamp - min_stamp
        summary_df.loc[k] = newrow
    return summary_df

# now collect a dataframe with the summary data about all trips in the subset
day_summary = pd.DataFrame()
for t in bustime_short.Trip.unique():
    try:
        avl_subset = bustime_short.query('Trip == @t')
        summary_df = trip_summary(avl_subset)
        summary_df['trip_id'] = t[9:]
        summary_df['TripDate'] = tripDateLookup
        summary_df.reset_index(inplace=True)
        summary_df.rename(columns={'index':'stop_id'},inplace=True)
        day_summary = day_summary.append(summary_df)
    except:
        print t
day_summary.set_index(['trip_id','stop_id'],inplace=True)
day_summary = day_summary.join(stop_times['arrival_time'])

print day_summary.shape