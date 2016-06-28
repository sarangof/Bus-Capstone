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
import time
os.chdir('/gpfs2/projects/project-bus_capstone_2016/workspace/share')

# get all the schedule data. (subset can be created later)
trips = gtfs.load_trips('gtfs/')
stops = gtfs.load_stops('gtfs/')
stop_times, tz_sched = gtfs.load_stop_times('gtfs/')
print 'Finished loading GTFS data.'

# get the sample of parsed A	VL data.  Beware, takes a few minutes.
bustime = pd.read_csv('newdata_parsed.csv')#,parse_dates=dt_columns)
qstr = ('Trip == "MTA NYCT_MV_B6-Weekday-SDon-038500_M5_203" or '
    'Trip == "MTA NYCT_MV_B6-Weekday-SDon-036500_M5_202" or '
    'Trip == "MTA NYCT_MV_B6-Weekday-SDon-040000_M5_204"')
bustime = bustime.query(qstr)
bustime.drop_duplicates(['vehicleID','RecordedAtTime'],inplace=True)
bustime.set_index(['Line','Trip','TripDate','vehicleID','RecordedAtTime'],
                  inplace=True,drop=True,verify_integrity=True)

# for now, use a truncated data set.  just get data for one line (M5).
tripDateLookup = "2016-06-13"
lineLookup = "MTA NYCT_M5"
bustime_short = bustime.xs((lineLookup,tripDateLookup),level=(0,2))
del bustime # to free up memory
print 'Finished loading BusTime data and and slicing three trips of M5 line.'

# This function returns some elements of interest for each trip and stop
def trip_summary(avl_subset,t):    
    fail =  0
    trip_id = t[9:]
    stop_list = list(stop_times.loc[trip_id].index)
    # collect AVL data around each stop into a dict
    stop_pings = {}
    for stop_id in stop_list:
        try:
            stop_pings[stop_id] = arrivals.nearby_pings(stop_id,trip_id,stop_times,
                                                        stops,avl_subset)
        except:
            fail += 1
            continue    
    print str(time.time()) + ' - finished stop pings dict.'
    # make a summary table about the AVL data near each stop on a trip
    summary_columns = ['N','arrival_actual_estimated']
    summary_df = pd.DataFrame(columns=summary_columns,index=stop_pings.keys())
    for k, v in stop_pings.iteritems():
        # we want to know how many AVL pings were returned  nearby   
        newrow = {'N':len(v)}
        min_stamp = v['ResponseTimeStamp'].min()
        newrow['arrival_actual_estimated'] = min_stamp
        summary_df.loc[k] = newrow
    print str(time.time()) + ' - finished ' + t[9:]
    return summary_df

# now collect a dataframe with the summary data about all trips in the subset
day_summary = pd.DataFrame()
for t in bustime_short.index.get_level_values(0).unique():
    try:
        avl_subset = bustime_short.loc[t]
        summary_df = trip_summary(avl_subset,t)
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

# day_summary.to_csv('day_summary.csv')

data = pd.read_csv('longjsons_parsed.csv')

qstr = ('Trip == "MTA NYCT_FB_B6-Weekday-SDon-043400_B41_16"')
bustime = data.query(qstr)
bustime.drop_duplicates(['vehicleID','RecordedAtTime'],inplace=True)
bustime.set_index(['Line','Trip','TripDate','vehicleID','RecordedAtTime'],
                  inplace=True,drop=True,verify_integrity=True)

# for now, use a truncated data set.  just get data for one line (M5).
tripDateLookup = "2016-05-25"
lineLookup = "MTA NYCT_B41"
avl_long = bustime.xs((lineLookup,tripDateLookup),level=(0,2))
del bustime,data # to free up memory



trip_id = "MV_B6-Weekday-SDon-040000_M5_204"
lat, lon = 40.850882, -73.936025
