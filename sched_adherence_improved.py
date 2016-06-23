# -*- coding: utf-8 -*-
"""
Created on Thu Jun 23 13:35:02 2016
@author: mu529
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

# get the sample of parsed AVL data.  Beware, takes a few minutes.
bustime = pd.read_csv('newdata_parsed.csv')#,parse_dates=dt_columns)
qstr = ('Trip == "MTA NYCT_MV_B6-Weekday-SDon-038500_M5_203" or '
    'Trip == "MTA NYCT_MV_B6-Weekday-SDon-036500_M5_202" or '
    'Trip == "MTA NYCT_MV_B6-Weekday-SDon-040000_M5_204"')
bustime = bustime.query(qstr)
bustime.drop_duplicates(['vehicleID','RecordedAtTime'],inplace=True)
bustime.set_index(['Line','Trip','TripDate','vehicleID'],
                  inplace=True,drop=True,verify_integrity=True)

# for now, use a truncated data set.  just get data for one line (M5).
tripDateLookup = "2016-06-13"
lineLookup = "MTA NYCT_M5"
bustime_short = bustime.xs((lineLookup,tripDateLookup),level=(0,2))
del bustime # to free up memory
print 'Finished loading BusTime data and and slicing three trips of M5 line.'
"""
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
"""
# testing another way to get a trip summary.

trip_group = stop_times.loc['MV_B6-Weekday-SDon-038500_M5_203']
trip_group = trip_group.join(stops[['stop_lon','stop_lat']])
# testing = arrivals.earliest_nearby(trip_group,'2016-06-13',avl_data)

stop_times2 = stop_times
stop_times2.reset_index(inplace=True)
stop_times2 = stop_times2.query('trip_id == "MV_B6-Weekday-SDon-038500_M5_203" or trip_id == "MV_B6-Weekday-SDon-036500_M5_202" or trip_id == "MV_B6-Weekday-SDon-040000_M5_204"')
stop_times2.set_index(['trip_id','stop_id'],drop=True,inplace=True)
stop_times2 = stop_times2.join(stops[['stop_lon','stop_lat']])

from scipy import spatial
def make_tree(df):
    if len(df)==0:
        return None
    points = list(zip(df.Longitude, df.Latitude))
    return spatial.KDTree(points)
trees = bustime_short.groupby(level=[0,1,2]).apply(make_tree)

# stop_times2.groupby(level=0).get_group('MV_B6-Weekday-SDon-038500_M5_203')

def f1(row):
    trip_id = 'MTA NYCT_' + row.name[0]
    tree = trees.xs((trip_id,'2016-06-13'),level=(1,2)).values[0]
    nearby = tree.query_ball_point([row[0][0],row[0][1]],r=0.001)
    if len(nearby)==0:
        return None
    else:
        return min(nearby)

def f2(trip_group):
    stop_locs = pd.Series(zip(trip_group.stop_lon,trip_group.stop_lat),
                          index=trip_group.index)
    return pd.DataFrame(stop_locs)

gb = bustime_short.groupby(level=[0,1,2])

locs = stop_times2.groupby(level=0,group_keys=False).apply(f2)
first_ixs = locs.apply(f1,axis=1)


.get_group(('MTA NYCT_M5','MTA NYCT_MV_B6-Weekday-SDon-040000_M5_204','2016-06-13')).iloc[2]

for index, value in first_ixs.iteritems():
    i_tup = ('MTA NYCT_M5','MTA NYCT_'+index[0],'2016-06-13')
    ts = bustime_groups.get_group(i_tup).iloc[value]['ResponseTimeStamp']
    ValueError: cannot convert float NaN to integer
# results = stop_times2.groupby(level=0).apply(arrivals.earliest_nearby,trip_date='2016-06-13',avl_data=bustime_short)
