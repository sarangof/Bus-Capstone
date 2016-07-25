# -*- coding: utf-8 -*-
"""
Demonstration script using M5 line on 2016-06-13.  Pass no arguments.

This reads AVL records and scheduled data.  Creates a dataframe showing the
earliest "nearby ping" for each stop on each trip.

@author: mu529
"""

import os
import pandas as pd
import gtfs #homemade module
from scipy import spatial
os.chdir('/gpfs2/projects/project-bus_capstone_2016/workspace/share')

def lonlat_to_tuple(trip_df):
    # creates location tuples from a df containing Longitude and Latitude cols
    stop_locs = pd.Series(zip(trip_df.stop_lon,trip_df.stop_lat),
                          index=trip_df.index)
    return pd.DataFrame(stop_locs,columns=['loc_tuple'])

def make_tree(df):
    # creates a KDTree object from a df containing Longitude and Latitude cols
    if len(df)==0:
        return None
    points = list(zip(df.Longitude, df.Latitude))
    return spatial.KDTree(points)
    
def first_ping_index(row):
    # for a row from stop_times, return indexes of nearby points from KDTree
    trip_id = row.name[0] #trip_id is contained in the row index
    tree = trees.xs((trip_id,'2016-06-13'),level=(1,2)).values[0]
    nearby = tree.query_ball_point([row[0][0],row[0][1]],r=0.001)
    if len(nearby)==0: #sometimes there are no nearby points
        return None
    else:
        return min(nearby)

# get all the schedule data. (subset can be created later)
trips = gtfs.load_trips('gtfs/')
stops = gtfs.load_stops('gtfs/')
stop_times, tz_sched = gtfs.load_stop_times('gtfs/')
print 'Finished loading GTFS data.'

# get the sample of parsed AVL data.  Beware, large files take more time.
bustime = pd.read_csv('newdata_parsed.csv')#,parse_dates=dt_columns)
bustime.drop_duplicates(['vehicleID','RecordedAtTime'],inplace=True)
bustime['Trip'] = bustime['Trip'].str.replace('MTA NYCT_','')
bustime.set_index(['Line','Trip','TripDate','vehicleID','RecordedAtTime'],
                  inplace=True,drop=True,verify_integrity=True)

# for demonstration, use a subset. Just get data for one line (M5) on one day.
tripDateLookup = "2016-06-13"
lineLookup = "MTA NYCT_M5"
bustime = bustime.xs((lineLookup,tripDateLookup),level=(0,2),
                           drop_level=False)
# note that the AVL dataframe must be sorted by timestammp, since iloc[]
# selection is used later in this script to find the earliest time
bustime.sort_index(inplace=True)
print 'Finished loading BusTime data and and slicing three trips of M5 line.'

# make a subset of the stop times data, only related to the reported trips
stop_times = stop_times.ix[bustime.index.get_level_values(1).unique()]
stop_times = stop_times.join(stops[['stop_lon','stop_lat']])

# make a GroupBy object for the trips we want to iterate through
gb = bustime.groupby(level=[0,1,2])
# create one KDTree for each group
trees = gb.apply(make_tree)
print 'Finished creating KDTree for each trip group'
# create location tuples for each trip-stop...
locs = stop_times.groupby(level=0,group_keys=False).apply(lonlat_to_tuple)
# ... and then for each trip-stop, return the local index of first nearby AVL 
# datapoint
first_ixs = locs.apply(first_ping_index,axis=1)
print 'Finished finding index of earliest ping for each trip-stop'
# finally, join the schedule data with the AVL timestamps returned
# by the first_ping_index() method.  Using a for loop in order to avoid
# resetting indexes.
for index, value in first_ixs.iteritems():
    i_tup = ('MTA NYCT_M5',index[0],'2016-06-13')
    if pd.isnull(value):
        stop_times.loc[index,'actual_arrival'] = None
    else:
        ts = gb.get_group(i_tup).iloc[value]['ResponseTimeStamp']
        stop_times.loc[index,'actual_arrival'] = ts
        
print stop_times['actual_arrival'].head(10)
print stop_times.shape