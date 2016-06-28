# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:18:01 2016
This module is meant for methods used for estimating actual vehicle arrival
times, whether it be at a certain stop or intersection etc. There may be
multiple techniques to make a certain estimation.
@author: mu529
"""

from scipy import spatial
import pandas as pd

def nearby_pings(stop_id,trip_id,stop_times,stops,avl_data,radius=0.001):
	# returns the AVL pings near a certain stop corresponding to a certain trip
    trip_stop_times = stop_times.loc[trip_id] # stop_times only for this trip
    # get only the interesting columns from stop reference data ...
    stop_locs = stops[['stop_lat','stop_lon','stop_name']]
    trip_stop_times = trip_stop_times.join(stop_locs) #... and join to times
    # create array of only the lon-lat values from AVL data
    points = zip(avl_data.Longitude.ravel(), avl_data.Latitude.ravel())
    tree = spatial.KDTree(points) # implement KDTree class
    # get the lon and lat of the user-specified stop_id
    lookup_point = [trip_stop_times.loc[stop_id].stop_lon,trip_stop_times.loc[stop_id].stop_lat]
    # and get index of AVL records that are nearby to the desired point
    nearby_points = tree.query_ball_point(lookup_point, radius)
    #nearest_point = tree.query(lookup_point,1)
    #print nearest_point
    # return the AVL records
    return avl_data.iloc[nearby_points]

def time_at_location(lat,lon,trip_id,avl_data,stop_times,radius=0.001):
	trip_stop_times = stop_times.loc[trip_id] 
	lookup = 'MTA NYCT_' + trip_id # lookup string used to query AVL data
	avl_subset = avl_data.query('Trip == @lookup')
	points = zip(avl_subset.Longitude.ravel(), avl_subset.Latitude.ravel())
	tree = spatial.KDTree(points) # implement KDTree class
	lookup_point = [lon,lat]
	nearby_points = tree.query_ball_point(lookup_point, radius)
	locations = avl_subset.iloc[nearby_points][['Latitude','Longitude']]
	times = avl_subset.iloc[nearby_points].index.get_level_values('RecordedAtTime')
	df = pd.DataFrame(avl_subset.iloc[nearby_points][['Latitude','Longitude']])
	df['RecordedAtTime'] = times
	df = df.set_index(['RecordedAtTime'])
	#resampled = df.resample('S').interpolate()
	return df#resampled

"""
Below is a work in progress.

    this is similar to nearby_pings but takes the trip-stop and returns one value
    designed to be used in groupby.apply.
    First need to join with stop lon and lat
    The lowest level in trip_group should be trip_id
    ? stop_id should be first column?
    Need to join trip data with lats and lons first"""
def pick_earliest(nearby_index,avl_subset):
    return avl_subset.iloc[nearby_index]['ResponseTimeStamp'].min()

def earliest_nearby(trip_group,trip_date,avl_data,radius=0.001):  
    # make a KDTree with all the avl_subset points
    trip_id = 'MTA NYCT_' + trip_group.index.get_level_values(0).unique()
    avl_subset = avl_data.loc[trip_id] # need to slice using trip_date and trip_id
    points = zip(avl_subset.Longitude, avl_subset.Latitude)
    tree = spatial.KDTree(points)
    # make a (N,2) array of lon-lat points for each stop
    stop_locs = pd.Series(zip(trip_group.stop_lon,trip_group.stop_lat),
                          index=trip_group.index)
    # apply tree.query_ball_point to that series and get a series of lists avl_subset indexes
    nearby_points = stop_locs.apply(tree.query_ball_point,r=radius)
    # get avl_subset.iloc[nearby_points] and choose soonest timestamp
    # for example 
    # avl_subset.iloc[nearby_points[87]]['ResponseTimeStamp'].min()
    earliest = nearby_points.apply(pick_earliest,avl_subset=avl_subset)
    # returns
    return earliest
