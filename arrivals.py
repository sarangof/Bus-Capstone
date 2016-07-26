# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:18:01 2016
This module is meant for methods used for estimating actual vehicle arrival
times, whether it be at a certain stop or intersection etc. There may be
multiple techniques to make a certain estimation.
@author: mu529
"""

import pandas as pd
from scipy import spatial
from scipy import interpolate
from itertools import compress

def longest_inc_range(s,tolerance=0):
    end_idx_local = 0
    start_idx_local = 0
    best_len = 0
    for i in xrange(1, len(s)):
        if s[i] < (s[i-1]-tolerance):
            # first check we achieved a new best
            local_max_len = end_idx_local - start_idx_local
            if local_max_len > best_len:
                best_len = local_max_len # reset the best length
                start_idx_best = start_idx_local
                end_idx_best = i  # and remember the local indexes as the best 
            # in any case, reset the local bookmarks
            start_idx_local = i
            end_idx_local = i
        else:
            # if it continues to increase, do nothing except move the end bookmark
            end_idx_local = i
            local_max_len = end_idx_local - start_idx_local
    if local_max_len > best_len:
        # in case the entire sequence was monotonic increasing
        return (0,end_idx_local+1)
    elif best_len == 0:
        # in case the entire sequence was monotonic decreasing
        return (None,None)
    else:
        return (start_idx_best,end_idx_best+1)

# for iteration over dataframe rows (each row is a collection of lists of necessary data)
def interpolate_all_stops(merged_row,tol=100):
    distance_stops = merged_row.shape_stop_dist
    # if the returned object is not a list, it does not contain any information, so there is nothing to interpolate.
    if type(distance_stops) != list:
        return [[]]
    # if there are fewer than 2 pings, no interpolation is possible
    if len(merged_row.recorded_time)<2:
        return [[]]    
    # assemble the ping data as a pandas Series, for convenient use of dropna() method
    list1, list2 = zip(*sorted(zip(merged_row.recorded_time,merged_row.veh_dist_along_shape)))
    veh_pings = pd.Series(index=list1,data=list2)
    veh_pings = veh_pings.dropna()
    # pings must be cleaned for cases when the vehicle "moves backwards" along the route.
    # this may occur when the vehicle is actually finishing another trip, or returning to the first stop
    # the proposed method is to identify the the largest monotonic increasing subsequence
    first, last = longest_inc_range(veh_pings.values,tolerance=tol)
    if len(veh_pings) == 0:
        return [[]]
    valid_pings = veh_pings.iloc[first:last]
    if len(valid_pings)<2:
        return [[]]
    # finally, perform the interpolation with the cleaned data
    x = valid_pings.values
    y = valid_pings.index.values
    f = interpolate.interp1d(x,y)
    xnew = distance_stops
    masker = (xnew > min(x)) & (xnew < max(x))
    xnew = list(compress(xnew,masker))
    # return the estimated times (as timedelta dtype) and the stop labels
    interp_times = pd.to_timedelta(f(xnew),unit='ns')
    return [list(compress(merged_row.shape_stop_id,masker)),interp_times]

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

"""
Below is a work in progress.

    this is similar to nearby_pings but takes the trip-stop and returns one value
    designed to be used in groupby.apply.
    First need to join with stop lon and lat
    The lowest level in trip_group should be trip_id
    ? stop_id should be first column?
    Need to join trip data with lats and lons first

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
