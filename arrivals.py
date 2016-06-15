# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:18:01 2016

This module is meant for methods used for estimating actual vehicle arrival
times, whether it be at a certain stop or intersection etc. There may be
multiple techniques to make a certain estimation.

@author: mu529
"""

from scipy import spatial
import numpy as np

def nearby_pings(stop_id,trip_id,stop_times,stops,avl_data,radius=0.001):
	# returns the AVL pings near a certain stop corresponding to a certain trip
	trip_stop_times = stop_times.loc[trip_id] # stop_times only for this trip
	# get only the interesting columns from stop reference data ...
	stop_locs = stops[['stop_lat','stop_lon','stop_name']]
	trip_stop_times = trip_stop_times.join(stop_locs) #... and join to times
	lookup = 'MTA NYCT_' + trip_id # lookup string used to query AVL data
	avl_subset = avl_data.query('Trip == @lookup')
	# create array of only the lon-lat values from AVL data
	points = zip(avl_subset.Longitude.ravel(), avl_subset.Latitude.ravel())
	tree = spatial.KDTree(points) # implement KDTree class
	# get the lon and lat of the user-specified stop_id
	lookup_point = [trip_stop_times.loc[stop_id].stop_lon,trip_stop_times.loc[stop_id].stop_lat]
	# and get index of AVL records that are nearby to the desired point
	nearby_points = tree.query_ball_point(lookup_point, radius)
	#nearest_point = tree.query(lookup_point,1)
	#print nearest_point
	# return the AVL records
	return avl_subset.iloc[nearby_points]

def time_at_location(lat,lon,trip_id,avl_data,stop_times,radius=0.001):
	trip_stop_times = stop_times.loc[trip_id] 
	lookup = 'MTA NYCT_' + trip_id # lookup string used to query AVL data
	avl_subset = avl_data.query('Trip == @lookup')
	points = zip(avl_subset.Longitude.ravel(), avl_subset.Latitude.ravel())
	tree = spatial.KDTree(points) # implement KDTree class
	lookup_point = [lon,lat]
	nearby_points = tree.query_ball_point(lookup_point, radius)
	locations = avl_subset.iloc[nearby_points][['Latitude','Longitude']]
	times = avl_subset.iloc[nearby_points]['RecordedAtTime']
	df = pd.dataFrame(avl_subset.iloc[nearby_points][['RecordedAtTime','Latitude','Longitude']])
	df = df.set_index(['RecordedAtTime'])
	resampled = df.resample('S').interpolate()
	return resampled

