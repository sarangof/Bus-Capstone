# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:18:01 2016

@author: mu529
"""

from scipy import spatial

def nearby_pings(stop_id,trip_id,stop_times,stops,avl_data,radius=0.001):
    # return the AVL pings near a certain stop corresponding to a certain trip
    trip_stop_times = stop_times.loc[trip_id]
    stop_locs = stops[['stop_lat','stop_lon','stop_name']]
    trip_stop_times = trip_stop_times.join(stop_locs)
    lookup = 'MTA NYCT_' + trip_id
    avl_subset = avl_data.query('Trip == @lookup')
    points = zip(avl_subset.Longitude.ravel(), avl_subset.Latitude.ravel())
    tree = spatial.KDTree(points)
    lookup_point = [trip_stop_times.loc[stop_id].stop_lon,trip_stop_times.loc[stop_id].stop_lat]
    nearby_points = tree.query_ball_point(lookup_point, radius)
    return avl_subset.iloc[nearby_points]