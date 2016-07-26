# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 16:39:17 2016

Cleans AVL data by removing any pings that report a "next stop" that is not valid for the trip referenced.
Takes three arguments of path to file for cleaning, date of schedule, and path of gtfs data

@author: mu529
"""

import pandas as pd 
import sys
import gtfs # homemade module

def valid_stop(row):
    try:
        return int(row.STOP_ID) in row.stop_id
    except:
        return False

def filter_invalid_stops(avl_df,stoptime_df):
    st = stoptime_df.reset_index(level=1)
    valid_stops = st.groupby(level=0)['stop_id'].apply(list)
    filtered = avl_df.merge(pd.DataFrame(valid_stops),left_on='TRIP_ID',
                            right_index=True,how='left')
    masker = filtered.apply(valid_stop,axis=1)
    filtered.drop('stop_id',axis=1,inplace=True)
    return filtered[masker]

if __name__=='__main__':
    infile = sys.argv[1]
    sched_date = sys.argv[2]
    gtfspath = sys.argv[3]
    outfile = sys.argv[1][:-4]+'_cleaned.csv'

    # get the sample of parsed AVL data.  Beware, large files take more time.
    bustime = pd.read_csv(infile,header=None)
    bustime.columns = ['ROUTE_ID','latitude','longitude','recorded_time',
                       'vehicle_id','TRIP_ID','trip_date','SHAPE_ID',
                       'STOP_ID','distance_stop','distance_shape','status',
                       'destination']
    
    bustime.drop_duplicates(['vehicle_id','recorded_time'],inplace=True)
    bustime['TRIP_ID'] = bustime['TRIP_ID'].str.replace('MTA NYCT_','')
    bustime['TRIP_ID'] = bustime['TRIP_ID'].str.replace('MTABC_','')
    bustime['STOP_ID'] = bustime['STOP_ID'].str.replace('MTA_','')
    print 'Finished loading Bus Time data.'

    stop_times = gtfs.load_stop_times(sched_date,'gtfs/')[0]
    print 'Finished loading GTFS data.'
    filtered = filter_invalid_stops(bustime,stop_times)
    filtered.to_csv(outfile,index=False)
