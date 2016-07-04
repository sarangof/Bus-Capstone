# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:18:01 2016
@author: mu529

The three methods in this library are load_trips(), load_stops(), and load_stop_times()
Each has the mandatory argument of the base directory path of the GTFS files
Each returns a dataframe containing the merged data from the files with the primary
key field(s) as the index
"""

import os
from StringIO import StringIO
from pandas.io.common import ZipFile
import pandas as pd

def load_trips(dpath):
    # dpath argument is the path to the directory containing zips for each 
    # agency or borough
    trips = pd.DataFrame()
    for fname in os.listdir(dpath):    
        try:
            with ZipFile(dpath+fname) as zf:
                raw_text = zf.read('trips.txt')    
            csvdata=StringIO(raw_text)
            trips = trips.append(pd.read_csv(csvdata))
        except:
            print 'Error reading from ' + fname
    trips.set_index('trip_id',drop=True,inplace=True,verify_integrity=True)
    return trips

def load_stops(dpath,clean=True):
    # dpath argument is the path to the directory containing zips for each 
    # agency or borough
    # clean=True eliminates duplicates that occur due to stops being shared by
    # multiple lines across GTFS files.  Method will return an error if this is set
    # to false but duplicates exist
    stops = pd.DataFrame()
    for fname in os.listdir(dpath):    
        try:
            with ZipFile(dpath+fname) as zf:
                raw_text = zf.read('stops.txt')  
            times_df = pd.read_csv(StringIO(raw_text))
            stops = stops.append(times_df)
        except:
            print 'Error reading from ' + fname
    if clean==True:
        stops.drop_duplicates(subset='stop_id',inplace=True)
    else:
        pass
    stops.set_index('stop_id',drop=True,inplace=True,verify_integrity=True)
    return stops

def load_stop_times(dpath):
    # dpath argument is the path to the directory containing zips for each 
    # agency or borough
    # NOTE: returns times as string dtype (HH:MM:SS)
    stop_times = pd.DataFrame()
    agency_df = pd.DataFrame()
    for fname in os.listdir(dpath):    
        try:
            with ZipFile(dpath+fname) as zf:
                raw_text = zf.read('stop_times.txt')   
                agency = zf.read('agency.txt')
            stop_times = stop_times.append(pd.read_csv(StringIO(raw_text)))
            agency_df = agency_df.append(pd.read_csv(StringIO(agency)))
        except:
            print 'Error reading from ' + fname
    stop_times.set_index(['trip_id','stop_id'],drop=True,inplace=True)    
    tz_string = agency_df['agency_timezone'].unique()
    if len(tz_string)==1:
        tz_string = tz_string[0]
    else:
        tz_string = 'error: zero or multiple zones'
    return stop_times, tz_string

def load_trip_shapes(dpath,clean=True):
    # dpath argument is the path to the directory containing zips for each 
    # agency or borough
    # clean=True eliminates duplicates that occur due to shapes being shared by
    # multiple trips across GTFS files.  Method will return an error if this is set
    # to false but duplicates exist
    trip_shapes = pd.DataFrame()
    for fname in os.listdir(dpath):    
        try:
            with ZipFile(dpath+fname) as zf:
                raw_text = zf.read('shapes.txt')    
            csvdata=StringIO(raw_text)
            trip_shapes = trip_shapes.append(pd.read_csv(csvdata))
        except:
            print 'Error reading from ' + fname
    if clean==True:
        trip_shapes.drop_duplicates(subset='shape_id',inplace=True)
    else:
        pass
    trip_shapes.set_index('shape_id',drop=True,inplace=True,verify_integrity=True)
    return trip_shapes
    
class TransitCalendar:
    def __init__(self, dpath):
        service_dates = pd.DataFrame()
        for fname in os.listdir(dpath):    
            try:
                with ZipFile(dpath+fname) as zf:
                    raw_text = zf.read('calendar.txt')    
                csvdata=StringIO(raw_text)
                service_dates = service_dates.append(pd.read_csv(csvdata))
            except:
                print 'Error reading from ' + fname
        service_dates.set_index('service_id',drop=True,inplace=True,verify_integrity=True)
        self.service_dates = service_dates
    def get_service_ids(self,d):
        import calendar
        trip_dow = calendar.weekday(int(d[:4]),int(d[5:7]),int(d[8:10]))
        trip_date = int(d.replace('-',''))    
        bools = (self.service_dates.start_date <= trip_date) & (trip_date <= self.service_dates.end_date)       
        subset = self.service_dates.loc[bools]
        return list(subset[subset.iloc[:,trip_dow]==1].index)
