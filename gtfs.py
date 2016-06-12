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
            csvdata=StringIO(raw_text)
            stops = stops.append(pd.read_csv(csvdata))
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
    stop_times = pd.DataFrame()
    for fname in os.listdir(dpath):    
        try:
            with ZipFile(dpath+fname) as zf:
                raw_text = zf.read('stop_times.txt')    
            csvdata=StringIO(raw_text)
            stop_times = stop_times.append(pd.read_csv(csvdata))
        except:
            print 'Error reading from ' + fname
    stop_times.set_index(['trip_id','stop_id'],drop=True,inplace=True,
                          verify_integrity=True)    
    return stop_times