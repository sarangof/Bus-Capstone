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

defaultpath = '/gpfs2/projects/project-bus_capstone_2016/workspace/share/gtfs/'

def build_metadata(dpath=defaultpath):        
    for root, dirs, files in os.walk(dpath):
        mdata = pd.DataFrame(columns=['file','min_eff','max_disc','file_date'])
        for fname in files:    
            try:
                with ZipFile(root + '/' + fname) as zf:
                    raw_text = zf.read('calendar.txt')
                    fdatetime = zf.getinfo('calendar.txt').date_time
                fdate = (str(fdatetime[0]) + '-' + str(fdatetime[1]).zfill(2) +
                            '-' + str(fdatetime[2]).zfill(2))
                csvdata = pd.read_csv(StringIO(raw_text))
                min_e = str(min(csvdata['start_date']))
                min_eff = min_e[:4] + '-' + min_e[4:6] + '-' + min_e[6:8]
                max_d = str(max(csvdata['end_date']))
                max_disc = max_d[:4] + '-' + max_d[4:6] + '-' + max_d[6:8]
                nrow = {'file':fname,'min_eff':min_eff,'max_disc':max_disc,\
                        'file_date':fdate}
                mdata = mdata.append(nrow,ignore_index=True)
            except:
                print 'Error reading from ' + fname
        mdata.to_csv(root+'/metadata.txt',index=False)

def effective_files(trip_date,end_date=None,dpath=defaultpath):
    file_list = []    
    for root, dirs, files in os.walk(dpath):  
        if 'metadata.txt' in files:
            mdata = pd.read_csv(root+'/'+'metadata.txt')
            subset = mdata.query('min_eff <= @trip_date <= max_disc & file_date <= @trip_date')
            subset = subset.sort('file_date',ascending=False)
            try:
                filename = subset.iloc[0]['file']
                file_list.append(root+'/'+filename)
            except:
                pass
    return file_list
    
def load_trips(trip_date,dpath=defaultpath):
    # dpath argument is the path to the directory containing subdirectories
    trips = pd.DataFrame()
    for fi in effective_files(trip_date,dpath=dpath):    
        try:
            with ZipFile(fi) as zf:
                raw_text = zf.read('trips.txt')    
            csvdata=StringIO(raw_text)
            trips = trips.append(pd.read_csv(csvdata))
        except:
            print 'Error reading from ' + fi
    trips.set_index('trip_id',drop=True,inplace=True,verify_integrity=True)
    return trips

def load_stops(trip_date,dpath,clean=True):
    # dpath argument is the path to the directory containing zips for each 
    # agency or borough
    # clean=True eliminates duplicates that occur due to stops being shared by
    # multiple lines across GTFS files.  Method will return an error if this is set
    # to false but duplicates exist
    stops = pd.DataFrame()
    for fi in effective_files(trip_date,dpath=dpath):    
        try:
            with ZipFile(fi) as zf:
                raw_text = zf.read('stops.txt')  
            times_df = pd.read_csv(StringIO(raw_text))
            stops = stops.append(times_df)
        except:
            print 'Error reading from ' + fi
    if clean==True:
        stops.drop_duplicates(subset='stop_id',inplace=True)
    else:
        pass
    stops.set_index('stop_id',drop=True,inplace=True,verify_integrity=True)
    return stops

def load_stop_times(trip_date,dpath):
    # dpath argument is the path to the directory containing zips for each 
    # agency or borough
    # NOTE: returns times as string dtype (HH:MM:SS)
    stop_times = pd.DataFrame()
    agency_df = pd.DataFrame()
    for fi in effective_files(trip_date,dpath=dpath):    
        try:
            with ZipFile(fi) as zf:
                raw_text = zf.read('stop_times.txt')   
                agency = zf.read('agency.txt')
            stop_times = stop_times.append(pd.read_csv(StringIO(raw_text)))
            agency_df = agency_df.append(pd.read_csv(StringIO(agency)))
        except:
            print 'Error reading from ' + fi
    stop_times.set_index(['trip_id','stop_id'],drop=True,inplace=True)    
    tz_string = agency_df['agency_timezone'].unique()
    if len(tz_string)==1:
        tz_string = tz_string[0]
    else:
        tz_string = 'error: zero or multiple zones'
    return stop_times, tz_string

def load_trip_shapes(trip_date,dpath,clean=True):
    # dpath argument is the path to the directory containing zips for each 
    # agency or borough
    # clean=True eliminates duplicates that occur due to shapes being shared by
    # multiple trips across GTFS files.  Method will return an error if this is set
    # to false but duplicates exist
    trip_shapes = pd.DataFrame()
    for fi in effective_files(trip_date,dpath=dpath):    
        try:
            with ZipFile(fi) as zf:
                raw_text = zf.read('shapes.txt')    
            csvdata=StringIO(raw_text)
            trip_shapes = trip_shapes.append(pd.read_csv(csvdata))
        except:
            print 'Error reading from ' + fi
    if clean==True:
        trip_shapes.drop_duplicates(subset='shape_id',inplace=True)
    else:
        pass
    trip_shapes.set_index('shape_id',drop=True,inplace=True,verify_integrity=True)
    return trip_shapes



class TransitCalendar:
    def __init__(self, trip_date, dpath):
        service_dates = pd.DataFrame()
        for fi in effective_files(trip_date,dpath=dpath):    
            try:
                with ZipFile(fi) as zf:
                    raw_text = zf.read('calendar.txt')    
                csvdata=StringIO(raw_text)
                service_dates = service_dates.append(pd.read_csv(csvdata))
            except:
                print 'Error reading from ' + fi
        service_dates.set_index('service_id',drop=True,inplace=True,verify_integrity=True)
        self.service_dates = service_dates
        service_exc = pd.DataFrame(columns=['service_id','date','exception_type'])
        for fname in os.listdir(dpath):    
            try:
                with ZipFile(dpath+fname) as zf:
                    raw_text = zf.read('calendar_dates.txt')    
                csvdata=StringIO(raw_text)
                service_exc = service_exc.append(pd.read_csv(csvdata,dtype={0:str,1:str,2:int}))
            except:
                print 'Error reading from ' + fname
        # service_exc.set_index('date',drop=True,inplace=True)
        self.service_exc = service_exc
    def get_service_ids(self,d):
        import calendar
        trip_dow = calendar.weekday(int(d[:4]),int(d[5:7]),int(d[8:10]))
        trip_date = int(d.replace('-',''))    
        bools = (self.service_dates.start_date <= trip_date) & (trip_date <= self.service_dates.end_date)       
        subset = self.service_dates.loc[bools]
        service_list = list(subset[subset.iloc[:,trip_dow]==1].index)
        qstr = 'date == "' + d.replace('-','') + '"'     
        exceptions = self.service_exc.query(qstr)
        service_list += list(exceptions.query('exception_type==1')['service_id'])
        for exc in list(exceptions.query('exception_type==2')['service_id']):
            if exc in service_list:
                service_list.remove(exc)
        return service_list
