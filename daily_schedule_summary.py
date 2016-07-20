# -*- coding: utf-8 -*-
"""
Created on Wed Jul 20 17:45:21 2016

@author: mu529
"""

import os
import pandas as pd
from datetime import datetime

import ttools #homemade module
import gtfs #homemade module
os.chdir('/gpfs2/projects/project-bus_capstone_2016/workspace/share')

schedule_samples = ['2015-01-04','2015-04-05','2015-06-27','2015-07-06','2015-09-05','2015-09-15','2015-10-12']

base = datetime.strptime(schedule_samples[1], '%Y-%m-%d')
numdays = datetime.strptime(schedule_samples[2], '%Y-%m-%d') - base
date_list = [base + ttools.datetime.timedelta(days=(x-1)) for x in range(0, numdays.days)]

ss = schedule_samples[1]
# get all the schedule data. (subset can be created later)
trips = gtfs.load_trips(ss,'gtfs/')
stop_times, tz_sched = gtfs.load_stop_times(ss,'gtfs/')
stop_times['arrival_time'] = pd.to_timedelta(stop_times['arrival_time'])
print 'Finished loading season schedule'

pd.DataFrame(columns=['date','count','mean','std','min','25%','50%','75%','max']).to_csv(ss+'_schedules.csv',index=False)
for dd in date_list:
    ds = datetime.strftime(dd,'%Y-%m-%d')
    try:     
        tcal=gtfs.TransitCalendar(ds)
        day_services = tcal.get_service_ids(ds)
        day_trips = trips.service_id.isin(day_services)
        day_stops = stop_times.reset_index(level=1).loc[day_trips]
        day_stops.set_index('stop_id',append=True,inplace=True)
        trip_durations = day_stops.groupby(level=(0))['arrival_time'].max()- day_stops.groupby(level=(0))['arrival_time'].min()
        stats = pd.DataFrame(trip_durations.describe())
        stats = stats.rename(columns={'arrival_time':ds}).transpose()
        stats.to_csv(ss+'_schedules.csv',mode='a',header=False)
    except:
        print "error on " + ds
    
