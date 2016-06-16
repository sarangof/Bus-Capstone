# -*- coding: utf-8 -*-
"""
Created on Tue May 24 23:46:23 2016

This module is for parsing and extracting from raw json data files.
If called directly, the first argument required is the name of the directory where
the json files are located (all contents of the directory will be processed).
The second argument is the name of the output CSV file

@author: mu529
"""

import os
import sys
import json
import time
import pandas as pd

def json_to_df(a):
    # method to parse a single json and return a dataframe with AVL data
    Linelist = []
    RecordTimelist = []
    Latitudelist = []
    Longitudelist = []
    vehicleIDlist = []
    Triplist = []
    vdata = a['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']
    l = len(vdata)
    for i in range(0,l):
        Line = vdata[i]['MonitoredVehicleJourney']['LineRef']
        vehicleID = vdata[i]['MonitoredVehicleJourney']['VehicleRef']
        RecordTime = vdata[i]['RecordedAtTime']
        Latitude = vdata[i]['MonitoredVehicleJourney']['VehicleLocation']['Latitude']
        Longitude = vdata[i]['MonitoredVehicleJourney']['VehicleLocation']['Longitude']
        Trip = vdata[i]['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']
        Linelist.append(Line)
        RecordTimelist.append(RecordTime)
        Latitudelist.append(Latitude)
        Longitudelist.append(Longitude)
        vehicleIDlist.append(vehicleID)
        Triplist.append(Trip)
    df = pd.DataFrame(Linelist)
    df['vehicleID']=vehicleIDlist
    df['Latitude']=Latitudelist
    df['Longitude']=Longitudelist
    df['Trip']=Triplist
    df['RecordedAtTime']=RecordTimelist
    df['MonitoringTimeStamp'] = a['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['ResponseTimestamp']
    df['ResponseTimeStamp'] = a['Siri']['ServiceDelivery']['ResponseTimestamp']
    df = df.rename(columns = {0:'Line'})
    return df

def extract(inpath,outfile):
    # first create empty dataframe
    results = pd.DataFrame(columns=[u'Line', u'vehicleID', u'Latitude',u'Longitude',
                                    u'Trip',u'RecordedAtTime',u'MonitoringTimeStamp', 
                                    u'ResponseTimeStamp'])
    # write initial output file with headers but no data
    results.to_csv(outfile)
    for f in os.listdir(inpath + '/'):    
        try:
            with open(inpath + '/' + f,'rb') as infile:
                data = json.load(infile)
            results = json_to_df(data)
#            results['RecordedAtTime'] = pd.to_datetime(results['RecordedAtTime'])
#            results['MonitoringTimeStamp'] = pd.to_datetime(results['MonitoringTimeStamp'])
#            results['ResponseTimeStamp'] = pd.to_datetime(results['ResponseTimeStamp'])
            results.to_csv(outfile,mode='a',header=False)
        except:
            print 'Error processing file ' + f    
    
if __name__=='__main__':
    json_dir = sys.argv[1]
    outfile = sys.argv[2]
    print 'Starting time:'
    print time.time()
    extract(json_dir,outfile)
    print 'Finished at:'
    print time.time()