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
    TripDatelist = []
    vdata = a['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']
    l = len(vdata)
    for i in range(0,l):
        vehicleID = vdata[i]['MonitoredVehicleJourney']['VehicleRef']
        Line = vdata[i]['MonitoredVehicleJourney']['LineRef']
        RecordTime = vdata[i]['RecordedAtTime']
        Latitude = vdata[i]['MonitoredVehicleJourney']['VehicleLocation']['Latitude']
        Longitude = vdata[i]['MonitoredVehicleJourney']['VehicleLocation']['Longitude']
        Trip = vdata[i]['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']
        TripDate = vdata[i]['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DataFrameRef']
        vehicleIDlist.append(vehicleID)        
        Linelist.append(Line)
        RecordTimelist.append(RecordTime)
        Latitudelist.append(Latitude)
        Longitudelist.append(Longitude)        
        Triplist.append(Trip)
        TripDatelist.append(TripDate)
    df = pd.DataFrame(data=Linelist,index=vehicleIDlist,columns=['Line'])
    df['RecordedAtTime']=RecordTimelist
    df['Latitude']=Latitudelist
    df['Longitude']=Longitudelist
    df['Trip']=Triplist
    df['TripDate']=TripDatelist
    df['ResponseTimeStamp'] = a['Siri']['ServiceDelivery']['ResponseTimestamp']
    return df

def extract(inpath,outfile):
    # first create empty dataframe
    results = pd.DataFrame(columns=[u'Line', u'RecordedAtTime', u'Latitude', u'Longitude', u'Trip',u'TripDate', u'ResponseTimeStamp'])
    results.index.rename('vehicleID',inplace=True)    
    # write initial output file with headers but no data
    results.to_csv(outfile)
    for f in os.listdir(inpath + '/'):    
        try:
            with open(inpath + '/' + f,'rb') as infile:
                data = json.load(infile)
            results = json_to_df(data)
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