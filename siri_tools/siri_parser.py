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
    Patternlist = []
    CallReflist = []
    EstArrivallist = []
    DistFromCalllist = []
    CallDistRoutelist = []
    PresentableDistlist = []
    DestinationReflist = []
    DestinationNamelist = []
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
        DestinationReflist.append(vdata[i]['MonitoredVehicleJourney']['DestinationRef'])
        DestinationNamelist.append(vdata[i]['MonitoredVehicleJourney']['DestinationName'])
        # these five fields are only present in the "long" json response format        
        try:        
            TripPattern = vdata[i]['MonitoredVehicleJourney']['JourneyPatternRef']        
            MonitoredCallRef = vdata[i]['MonitoredVehicleJourney']['MonitoredCall']['StopPointRef']
            EstCallArrival = vdata[i]['MonitoredVehicleJourney']['MonitoredCall']['ExpectedArrivalTime']
            DistFromCall = vdata[i]['MonitoredVehicleJourney']['MonitoredCall']['Extensions']['Distances']['DistanceFromCall']
            CallDistAlongRoute = vdata[i]['MonitoredVehicleJourney']['MonitoredCall']['Extensions']['Distances']['CallDistanceAlongRoute']
            PresentableDistance = vdata[i]['MonitoredVehicleJourney']['MonitoredCall']['Extensions']['Distances']['PresentableDistance']
        except:
            MonitoredCallRef = ''
            EstCallArrival = ''
            DistFromCall = ''
            CallDistAlongRoute = ''
            PresentableDistance = ''
        Patternlist.append(TripPattern)
        CallReflist.append(MonitoredCallRef)
        EstArrivallist.append(EstCallArrival)
        DistFromCalllist.append(DistFromCall)
        CallDistRoutelist.append(CallDistAlongRoute)
        PresentableDistlist.append(PresentableDistance)    
    df = pd.DataFrame(data=Linelist,index=vehicleIDlist,columns=['ROUTE_ID'])
    df['recorded_time']=RecordTimelist
    df['latitude']=Latitudelist
    df['longitude']=Longitudelist
    df['TRIP_ID']=Triplist
    df['trip_date']=TripDatelist
    df['destination']=DestinationReflist
    df['destination_name']=DestinationNamelist
    df['SHAPE_ID']=Patternlist
    df['STOP_ID']=CallReflist
    df['EstCallArrival']=EstArrivallist
    df['distance_stop']=DistFromCalllist
    df['distance_shape']=CallDistRoutelist
    df['status']=PresentableDistlist
    df['ResponseTimeStamp'] = a['Siri']['ServiceDelivery']['ResponseTimestamp']
    return df

def extract(inpath,outfile):
    # first create empty dataframe
    results = pd.DataFrame(columns=[u'ROUTE_ID', u'recorded_time', u'latitude', u'longitude', u'TRIP_ID',u'trip_date',u'SHAPE_ID',u'STOP_ID',u'EstCallArrival',u'distance_stop',u'distance_shape',u'status',u'ResponseTimeStamp'])
    results.index.rename('vehicle_id',inplace=True)    
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