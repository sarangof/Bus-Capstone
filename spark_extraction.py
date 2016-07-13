# import dependencies
import pyspark
from pyspark.sql import HiveContext
import sys
import os
import json

# zip rows(lists)
def parse_list(p):
    if p.Line!=None:
        return zip(p.Line,p.Latitude,p.Longitude,p.RecordedAtTime,p.vehicleID,p.Trip,p.TripDate,p.TripPattern,p.MonitoredCallRef,p.DistF\
romCall,p.CallDistAlongRoute,p.PresentableDistance)
    else:
        return []

def filterrow(x):
    if x[0] == line:
        return x

if __name__=='__main__':
    sc = pyspark.SparkContext()
    sqlContext = HiveContext(sc)
    bus_file='BusTime/2015_01_0*.jsons' #read multiple_josns
    bus = sqlContext.read.json(bus_file)
    bus.registerTempTable("bus") # register into table in order to use sql
    with open(sys.argv[-2]) as fr: #read sql
        query = fr.read()
    output = sqlContext.sql(query).flatMap(parse_list)
    for line in lines:
        output.filter(filterrow).map(lambda x: ",".join(map(str, x))).saveAsTextFile('%s/%s.csv' %(sys.argv[-1],line) # apply sql

