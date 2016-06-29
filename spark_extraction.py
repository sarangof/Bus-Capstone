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

if __name__=='__main__':
    sc = pyspark.SparkContext()
    sqlContext = HiveContext(sc)
    bus_file='BusTime/2015_01_0*.jsons' #read multiple_josns
    bus = sqlContext.read.json(bus_file)
    bus.registerTempTable("bus") # register into table in order to use sql
    with open(sys.argv[-2]) as fr: #read sql
        query = fr.read()
    output = sqlContext.sql(query) # apply sql
    output.flatMap(parse_list).map(lambda x: ",".join(map(str, x))).saveAsTextFile(sys.argv[-1]) #flatmap&save to csv

