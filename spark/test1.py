
# import dependencies
import pyspark
from pyspark.sql import HiveContext
import sys
import os

# zip rows(lists)
def parse_list(p):
    if p.Line!=None:
        return zip(p.Line,p.Latitude,p.Longitude,p.RecordedAtTime,p.vehicleID,p.Trip,p.TripDate,p.TripPattern,p.MonitoredCallRef,p.DistFromCall,p.CallDistAlongRoute,p.PresentableDistance)
    else:
        return []

if __name__=='__main__':
    sc = pyspark.SparkContext()
    sqlContext = HiveContext(sc)
    bus_file='BusTime/2015_*.jsons' #read multiple_josns
    bus = sqlContext.read.json(bus_file)
    bus.registerTempTable("bus") # register into table in order to use sql
    with open(sys.argv[-2]) as fr: #read sql
        query = fr.read()
    sqlContext.sql(query).flatMap(parse_list).map(lambda x: ",".join(map(str, x))).saveAsTextFile(sys.argv[-1])
