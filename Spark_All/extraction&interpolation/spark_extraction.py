# import dependencies                                                                                                                      
import pyspark
from pyspark.sql import HiveContext
import sys
import os

# zip rows(lists)                                                                                                                          
def parse_list(p):
    if p.Line!=None:
        return zip(p.ROUTE_ID,p.latitude,p.longitude,p.recorded_time\
                   ,p.vehicle_id,p.TRIP_ID,p.tripdate,p.SHAPE_ID\
                   ,p.STOP_ID,p.distance_stop,p.distance_shape,p.status,p.destination)
    else:
        return []

if __name__=='__main__':
    sc = pyspark.SparkContext()
    sqlContext = HiveContext(sc)
    bus_file='BusTime/2015_12_03*.jsons' #read multiple_josns                                                                              
    bus = sqlContext.read.json(bus_file)
    bus.registerTempTable("bus") # register into table in order to use sql                                                                 
    with open(sys.argv[-2]) as fr: #read sql                                                                                               
        query = fr.read()
    output = sqlContext.sql(query)     
    output.flatMap(parse_list)\
          .map(lambda x: ",".join(map(str, x)))\
          .map(lambda x: x.replace('MTA NYCT_', '').replace('MTABC_','').replace('MTA_','').replace('-05:00',''))\
          .saveAsTextFile(sys.argv[-1])

