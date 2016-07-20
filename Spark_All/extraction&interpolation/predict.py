# import dependencies                                                                                                                      
import pyspark
from pyspark.sql import HiveContext
import sys
import os
import time
import dateutil.parser
from scipy.interpolate import interp1d


# zip rows(lists)                                                                                                                          
def parse_list(p):
    if p.ROUTE_ID!=None:
        return zip(p.ROUTE_ID,p.latitude,p.longitude,p.recorded_time\
                   ,p.vehicle_id,p.TRIP_ID,p.tripdate,p.SHAPE_ID\
                   ,p.STOP_ID,p.distance_stop,p.distance_shape,p.status,p.destination)
    else:
        return []

def unix_time(x):
    dt = dateutil.parser.parse(x)
    return time.mktime(dt.timetuple())

def findIncreasingList(parts):
    prev = 0
    for record in parts:
        if record[-1]<prev:
            return
        prev = record[-1]
        yield record

def predict(x):
    pre_x = [p[-3] for p in x if p[-3]!=None]
    if len(pre_x) >= 2:
        pre_y = [unix_time(p[3]) for p in x if p[-3]!=None]
        f = interp1d(pre_x, pre_y)
    else:
        return []
    return findIncreasingList([(p[0],p[5],p[-5],f(p[-3]+p[-4]))\
                               for p in x if p[-3]!=None and (p[-3]+p[-4]) <= pre_x[-1]])

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
          .map(lambda x:((x[5],x[6]),x)).groupByKey() \
          .flatMap(lambda x: predict(x[1]))\
          .map(lambda x: ",".join(map(str, x)))\
          .map(lambda x: x.replace('MTA NYCT_', '').replace('MTABC_','').replace('MTA_',''))\
          .saveAsTextFile(sys.argv[-1])
