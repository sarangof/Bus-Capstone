# import dependencies                                                                                                                      
import pyspark
from pyspark.sql import SQLContext
import sys
import os
from pyspark.sql.types import *
from pyspark.sql.functions import split
                
def get_sec(s):
    l = s.split(':')
    return int(l[0]) * 3600 + int(l[1]) * 60 + int(l[2]) # convert time to seconds 

if __name__=='__main__':
    sc = pyspark.SparkContext()
    sqlContext = SQLContext(sc)
	
	stop_times_schema = StructType([StructField("trip_id", StringType(), True),\
                           StructField("arrival_time", StringType(), True),\
                           StructField("departure_time", StringType(), True),\
                           StructField("stop_id", StringType(), True),\
                           StructField("stop_sequence", IntegerType(), True),\
                           StructField("pickup_type", IntegerType(), True),
                           StructField("drop_off_type", IntegerType(), True)])

	real_stoptimes_schema = StructType([StructField("ROUTE_ID", StringType(), True),\
                           StructField("TRIP_ID", StringType(), True),\
                           StructField("STOP_ID", StringType(), True),\
                           StructField("time",IntegerType(), True)])

	real_stoptimes = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('1203_predict.csv', schema = real_stoptimes_schema)              
	stoptimes = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('stop_times.txt',schema = stop_times_schema)
	
	new_time = real_stoptimes.withColumn('realtime',split(pyspark.sql.functions.from_unixtime(real_stoptimes.time), ' ')[1]) #convert Unix time to time
	new_time.registerTempTable('new_time')
	stoptimes.registerTempTable('stoptimes')

	sqlContext.registerFunction("getsec", lambda x: get_sec(x), IntegerType()) #register python function into sql

	join = sqlContext.sql('SELECT ROUTE_ID,TRIP_ID,STOP_ID,time,realtime,(getsec(realtime)-getsec(arrival_time)) as delay\
                		   FROM new_time\
                 		   INNNER JOIN stoptimes\
                    	   ON (TRIP_ID = trip_id AND STOP_ID = stop_id)') # join with GTFS data
  with open(sys.argv[-2]) as fr: #read sql                                                                                               
    query = fr.read()

	join.registerTempTable('new_join')
	ontime_ratio = sqlContext.sql(query)


	ontime_ratio.write\
	    		.format('com.databricks.spark.csv')\
	    		.save(sys.argv[-1])