# import dependencies                                                                                                                      
import pyspark
from pyspark.sql import HiveContext
import sys
import os
from pyspark.sql.types import *

                

if __name__=='__main__':
    sc = pyspark.SparkContext()
    sqlContext = HiveContext(sc)
    
    customSchema = StructType([StructField("Line", StringType(), True),\
                                                              StructField("Latitude", DoubleType(), True),\
                                                              StructField("Longitude", DoubleType(), True),\
                                                              StructField("RecordedAtTime", StringType(), True),\
                                                              StructField("vehicleID", StringType(), True),\
                                                              StructField("TripRef", StringType(), True),\
                                                              StructField("TripDate", DateType(), True),\
                                                              StructField("TripPattern", StringType(), True),\
                                                              StructField("Stop_ID", StringType(), True),\
                                                              StructField("DistFromCall", DoubleType(), True),\
                                                              StructField("CallDistAlongRoute", DoubleType(), True),\
                                                              StructField("PresentableDistance", StringType(), True)])                    
    record = sqlContext.read\
            .format('com.databricks.spark.csv')\
            .options(header='true').load('final_no_pre.csv', schema = customSchema)

    record.registerTempTable('record')

    with open(sys.argv[-2]) as fr:
        query = fr.read()
        gaps = sqlContext.sql(query)
        gaps.map(lambda x: ",".join(map(str, x))).saveAsTextFile(sys.argv[-1])
