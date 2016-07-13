# Spark_project

## This project reads 3 TB of nested JSON data and apply bunch of spark techniques to analysis the bus delays and headways

## Techniques Included


Read JSON & Read home SQL query to select the right elements


Flatten Arrays using flatMap
```
(1,2,3) (a,b,c) ($,#,&) => (1,a,$),(2,b,#),(3,c,&)
```
groupByKey & Interpolate
```
use groupBykey to cast interpolation of time&distance to all trips
```
Read csv using Spark_CSV tool
```
https://github.com/databricks/spark-csv
```
SparkSQL manipulation