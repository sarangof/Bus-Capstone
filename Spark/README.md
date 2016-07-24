# Spark_project

## This project reads 3 TB of nested JSON data and apply bunch of spark techniques to analysis the bus delays and headways

## Techniques Included

- _Read JSON_
```
sqlContext.read.json()
```
- _Extract elements from JSON using Spark Query_ [spark_extract.sql](https://github.com/sarangof/Bus-Capstone/blob/master/Spark/spark_extract.sql)

- _Flatten Arrays_ using `flatMap(zip([columns]))`
```
(1,2,3) (a,b,c) ($,#,&) => (1,a,$),(2,b,#),(3,c,&)
```
- _groupByKey & Interpolate_
use `groupBykey` to cast interpolation of time&distance to all trips

using[Scipy Interpolate1D](http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html#scipy.interpolate.interp1d)

- _Read csv using Spark_CSV tool_
```
sqlContext.read.format('com.databricks.spark.csv').options(header='true').load()
```
[Spark_CSV_package](https://github.com/databricks/spark-csv)

- _SparkSQL manipulation_

`Inner join` the interpolated data with GTFS schedule
`IF` `COUNT` to calculate the multiple on time perfornace.

_if_ clause in spark is same as case in regular SQL
For more info. [ontime_ratio.sql]('https://github.com/sarangof/Bus-Capstone/blob/master/Spark/ontime_ratio/ontime_ratio.sql')

## Data Schema
| JSON ELEMENT(schema)                           | Column NAME    | explanation                                   |
|------------------------------------------------|----------------|-----------------------------------------------|
| LineRef                                        | ROUTE_ID       | Name of bus line(B42)                         |
| VehicleLocation.Latitude                       | latitude       | latitude of record                            |
| VehicleLocation.Longitude                      | longitude      | longitude of record                           |
| RecordedAtTime                                 | recorded_time  | What time it get recorded                     |
| VehicleRef                                     | vehicle_id     | ID of vehicle                                 |
| FramedVehicleJourneyRef.DatedVehicleJourneyRef | TRIP_ID        | Same as trip_id in GTFS*                      |
| FramedVehicleJourneyRef.DataFrameRef           | trip_date      | Date of the trip                              |
| JourneyPatternRef                              | SHAPE_ID       | Same as shape_id in GTFS*                     |
| StopPointRef                                   | STOP_ID        | Id of next stop(Same as stop_id in GTFS)      |
| Extensions.Distances.DistanceFromCall          | distance_stop  | Distance to next stop                         |
| Extensions. Distances.CallDistanceAlongRoute   | distance_shape | Stop_s total distance along the shape         |
| Extensions. Distances.PresentableDistance      | status         | Report the current status of bus to next stop |
| DestinationRef                                 | destination    | Headsign of bus                               |
