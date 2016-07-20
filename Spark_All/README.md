# Spark_project

## This project reads 3 TB of nested JSON data and apply bunch of spark techniques to analysis the bus delays and headways

## Techniques Included


Read JSON & CSV
```
sqlContext.read.json()
qlContext.read.format('com.databricks.spark.csv').options(header='true').load()
```

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

```
`Inner join` the interpolated data with GTFS schedule
`IF` `COUNT` to calculate the multiple on time perfornace.
```
For more info. Refer `ontime_ratio/ontime_ratio.sql`
## Data Schema
```
| JSON ELEMENT(schema)                           | Column NAME    | explanation                                                 |                           |                 | 
|------------------------------------------------|----------------|-------------------------------------------------------------|---------------------------|-----------------| 
| LineRef                                        | ROUTE_ID       | Name of bus line(B42)                                       |                           |                 | 
| VehicleLocation.Latitude                       | latitude       | latitude of record                                          |                           |                 | 
| VehicleLocation.Longitude                      | longitude      | longitude of record                                         |                           |                 | 
| RecordedAtTime                                 | recorded_time  | What time it get recorded                                   |                           |                 | 
| VehicleRef                                     | vehicle_id     | ID of vehicle                                               |                           |                 | 
| FramedVehicleJourneyRef.DatedVehicleJourneyRef | TRIP_ID        | Same as trip_id in GTFS*                                    |                           |                 | 
| FramedVehicleJourneyRef.DataFrameRef           | trip_date      | Date of the trip                                            |                           |                 | 
| JourneyPatternRef                              | SHAPE_ID       | Same as shape_id in GTFS*                                   |                           |                 | 
| StopPointRef                                   | STOP_ID        | "Id of next stop                                            | Same as stop_id in GTFS*" |                 | 
| Extensions.Distances.DistanceFromCall          | distance_stop  | Distance to next stop                                       |                           |                 | 
| Extensions. Distances.CallDistanceAlongRoute   | distance_shape | Stop_s total distance along the shape                       |                           |                 | 
| Extensions. Distances.PresentableDistance      | status         | "Report the current status of bus to next stop( approaching |  at stop                  |  <1 stop away)" | 
| DestinationRef                                 | destination    | Headsign of bus                                             |                           |                 | 
```