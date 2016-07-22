##Bus Reliability Metrics using Public MTA Bus Time Data
#### Capstone Project of the New York University Center for Urban Science and Progress

* [Technical report] (https://github.com/sarangof/Bus-Capstone/blob/master/paper/technical_report.pdf)

* [Sponsor report] (https://github.com/sarangof/Bus-Capstone/blob/master/paper/sponsor_report.pdf)


Inline-style: 
![alt text](https://github.com/sarangof/Bus-Capstone/blob/master/plots/on_time_performance_stops.png "Sample of on time performance")



#### Data retrieval modules
##### siri_parser
*method* siri_parser.**json_to_df**(a)

Single required argument *a* is a single json string received from the siri api.
    
return a pandas dataframe with the following columns: `Line`, `RecordedAtTime`, `Latitude`, `Longitude`, `Trip`, `TripDate`, `ResponseTimeStamp`

##### gtfs

### Spark Method

The data processing and manipulating is all done in Spark for the whole dataset in 2015.
Use command 
```
./XXX.sh XXX.py XXX.sql output XXX.csv [NumofExecutors]
```
to submit the job
#### For more info
please refer `Spark_All` for all the information
