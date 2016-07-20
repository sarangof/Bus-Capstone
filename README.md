##Bus Reliability Metrics using Public MTA Bus Time Data
#### Capstone Project of the New York University Center for Urban Science and Progress

* [Technical report]: (https://github.com/sarangof/Bus-Capstone/blob/master/paper/technical_report.pdf)

* [Sponsor report:] (https://github.com/sarangof/Bus-Capstone/blob/master/paper/sponsor_report.pdf)

#### Data retrieval modules
##### siri_parser
*method* siri_parser.**json_to_df**(a)

Single required argument *a* is a single json string received from the siri api.
    
return a pandas dataframe with the following columns: `Line`, `RecordedAtTime`, `Latitude`, `Longitude`, `Trip`, `TripDate`, `ResponseTimeStamp`

##### gtfs

