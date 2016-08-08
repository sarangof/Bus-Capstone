#### Data retrieval modules
##### siri_repeat
Calls the BusTime API and saves the result json  
When executed directly, it takes two arguments: (1) what second on-the-minute it runs and (2) the name of a single-line file containing the API key  
Example: `python siri_repeat.py 15 MYKEY` will launch the script (until interrupted) and call the SIRI API every minute at the 15-second mark, using the API key stored in local file named MYKEY.  
Results are saved in directory `jsons/`  
* *method* siri_repeat.**get_bustime**(api_key)  
   Method for calling Bus Time API once.  Takes key directly as **string* format.  
   Returns **json** datatype.
##### extract_trip_dates
   Script parses a single unsorted CSV and extracts records where the value of 7th column (`trip_date`, in this case) matches a list.
   Edit script directly to change file path and/or column index.
##### clean_bustime
   Cleans AVL data by removing any pings that report a "next stop" that is not valid for the trip referenced.
   Takes three arguments of path to file for cleaning, date of schedule, and path of gtfs data
   *method* clean_bustime.**valid_row**(row)
      Method for checking if `STOP_ID` element is contained in `stop_id` list of valid stops
   *method* clean_bustime.**filter_invalid_stops**(avl_df,stoptime_df)
      Method takes raw Bus Time data and GTFS schedule data and filters Bus Time records preceding an invalid stop for the reported trip

##### siri_parser
   *method* siri_parser.**json_to_df**(a)
      Single required argument *a* is a single json string received from the siri api.
      return a pandas dataframe with the following columns: `Line`, `RecordedAtTime`, `Latitude`, `Longitude`, `Trip`, `TripDate`, `ResponseTimeStamp`
   *method* siri_parser.**extract**(inpath,outfile)
   *method* siri_parser.**extract_trip_dates**(inpath,outpath,datelist)