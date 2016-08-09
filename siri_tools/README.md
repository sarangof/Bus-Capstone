#### Data retrieval modules
##### [siri_repeat](siri_repeat.py)
Calls the BusTime API and saves the result json.  When executed directly, it takes two arguments: (1) what second on-the-minute it runs and (2) the name of a single-line file containing the API key.  Results are saved in directory `jsons/`  
Example: `python siri_repeat.py 15 MYKEY` will launch the script (until interrupted) and call the SIRI API every minute at the 15-second mark, using the API key stored in local file named MYKEY.  

* *method* siri_repeat.**get_bustime**(api_key)  

    for calling Bus Time API once.  Takes key directly as *string* format.  
    **returns** *json* datatype.  
   
##### [extract_trip_dates](extract_trip_dates.py)
Script parses a single unsorted CSV and extracts records where the value of 7th column (`trip_date`, in this case) matches a list.  Edit script directly to change file path and/or column index.  

##### [clean_bustime](clean_bustime.py)
Cleans AVL data by removing any pings that report a "next stop" that is not associated with the reported `TRIP_ID`.  Takes three arguments: path to file for cleaning, date of schedule, and path of gtfs data.  
Example: `python clean_bustime.py bustime_parsed.csv 2015-12-03 gtfs/` will create a file `bustime_parsed_cleaned.csv` that excludes records without valid `STOP_ID` elements.  
* *method* clean_bustime.**valid_row**(row)  

    for checking if `STOP_ID` element is contained in `stop_id` list of valid stops  
	**returns** *bool*
* *method* clean_bustime.**filter_invalid_stops**(avl_df,stoptime_df)  

    takes raw Bus Time data and GTFS schedule data and filters Bus Time records preceding an invalid stop for the reported trip  
	**returns** *pandas DataFrame* of filtered records

##### [siri_parser](siri_parser.py)
This module is for parsing and extracting from raw json data files.  If called directly, the first argument required is the name of the directory where the json files are located (all contents of the directory will be processed). The second argument is the name of the output CSV file.  
Example: `python siri_parser.py jsons bustime_parsed.csv` will create a file `bustime_parsed.csv` after extracting these elements from each JSON.  
* *method* siri_parser.**json_to_df**(a)  

    takes a single siri response json in *string* format as *a*.  See [Data Schema](../Spark#data-schema) for translation of field names from SIRI standard.   
	**returns** *pandas DataFrame* with the following columns: `ROUTE_ID`, `recorded_time`, `latitude`, `longitude`, `TRIP_ID`, `trip_date`, `destination`, `destination_name`, `SHAPE_ID`, `STOP_ID`, `EstCallArrival`, `distance_stop`, `distance_shape`, `status`, `ResponseTimeStamp`  
* *method* siri_parser.**extract**(inpath,outfile)  

    takes *jsons* aggregated file, where each row is a single *json* string.
	writes a new CSV with parsed data using **json_to_df** method.  