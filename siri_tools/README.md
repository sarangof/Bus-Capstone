#### Data retrieval modules
##### siri_repeat
Calls the BusTime API and saves the result json
When executed directly, it takes two arguments: (1) what second on-the-minute it runs and (2) the name of a single-line file containing the API key 
Example: 'python siri_repeat.py 15 MYKEY' will launch the script (until interrupted) and call the SIRI API every minute at the 15-second mark, using the API key stored in local file named MYKEY.
Results are saved in directory 'jsons/'

*method* siri_repeat.**get_bustime**(api_key)
Method for calling Bus Time API once.  Takes key directly as **string* format.
Returns **json** datatype.
##### extract_trip_dates
##### clean_bustime
##### siri_parser
*method* siri_parser.**json_to_df**(a)

Single required argument *a* is a single json string received from the siri api.
    
return a pandas dataframe with the following columns: `Line`, `RecordedAtTime`, `Latitude`, `Longitude`, `Trip`, `TripDate`, `ResponseTimeStamp`

*method* siri_parser.**extract**(inpath,outfile)

*method* siri_parser.**extract_trip_dates**(inpath,outpath,datelist)