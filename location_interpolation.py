

# For each trip id
# 	For each record (different time stamps?)
# 		Get position
import os
import pandas as pd
import numpy as np

import gtfs
import arrivals
os.chdir('/green-projects/project-bus_capstone_2016/workspace/share')

trips = gtfs.load_trips('gtfs/')
stops = gtfs.load_stops('gtfs/')
stop_times = gtfs.load_stop_times('gtfs/')
print 'Finished loading GTFS data.'