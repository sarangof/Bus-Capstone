# -*- coding: utf-8 -*-
"""
Created on Tue May 24 23:46:23 2016

This script takes a file of AVL data and plots the distributions of vehicle
response frequency (seconds between observations) and of SIRI API call times
(seconds between ResponseTimeStamps)

Requires the filepath as an argument

@author: mu529
"""

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

results = pd.read_csv(sys.argv[1])
results['RecordedAtTime'] = pd.to_datetime(results['RecordedAtTime'])
# results['MonitoringTimeStamp'] = pd.to_datetime(results['MonitoringTimeStamp'])
results['ResponseTimeStamp'] = pd.to_datetime(results['ResponseTimeStamp'])
sorted_data = results.sort(columns=['vehicleID','RecordedAtTime'])
sorted_data['ElapsedSeconds'] = sorted_data['RecordedAtTime'].diff()/np.timedelta64(1, 's')
sorted_rtimes = pd.Series(data=np.sort(results['ResponseTimeStamp'].unique()))

plt.figure(1)
plt.hist(sorted_data['ElapsedSeconds'].dropna().values,bins=30,range=(0,119))
plt.title("Elapsed Seconds Between Vehicle Observations")
plt.xlabel("Seconds")
plt.ylabel("Occurences")

plt.figure(2)
plt.hist(sorted_rtimes.diff()/np.timedelta64(1, 's'),bins=30,range=(0,119))
plt.title("Elapsed Seconds Between SIRI Timestamps")
plt.xlabel("Seconds")
plt.ylabel("Occurences")
plt.show()
