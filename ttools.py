# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 18:52:10 2016
### NOT COMPLETE ###
This code will custom functions used for parsing the date and time info from
both schedule (GTFS) and actual (siri) data.

Based on use of regex in:
http://stackoverflow.com/a/4628148

@author: mu529
"""

import re
import pytz
import datetime
import pandas as pd

def parseTimeDelta(s):
    """Create timedelta object representing time delta
       expressed in a string
   
    Takes a string in the format produced by calling str() on
    a python timedelta object and returns a timedelta instance
    that would produce that string.
   
    Acceptable formats are: "X days, HH:MM:SS" or "HH:MM:SS".
    """
    if s is None:
        return None
    d = re.match(
            r'((?P<days>\d+) days, )?(?P<hours>\d+):'
            r'(?P<minutes>\d+):(?P<seconds>\d+)',
            str(s)).groupdict(0)
    return datetime.timedelta(**dict(( (key, int(value))
                              for key, value in d.items() )))

tds = pd.to_datetime('2016-06-13') + day_summary['arrival_time'].apply(parseTimeDelta)
utcoffset = pytz.timezone(tz_sched).localize(datetime.datetime(2016,6,13)).strftime('%z')
utcoffset = datetime.timedelta(hours=int(utcoffset[:3]), minutes=int(utcoffset[4:]))
day_summary['arrival_scheduled'] = tds - utcoffset
del day_summary['arrival_time']

day_summary.to_pickle('WIP.pkl')
day_summary.to_csv('day_summary.csv')