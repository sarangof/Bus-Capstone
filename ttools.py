# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 18:52:10 2016
This code will custom functions used for parsing the date and time info from
both schedule (GTFS) and actual (siri) data.

parseTime() is meant for schedule times from GTFS, since there is no need to
interpret the date.

parseActualTime() is meant for actual arrival times from Siri, and requires
an additional argument tdate so it knows the trip reference date, since some
trips occur after midnight.

Based on use of regex in:
http://stackoverflow.com/a/4628148

@author: mu529
"""

import re
import datetime
import pandas as pd

def parseTime(s):
    """Create timedelta object representing time delta
       expressed in a string
   
    Takes a string in the format produced by calling str() on
    a python timedelta object and returns a timedelta instance
    that would produce that string.
   
    Acceptable formats are: "X days, HH:MM:SS" or "HH:MM:SS".
    """
    if pd.isnull(s):
        return None
    d = re.match(
            r'((?P<days>\d+) days, )?(?P<hours>\d+):'
            r'(?P<minutes>\d+):(?P<seconds>\d+)',
            s).groupdict(0)
    return datetime.timedelta(**dict(( (key, int(value))
                              for key, value in d.items() )))

def parseActualTime(tstring,tdate):
    """Takes in an ISO8601 format string and returns a timedelta
    Also requires the trip reference date as a string YYYY-MM-DD
    Returns elapsed time since reference date as timedelta"""
    if pd.isnull(tstring):
        return None
    if tdate==tstring[:10]:
        doffset=0
    else:
        doffset=1
    s = str(doffset) + ' days, ' + tstring[11:19] # no millisecs
    return parseTime(s)

if __name__=='__main__':
    import os
    os.chdir('/gpfs2/projects/project-bus_capstone_2016/workspace/share')
    m5 = pd.read_csv('day_summary.csv')
    print 'Here is an example of parsing actual arrival times:'
    print ''
    actual_parsed = m5.arrival_actual_estimated.apply(parseActualTime,
                                                      tdate='2016-06-13')
    print actual_parsed.head()
    print ''
    print 'Here is an example of parsing schedule arrival times:'
    print ''
    schedule_parsed = m5.arrival_time.apply(parseTime)
    print schedule_parsed.head()
