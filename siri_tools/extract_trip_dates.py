# -*- coding: utf-8 -*-
"""
Created on Wed Jul 20 05:13:57 2016

@author: mu529
"""

import csv

inpath = '/gpfs2/projects/project-bus_capstone_2016/workspace/share/all.csv'
outpath = '/gpfs2/projects/project-bus_capstone_2016/workspace/share/dec2015_extract.csv'
datelist = ['2015-12-01','2015-12-02','2015-12-03','2015-12-04','2015-12-05','2015-12-06','2015-12-07']

row_count = 0
with open(inpath, "rb") as csvfile, open(outpath,'w') as outfile:
    datareader = csv.reader(csvfile)
    writer = csv.writer(outfile, delimiter=',')
    for row in datareader:
        if row[6] in datelist:
            row_count += 1
            writer.writerow(row)
            
            
