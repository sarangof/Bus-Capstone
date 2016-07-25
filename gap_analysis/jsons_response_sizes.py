# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 22:41:40 2016

@author: mu529
"""

import os
import pandas as pd
os.chdir('/gpfs2/projects/project-bus_capstone_2016/datamarts/BusTime/')

def summarize_jsonline(a):
    first = a.index('"ServiceDelivery":{"ResponseTimestamp"') + 40
    last = a.index('"ServiceDelivery":{"ResponseTimestamp"') + 69
    r = a[first:last]
    veh_cnt = a.count('VehicleLocation')
    v_len  =  a.rfind('RecordedAtTime') - a.index('{"VehicleActivity":')
    return {'response_time_stamp':r, 'veh_count':veh_cnt, 'veh_str_len':v_len}
    
def summarize_jsons(fpath):
    with open(fpath, 'r') as fi:
        jsons_extract = pd.DataFrame(columns=['response_time_stamp','veh_count','veh_str_len'])
        error_list = []
        for cnt,jsonline in enumerate(fi):
            try:
                results = summarize_jsonline(jsonline)
                jsons_extract = jsons_extract.append(results,ignore_index=True)
                # results.to_csv(outfile,mode='a',header=False)
            except:
                error_list.append(cnt)
    jsons_extract['filename'] = fpath.replace('.jsons','')
    return jsons_extract,error_list

accumulated = pd.DataFrame(columns=['response_time_stamp','veh_count','veh_str_len','filename'])
outfile = '/gpfs2/projects/project-bus_capstone_2016/workspace/share/jsons_summary.csv'
errorfile = '/gpfs2/projects/project-bus_capstone_2016/workspace/share/jsons_errors.txt'
accumulated.to_csv(outfile)
for fpath in os.listdir(os.getcwd()):
    try:
        je,error_list = summarize_jsons(fpath)
        je.to_csv(outfile,mode='a',header=False, float_format='%.0f')
        with open(errorfile,'a') as logfile:
            logfile.write(fpath.replace('.jsons','')+','+str(error_list)+'\n')
    except:
        with open(errorfile,'a') as logfile:
            logfile.write(fpath.replace('.jsons','')+',FileError\n')       