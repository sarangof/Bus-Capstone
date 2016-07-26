# -*- coding: utf-8 -*-
"""
This script calls the BusTime API and saves the result json
The first argument is what second on-the-minute it runs
The second arguemnt is the name of a single-line file containing the API key 

Results are saved in directory jsons
"""
import json
import urllib2
import time
import sys
import numpy as np

def get_bustime(api_key):
    url = ('http://bustime.mta.info/api/siri/vehicle-monitoring.json?key='+
        api_key+'&version=2&VehicleMonitoringDetailLevel=basic')
    request = urllib2.urlopen(url)
    data = json.loads(request.read())
    return data
        
if __name__=='__main__':
    with open(sys.argv[2],'rb') as keyfile:
        api_key = keyfile.read().replace('\n','')
    counter = 0
    starttime=int(np.ceil(time.time() - (time.time() % 60)) + np.float(sys.argv[1]))
    looptime = starttime    
    print 'Starting at ' + str(starttime)
    while True:
        apistart = 1*time.time()
        data = get_bustime(api_key)
        apifinish = 1*time.time()
        try:
            rts = data['Siri']['ServiceDelivery']['ResponseTimestamp'][:19]
            rts = rts.replace(':','')
            with open('jsons/' + rts + '.json','wb') as outfile:
                json.dump(data,outfile)
        except:
            print 'Error on count ' + str(counter)
        if apifinish < apistart + 58.0:
            time.sleep(60.0 - ((time.time() - starttime) % 60.0))            
        else:
            pass
        with open('siri_logs/'+str(starttime)+'_log.txt','a') as logfile:
            logfile.write(str(counter)+','+str(apistart)+','+str(apifinish)+'\n')
        counter += 1 
        
    
    
