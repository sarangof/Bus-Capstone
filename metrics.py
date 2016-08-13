import os
import pandas as pd
import sys
sys.path.append(os.getcwd())

# these modules are homemade
import gtfs
import ttools

def load_schedule(tdate,dpath):
	trips = gtfs.load_trips(tdate,dpath)
	stop_times, tz_sched = gtfs.load_stop_times(tdate,dpath)
	tcal=gtfs.TransitCalendar(tdate,dpath)
	active_services = tcal.get_service_ids(tdate)
	active_trips = trips.service_id.isin(active_services)
	active_stops = stop_times.reset_index().set_index('trip_id').loc[active_trips]
	active_stops['sched_hour'] = active_stops.arrival_time.str[:2].astype(int)
	active_stops['sched_arrival_time'] = active_stops.arrival_time.apply(ttools.parseTime)
	sched_times = active_stops.join(trips['route_id'],how='left')
	sched_times = sched_times.reset_index().sort(['route_id','sched_arrival_time'])
	sec = ttools.datetime.timedelta(seconds=1)
	sched_times['sched_headway'] = sched_times.groupby(['route_id','stop_id'])['sched_arrival_time'].diff()/sec
	sched_times.set_index(['trip_id','stop_id'],inplace=True,verify_integrity=True)
	return sched_times

def series_max(s):
    return s.idxmax()[1]

def peak_hour_int(x):
    if x >= 7 and x < 9:
        return True
    elif x >= 16 and x < 19:
        return True
    else:
        return False

def wait_ass(x,y):
    if x == True:
        if y >= 3*60:
            return False
        else:
            return True
    if x == False:
        if y>= 5*60:
            return False
        else:
            return True         

def headway_regularity(sche,inter):
    if inter >= 0.5*sche and inter <= 1.5*sche:
        return True
    else:
        return False 

def On_Time_Per(sche,inter):
    if inter > sche - 60 and inter <= sche + 300:
        return True
    else:
        return False

def process_metrics(infile,gtfs_path,outfile):
	interpolated = pd.read_csv(infile)
	interpolated['interpolated_arrival_time'] = pd.to_timedelta(interpolated['interpolated_arrival_time'])
	sec = ttools.datetime.timedelta(seconds=1)
	results = pd.DataFrame(columns=['wait_ass','OnTimeP','STOP_ID'])
	results.index.rename('ROUTE_ID',inplace=True)    
	# write initial output file with headers but no data
	results.to_csv(outfile)
	for td in interpolated.trip_date.unique():
		sched_times = load_schedule(td,gtfs_path)
		subset = interpolated[interpolated['trip_date']==td]
		merged = subset.merge(sched_times[['sched_arrival_time','sched_hour','sched_headway']],how='left',left_on=['TRIP_ID','STOP_ID'],right_index=True)
		merged.set_index(['ROUTE_ID','STOP_ID'],inplace=True)
		trip_groups = merged.groupby(level=(0,1)).size()
		densest_stops = trip_groups.groupby(level=(0)).apply(series_max)
		merged = merged.set_index('interpolated_arrival_time',append=True,drop=False).sort_index()
		merged['inter_headway'] = merged.groupby(level=(0,1))['interpolated_arrival_time'].diff()/sec
		merged['P_hour'] = map(lambda x:peak_hour_int(x),merged['sched_hour'])
		merged['hw_diff'] = merged['inter_headway']-merged['sched_headway']
		merged['wait_ass'] = map(lambda x,y:wait_ass(x,y), merged['P_hour'],merged['hw_diff'])
		merged['headway_reg']=map(lambda x,y:headway_regularity(x,y), merged['sched_headway'],merged['inter_headway'])
		merged['OnTimeP'] = map(lambda x,y:On_Time_Per(x,y), merged['sched_headway'],merged['inter_headway'] )
		metrics = 1.0 * merged.groupby(level=(0,1))[['wait_ass','OnTimeP']].mean()
		metrics['trip_date'] = td
		metrics.reset_index(level=1,inplace=True)
		metrics.to_csv(outfile,mode='a',header=False)

if __name__=='__main__':
	infile = sys.argv[1]
	gtfs_path = sys.argv[2]
	outfile = sys.argv[3]
	process_metrics(infile,gtfs_path,outfile)
