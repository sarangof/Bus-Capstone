SELECT ROUTE_ID,STOP_ID, date,\
                      COUNT(IF((delay BETWEEN -60 AND 300),1,null))/COUNT(delay) as ontime_ratio,\
                      COUNT(IF((HOUR(realtime) BETWEEN 6 AND 9) OR (HOUR(realtime) BETWEEN 16 AND 19) AND (delay BETWEEN -300 AND 300),1,null))/COUNT(IF((HOUR(realtime) BETWEEN 6 AND 9) OR (HOUR(realtime) BETWEEN 16 AND 19),1,null)) as peak_wait,\
                      COUNT(IF((HOUR(realtime) NOT BETWEEN 6 AND 9) OR (HOUR(realtime) NOT BETWEEN 16 AND 19) AND (delay BETWEEN -300 AND 300),1,null))/COUNT(IF((HOUR(realtime) NOT BETWEEN 6 AND 9) OR (HOUR(realtime) NOT BETWEEN 16 AND 19),1,null)) as peak_wait\
                      FROM new_join\
                      GROUP BY ROUTE_ID, STOP_ID, date