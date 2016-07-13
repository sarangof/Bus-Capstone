SELECT Line, TripDate, count(TripRef) AS trip_id 
FROM record 
GROUP BY Line, TripDate
ORDER BY TripDate ASC