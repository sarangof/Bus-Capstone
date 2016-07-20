SELECT Line, TripDate, count(RecordedAtTime) AS trips 
FROM record 
GROUP BY Line, TripDate