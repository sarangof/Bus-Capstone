import csv
import sqlite3

with open('all.csv') as f, sqlite3.connect('bustime.sqlite') as cnx:
    reader = csv.reader(f)
    c = cnx.cursor()
    c.execute('CREATE TABLE avl_data (ROUTE_ID,latitude,longitude,recorded_time,vehicle_id,TRIP_ID,trip_date,SHAPE_ID,STOP_ID,distance_stop,distance_shape,status,destination)')
    c.executemany('INSERT INTO avl_data values (?,?,?,?,?,?,?,?,?,?,?,?,?)', reader)
    cnx.commit()
