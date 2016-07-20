import pandas as pd
import sqlite3

conn = sqlite3.connect('bustime.sqlite')
df = pd.read_sql_query('SELECT * from avl_data WHERE trip_date IS "2015-12-03"',conn)
print df.columns
print df.shape
conn.close()
