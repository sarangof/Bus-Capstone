import sqlite3

with sqlite3.connect('bustime.sqlite') as cnx:
    c = cnx.cursor()
    c.execute('CREATE INDEX avl_index ON avl_data (ROUTE_ID, trip_date)')
    cnx.commit()
