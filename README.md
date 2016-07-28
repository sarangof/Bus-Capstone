# Bus-Capstone
Capstone project for NYC Department of Transportation.

### Important documentation:

*   Documentation of data processing and Spark
    * __Python Data Processing__
        * [Demostration](https://github.com/sarangof/Bus-Capstone/tree/master/demonstration): Ipython Notebook that demonstrates all the process

        * Modules created

            1. [Siri Tools](https://github.com/sarangof/Bus-Capstone/blob/master/siri_parser.py): Modules to parse the original JSON files
            2. [Time Tools](https://github.com/sarangof/Bus-Capstone/blob/master/ttools.py): Homemade Timedelta Converter
            3. [GTFS](https://github.com/sarangof/Bus-Capstone/blob/master/gtfs.py): Extract the Schedule Data from GTFS Schedules(originally in ZIP)
            4. [Arrival Time](https://github.com/sarangof/Bus-Capstone/blob/master/arrivals.py):Estimated the arrival time for each stop using Scipy [KD-Tree](http://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.spatial.KDTree.html) and [Interpolate](http://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html)

    * __[Processing using SPARK](https://github.com/sarangof/Bus-Capstone/tree/master/Spark#parsing-and-manipulate-bus-time-data-using-pyspark)__

      * For details, check the [Spark](https://github.com/sarangof/Bus-Capstone/tree/master/Spark) folder.

*   [Sponsor report](https://github.com/sarangof/Bus-Capstone/blob/master/paper/sponsor_report_final.pdf)

*   [Technical report](https://github.com/sarangof/Bus-Capstone/blob/master/paper/technical_report.pdf)

## Final Product Sample
Darker means poorer on time performance for the buses

![alt text](https://github.com/sarangof/Bus-Capstone/blob/master/plots/on_time_performance_stops.png "Sample of on time performance")

Open Interactive Map in [Carto Map](https://saf537.carto.com/viz/c21efdeb-ec45-45f2-b2d3-c47993bb89ff/public_map)
