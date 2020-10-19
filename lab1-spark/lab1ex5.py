from pyspark import SparkContext
sc = SparkContext(appName = "Exercise 5")

#Import stations and precipitation file
stations_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
rain_file = sc.textFile("BDA/input/precipitation-readings.csv")

#Split the two imported files on ";"
station_lines = stations_file.map(lambda line: line.split(";"))
rain_lines = rain_file.map(lambda line: line.split(";"))

#Map rain to a key-value pair. ((Station, year-month), precipitation)
rain = rain_lines.map(lambda x: ((x[0],x[1][0:7]), float(x[3])))
rain = rain.filter(lambda x: (int(x[0][1][0:4]) <= 2016 and int(x[0][1][0:4]) >= 1993))

# List of all stations relevent
stations = station_lines.map(lambda x: (x[0])).collect()

#Filter away stations not i Region ostergotland
rain_ost = rain.filter(lambda x: (x[0][0] in stations))

#Sum the precipitation for each station, each month
rain_ost_month_station = rain_ost.reduceByKey(lambda a,b: a+b)

#Map rain_ost_mont_station to get key-vale pair (Year-month, (Sum of precipitation that month, count))
rain_ost_month = rain_ost_month_station.map(lambda x: ((x[0][1]),(x[1], 1)))

#Sum total precipitation of each month and count number of measurements made
rain_ost_month = rain_ost_month.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))

#Get average precipitation of each month for region Ostergotland
rain_ost_month_avg = rain_ost_month.mapValues(lambda x: (x[0]/x[1]))

#Coalesce and sort
output = rain_ost_month_avg.coalesce(1, shuffle=True)
output_sorted = output.sortBy(ascending = False, keyfunc=lambda x: (int(x[0][0:4])))
output_sorted.saveAsTextFile("BDA/output")
