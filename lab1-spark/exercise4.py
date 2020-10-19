from pyspark import SparkContext
sc = SparkContext(appName = "exercise 4")

#Import temperature and precipitation files
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
rain_file = sc.textFile("BDA/input/precipitation-readings.csv")

#Split the two imported files on ";"
temp_lines = temperature_file.map(lambda line: line.split(";"))
rain_lines = rain_file.map(lambda line: line.split(";"))

#Map the temperature file to key-value pair. (Station, temperature)
#Map the precipitation file to key-value pair. (Station, precipitation)
temperatures = temp_lines.map(lambda x: (x[0], float(x[3])))
rain = rain_lines.map(lambda x: (x[0], float(x[3])))

#Reduce temperatures to get maximum temperature for each station
#Reduce rain to get maximum precipitation for each station
temperatures = temperatures.reduceByKey(lambda a,b: max(a,b))
rain = rain.reduceByKey(lambda a,b: max(a,b))

#Filter to get temperatures and precipitation within the scope of the assignment
temperatures = temperatures.filter(lambda x: x[1] >= 25 and x[1] <= 30)
rain = rain.filter(lambda x: x[1] >= 100 and x[1] <= 200)

#Join and save
temp_rain = temperatures.join(rain).coalesce(1, shuffle=True)
temp_rain.saveAsTextFile("BDA/output")
