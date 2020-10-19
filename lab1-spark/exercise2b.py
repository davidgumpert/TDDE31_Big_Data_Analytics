from pyspark import SparkContext

sc = SparkContext(appName = "exercise 2b")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_and_month_temperature = lines.map(lambda x: (x[1][0:7], (x[0],float(x[3]))))

#filter
year_and_month_temperature = year_and_month_temperature.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)

#Get all temperature > 10
high_temperatures = year_and_month_temperature.filter(lambda x: int(x[1][1])>=10)

#Map to prep for count
high_temperatures = high_temperatures.map(lambda x: (x[1][0],(x[0],1))).distinct().map(lambda x: (x[1][0], x[1][1]))

#Count number of temperatures above 10 degrees for each month
count_of_high_temperatures = high_temperatures.reduceByKey(lambda a,b: a + b)

#Coalesce, sort and save
count_of_high_temperatures = count_of_high_temperatures.coalesce(1, shuffle=True)
count_of_high_temperatures = count_of_high_temperatures.sortBy(ascending = False, keyfunc=lambda k:int(k[0][0:4]))
count_of_high_temperatures.saveAsTextFile("BDA/output")

