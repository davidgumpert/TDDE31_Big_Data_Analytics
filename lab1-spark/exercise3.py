from pyspark import SparkContext
sc = SparkContext(appName = "exercise 3")

#Read file and split
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

#Map for the key-value pairs: (key, value) = ((year-month, station), temperature)
monthly_temperature = lines.map(lambda x: ((x[1][0:7], x[0]), (float(x[3]),1)))

#Filter over year span
filtered_monthly_temperature = monthly_temperature.filter(lambda x: int(x[0][0][0:4])>=1950 and int(x[0][0][0:4])<=2014)

#Sum temperature and count on each month.
sum_monthly_temperature = filtered_monthly_temperature.reduceByKey(lambda a,b: (a[0] + b[0],a[1]+b[1]))

# Divide the sum of the temperature by the count of readings that month 
avg_monthly_temperature = sum_monthly_temperature.map(lambda x: (x[0], x[1][0]/x[1][1]))

# Coalesce and sort
avg_monthly_temperature = avg_monthly_temperature.coalesce(1, shuffle=True)
avg_monthly_temperature = avg_monthly_temperature.sortBy(ascending = False, keyfunc=lambda k:int(k[0][0][0:4]))

#Save to file
avg_monthly_temperature.saveAsTextFile("BDA/output")
