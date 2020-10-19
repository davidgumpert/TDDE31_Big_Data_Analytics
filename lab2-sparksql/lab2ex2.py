from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
sc = SparkContext(appName = "Lab2ex2")
sqlContext = SQLContext(sc)

# Importing needednecessary files 
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
temp_lines = temp_file.map(lambda line: line.split(";"))

# Mapping data in rows with appropriate columns
tempReadingsRow = temp_lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], value=float(p[3]), quality=p[4]))

# Creating a sql data frame
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow)
schemaTempReadings.registerTempTable("tempReadingsTable")

# Getting all temperatures above 10 deg for each month and order by descending temp
high_temps = schemaTempReadings.select("year", "month", "value").filter((schemaTempReadings["year"]>=1950) & (schemaTempReadings["year"]<=2014)).groupBy("year", "month").agg(F.count(schemaTempReadings["value"]>10).alias("value")).orderBy("value", ascending=False)

# Getting all disctinct station temperatures above 10 deg for each month and order by descending temp
high_dist_temps = schemaTempReadings.select("year", "month", "value", "station").filter((schemaTempReadings["year"]>=1950) & (schemaTempReadings["year"]<=2014) & (schemaTempReadings["value"] > 10)).select("year", "month", "station").distinct().groupBy("year", "month").agg(F.count("station").alias("value")).orderBy("value", ascending=False)

# Saving results. Either all or just the distinct.
high_temps.rdd.coalesce(1,shuffle=True).saveAsTextFile("BDA/output")
#high_dist_temps.rdd.coalesce(1,shuffle=True).saveAsTextFile("BDA/output")