from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Lab2ex1")
sqlContext = SQLContext(sc)

#Importing temperature file and splitting it on ";"
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
temp_lines = temp_file.map(lambda line: line.split(";"))

#Mapping data in rows with appropriate columns
tempReadingsRow = temp_lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], time=p[2], value=float(p[3]), quality=p[4]))

#Creating sql dataframe
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow)
schemaTempReadings.registerTempTable("tempReadingsTable")

#----------------------------- FOR MAX -----------------------------#
#Grouping by value
max_temps = schemaTempReadings.select("year", "value").filter((schemaTempReadings["year"]>=1950) & (schemaTempReadings["year"]<=2014)).groupBy("year").agg(F.max("value").alias("value"))

#Creating stations to join with max_temps since we lose station when selecting on  year and value above
stations = schemaTempReadings.select("year", "station", "value")

#Joining temps with station on year and temperature  and ordering by value (temperature) in descending order
max_temps_station = max_temps.join(stations, ["year", "value"]).orderBy("value", ascending = False)

#Save to output in one file
max_temps_station.rdd.coalesce(1, shuffle=False).saveAsTextFile("BDA/output")

#----------------------------- FOR MAX -----------------------------#
#Grouping by value
min_temps = schemaTempReadings.select("year", "value").filter((schemaTempReadings["year"]>=1950) & (schemaTempReadings["year"]<=2014)).groupBy("year").agg(F.max("value").alias("value"))

#Creating stations to join with max_temps since we lose station when selecting on  year and value above
stations = schemaTempReadings.select("year", "station", "value")

#Joining temps with station on year and temperature  and ordering by value (temperature) in descending order
min_temps_station = min_temps.join(stations, ["year", "value"]).orderBy("value", ascending = False)

#Save to output in one file
min_temps_station.rdd.coalesce(1, shuffle=False).saveAsTextFile("BDA/output")
