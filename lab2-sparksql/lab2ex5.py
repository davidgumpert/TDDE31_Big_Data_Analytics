from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql import HiveContext

sc = SparkContext(appName = "Lab2ex5")
sqlContext = SQLContext(sc)

station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
prec_file = sc.textFile("BDA/input/precipitation-readings.csv")

station_lines = station_file.map(lambda line: line.split(";"))
prec_lines = prec_file.map(lambda line: line.split(";"))

stationReadingsRow = station_lines.map(lambda p: Row(station=p[0]))

precReadingsRow = prec_lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], day=p[1].split("-")[2], time=p[2], value=float(p[3])))

schemaStationReadings = sqlContext.createDataFrame(stationReadingsRow)
schemaStationReadings.registerTempTable("stationReadingsTable")

schemaPrecReadings = sqlContext.createDataFrame(precReadingsRow)
schemaPrecReadings.registerTempTable("precReadingsTable")

focal_readings = schemaStationReadings.join(schemaPrecReadings, ["station"])

focal_readings = focal_readings.select("year", "month", "station", "value").filter((focal_readings["year"]>=1993) & (focal_readings["year"]<=2016)).groupBy("year", "month", "station").agg(F.sum("value").alias("sumMonth")).select("year", "month", "sumMonth").groupBy("year", "month").agg(F.avg("sumMonth").alias("avgMonthPrec")).orderBy(["year","month"], ascending=[False,False])

focal_readings.rdd.coalesce(1,shuffle=True).saveAsTextFile("BDA/output")
