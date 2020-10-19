from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql import HiveContext

sc = SparkContext(appName = "Lab2ex3")
sqlContext = SQLContext(sc)

temp_file = sc.textFile("BDA/input/temperature-readings.csv")
temp_lines = temp_file.map(lambda line: line.split(";"))
prec_file = sc.textFile("BDA/input/precipitation-readings.csv")
prec_lines = prec_file.map(lambda line: line.split(";"))

tempReadingsRow = temp_lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], day=p[1].split("-")[2], time=p[2], value=float(p[3])))

precReadingsRow = prec_lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], day=p[1].split("-")[2], time=p[2], value=float(p[3])))

schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow)
schemaTempReadings.registerTempTable("tempReadingsTable")

schemaPrecReadings = sqlContext.createDataFrame(precReadingsRow)
schemaTempReadings.registerTempTable("precReadingsTable")

daily_prec = schemaPrecReadings.select("year", "month", "day", "station", "value").groupBy("year", "month", "day", "station").agg(F.sum("value").alias("sumPrec"))

daily_temp = schemaTempReadings.select("year", "month", "day", "station", "value").groupBy("year", "month", "day", "station").agg(F.max("value").alias("maxTemp"))

prec_temp = daily_prec.join(daily_temp, ["year", "month", "day", "station"]).select("station", "sumPrec", "maxTemp").groupBy("station").agg(F.max("sumPrec").alias("stationMaxPrec"), F.max("maxTemp").alias("stationMaxTemp")).orderBy("station", ascending=False)

filtered_max = prec_temp.filter((prec_temp["stationMaxPrec"] >= 100) & (prec_temp["stationMaxPrec"] <= 200) & (prec_temp["stationMaxTemp"] >= 25) & (prec_temp["stationMaxTemp"] <= 30))


filtered_max.rdd.coalesce(1,shuffle=True).saveAsTextFile("BDA/output")
