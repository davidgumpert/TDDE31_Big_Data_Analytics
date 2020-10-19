from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Lab2ex3")
sqlContext = SQLContext(sc)

temp_file = sc.textFile("BDA/input/temperature-readings.csv")
temp_lines = temp_file.map(lambda line: line.split(";"))

tempReadingsRow = temp_lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1], day=p[1].split("-")[2], time=p[2], value=float(p[3]), quality=p[4]))

schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow)
schemaTempReadings.registerTempTable("tempReadingsTable")

min_max_val_day = schemaTempReadings.select("year", "month", "day", "station", "value").filter((schemaTempReadings["year"]>=1960) & (schemaTempReadings["year"]<=2014)).groupBy("year", "month", "day", "station").agg(F.max(schemaTempReadings["value"]).alias("maxVal"), F.min(schemaTempReadings["value"]).alias("minVal"))

monthly_avg = min_max_val_day.select("year", "month", "station", ((min_max_val_day["maxVal"] + min_max_val_day["minVal"])/2).alias("dailyAvg")).groupBy("year", "month", "station").agg(F.avg("dailyAvg").alias("monthlyAvg")).orderBy("monthlyAvg", ascending=False)

monthly_avg.rdd.coalesce(1,shuffle=True).saveAsTextFile("BDA/output")
