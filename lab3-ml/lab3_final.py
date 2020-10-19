## -------- IMPORTING NECESSARY PACKAGES --------- ## 
from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext(appName="lab_kernel")


## -------- GENERAL FUNCTIONS --------- ## 

#Function to convert string date to the appropriate datetime variable type
def toDateTime(strDate):
    splitDate = strDate.split("-")
    convertedDate = datetime(int(splitDate[0]), int(splitDate[1]), int(splitDate[2]))
    return convertedDate

#Function to calculate the great circle distance between two points on the earth (specified in decimal degrees)
def haversine(lon1, lat1, lon2, lat2):
    # converting decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


## -------- KERNEL FUNCTIONS --------- ## 

# --- The distance Kernel Function --- #
def distanceKernel(a, b, new_a, new_b, h_distance):
     return (exp(-(haversine(a, b, new_a, new_b)/h_distance)**2))
    

# --- The date Kernel Function --- #
def dateDiffCalc(givenDate, newDate):
    givenDate = toDateTime(givenDate)
    newDate = toDateTime(newDate)
    diff = givenDate - newDate
    diffDays = diff.days % 365
    if(diffDays > 183):
        diffDays = (183 - diffDays)%183
    return diffDays

def dateKernel(givenDate, newDate, h_date):
    return (exp(-(dateDiffCalc(givenDate, newDate)/h_date)**2))


# --- The time Kernel Function --- #
def timeDiffCalc(timeStamp, newTime):
    diffTime = abs(timeStamp - newTime)
    if(diffTime > 12):
        diffTime = (12 - diffTime)%12
    return diffTime

def timeKernel(timeStamp, newTime, h_time):
    return (exp(-(timeDiffCalc(timeStamp, newTime)/h_time)**2))


## ------- VARIABLE DEFINITION -------- ##

h_distance = 100 # Up to you
h_date = 5 # Up to you
h_time = 2.5 # Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2013-01-24" # Up to you
givenDate = toDateTime(date)
timeStamps = [24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4]


## --------- LOADING AND SAMPLING DATA ------------ ##

#Loading the data (comment out top two lines and comment third and forth to run on super computer)
stations = sc.textFile("BDA/input/stations.csv")
temp = sc.textFile("BDA/input/temperature-readings.csv")
#stations = sc.textFile("/Users/davidgumpert/Desktop/JupiterNotebook/stations.csv")
#temp = sc.textFile("/Users/davidgumpert/Desktop/JupiterNotebook/temperature-readings-sample.csv")

#Splitting the data and getting relevant data for the assignment
stationLines = stations.map(lambda line: line.split(";"))
tempLines = temp.map(lambda line: line.split(";"))

#Making the station data into a broadcast variable, since it is smaller in size
stations_data = stationLines.map(lambda x: (str(x[0]), (float(x[3]), float(x[4]))))
bc = sc.broadcast(stations_data.collectAsMap())

#MCombining all necessary data from the temperature file with the corresponding coordinates for each station.
#Then filtering out the dates that we do not take into account in our prediction, the dates after our "givenDate"
data = tempLines.map(lambda x: ((x[0], x[1], int(x[2][0:2]), bc.value[str(x[0])][0], bc.value[str(x[0])][1]), float(x[3])))
data = data.filter(lambda x: (toDateTime(x[0][1]) < givenDate))

#Drawing a random sample of our data to test our algorithm on. Comment out if you wanna use full data set.
#Then casching this data since it will be reused numerous times and we don't want to read it from disc time after time.
data = data.sample(False, 1)
data = data.map(lambda x: (x[0],(dateKernel(date, x[0][1], h_date), distanceKernel(a, b, x[0][3], x[0][4], h_distance), x[1])))
data.cache()


## --------- PREDICTION LOOP ------------ ##

for i in timeStamps:
    #((Station, Date, Time, lat, lon), (date kernel, distance kernel, temp))
    
    allKern = data.map(lambda x: (x[0], (x[1][0], x[1][1], timeKernel(i, x[0][2], h_time), x[1][2])))
    #((Station, Date, Time, lat, lon), (date kernel, distance kernel, time kernel(i), temp))

    sumProdKern = allKern.map(lambda x: (x[0],(x[1][0] + x[1][1] + x[1][2], x[1][0] * x[1][1] * x[1][2], x[1][3])))
    #(x[0], (sum kernel, product kernel, temp))
    
    sumProdKern = sumProdKern.map(lambda x: (i, (x[1][0], x[1][0] * x[1][2], x[1][1], x[1][1] * x[1][2])))
    #(i, (sum kernel, sum kernel*temp, product kernel, productkernel*temp))
    
    sumProdKern = sumProdKern.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))
    #(i, (summed sum-kernel, summed sum-kernel*temp, summed product-kernel, summed product-kernel*temp))
    
    finalPred = sumProdKern.map(lambda x: (x[0], x[1][1]/x[1][0], x[1][3]/x[1][2]))
    #(i, sum prediction, product prediction)   
    
    if i == 24:
        result = finalPred
    else:
        result = result.union(finalPred)


## --------- SAVING RESULTS TO FILE ------------ ##

result.coalesce(1, shuffle = False).saveAsTextFile("BDA/output")






