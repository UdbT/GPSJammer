import csv
import numpy as np
import pandas as pd
import os
import folium
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.cm as cm
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
import time
from haversine import haversine
from joblib import Parallel, delayed
from multiprocessing import Pool, cpu_count

def cutDecimal(num_float):
    return round(num_float - num_float % 0.01, 2)

def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)

def deltaDist(x):
    return haversine((float(x["lat"]), float(x["lon"])),(float(x["lat2"]), float(x["lon2"])))

def deltaTime(x):
    return abs(
            datetime.strptime(x["time_stamp"], "%Y-%m-%d %H:%M:%S") - datetime.strptime(x["time_stamp2"], "%Y-%m-%d %H:%M:%S")
            ).seconds/3600

def calDelta(df):
    df.drop_duplicates(subset=["unit_id", "lat", "lon", "speed", "unit_type"], keep="last", inplace=True)
    if len(df) > 1:
        shifted = df.shift(1).rename(index=int,\
                columns={"time_stamp":"time_stamp2", "unit_id":"unit_id2", "lat":"lat2", "lon":"lon2", "speed":"speed2"})

        concated = pd.concat([df, shifted], axis=1, sort=False).iloc[1:,:]
        concated.drop(["unit_id2", "unit_type"], axis=1, inplace=True)

        concated["delta_dist"] = concated[["lat", "lon", "lat2", "lon2"]]\
            .apply(lambda x: deltaDist(x), axis=1)

        concated["delta_time"] = concated[["time_stamp", "time_stamp2"]]\
            .apply(lambda x: deltaTime(x) ,axis=1)
        concated = concated.loc[concated["delta_time"] <= 0.083] # Less than or equal to 5 minutes
        return concated

class GPSJammer:
    def __init__(self, date):
        self.kmn_all = pd.read_csv("km_n_new.csv", sep=",")
        self.kmn = None
        
        '''self.kmn_matrix = {}
        kmn_coordinate = kmn.loc[:,['km_latitude', 'km_longitude']].apply(cutDecimal)
        for index, row in kmn_coordinate.iterrows():
            self.kmn_matrix[str(row['km_latitude']) + '_' + str(row['km_longitude'])] = 0
            
            self.kmn_matrix[str(round(row['km_latitude'] + 0.01, 2)) + '_' + str(round(row['km_longitude'] + 0.01, 2))] = 0
            self.kmn_matrix[str(round(row['km_latitude'] + 0.01, 2)) + '_' + str(row['km_longitude'])] = 0
            
            self.kmn_matrix[str(round(row['km_latitude'] + 0.01, 2)) + '_' + str(round(row['km_longitude'] - 0.01, 2))] = 0
            self.kmn_matrix[str(row['km_latitude']) + '_' + str(round(row['km_longitude'] - 0.01, 2))] = 0
            
            self.kmn_matrix[str(round(row['km_latitude'] - 0.01, 2)) + '_' + str(round(row['km_longitude'] - 0.01, 2))] = 0
            self.kmn_matrix[str(round(row['km_latitude'] - 0.01, 2)) + '_' + str(row['km_longitude'])] = 0
            
            self.kmn_matrix[str(round(row['km_latitude'] - 0.01, 2)) + '_' + str(round(row['km_longitude'] + 0.01, 2))] = 0
            self.kmn_matrix[str(row['km_latitude']) + '_' + str(round(row['km_longitude'] + 0.01, 2))] = 0
        print(self.kmn_matrix)'''

        self.date = date
        self.dataPath = os.path.join(os.getcwd(), "data", date)
        self.unitDelta = {}

    def checkRoad(self, lat, lon):
        '''lat = cutDecimal(float(lat))
        lon = cutDecimal(float(lon))
        coordinate = str(lat) + '_' + str(lon)
        if coordinate in self.kmn_matrix:
            return True
        else:
            return False'''

        for ind in range(len(self.kmn_matrix) - 1):
            lats = [self.kmn_matrix[ind][0], self.kmn_matrix[ind+1][0]]
            longs = [self.kmn_matrix[ind][1], self.kmn_matrix[ind+1][1]]
            if float(lat) <= max(lats) and float(lat) >= min(lats) and\
                float(lon) <= max(longs) and float(lon) >= min(longs):
                return True
        return False
                  
    def getUnitId(self, filename):
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile)
            yield next(datareader)  # yield the header row
            for row in datareader:
                yield row[1]

    def toDatetime(self, row):
        timeStamp = row['time_stamp']
        return datetime.strptime(timeStamp, "%Y-%m-%d %H:%M:%S")

    def getUnitDelta(self, filename):
        dataset = pd.read_csv(filename)
        groups = dataset.groupby("unit_id")
        result = applyParallel(groups, calDelta)
        return result

    def allDeltaToCsv(self, force):
        columns = ['unit_id', 'time_start', 'time_end', 'delta_time', 'lat_start', 'lon_start', 'lat_end', 'lon_end', 'delta_dist', 'speed_old', 'speed_new']
        try:
            os.makedirs(os.path.join(self.dataPath, "delta"))
        except FileExistsError:
            if not force:
                print("(Delta is already created.)")
                return
            else:
                print("Forced to calculate delta.")
        csvFiles = os.listdir(self.dataPath)
        dataset = []
        for fileName in csvFiles:
            if not("road" in fileName) and not("delta" in fileName):
                print("--> " + fileName + " Reading...")
                dataset.append(self.getUnitDelta(os.path.join(self.dataPath, fileName)))
                print("--> " + "DONE!")
        dataset = pd.concat(dataset)
        dataset.to_csv(os.path.join(self.dataPath, "delta", "delta_"+self.date+".csv"))

    def getCarOnRoad(self, filename):
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile)
            yield next(datareader)  # yield the header row
            for row in datareader:
                if self.checkRoad(row[2], row[3]):
                    yield row

    def carOnRoadToCsv(self, roadNum):
        csvFiles = os.listdir(self.dataPath)
        try:
            os.makedirs(os.path.join(self.dataPath, "road"))
        except FileExistsError:
            pass
        columns = ['time_stamp', 'unit_id', 'lat', 'lon', 'speed', 'unit_type']
        resultFile = open(os.path.join(self.dataPath, "road", "road#" + str(roadNum) + ".csv"), 'w', newline='')
        csvWriter = csv.writer(resultFile)
        csvWriter.writerow(columns)
        for fileName in csvFiles:
            if not("road" in fileName) and not("delta" in fileName):
                print(fileName + " Reading...")
                for i, row in enumerate(self.getCarOnRoad(os.path.join(self.dataPath, fileName))):
                    if i == 0:
                        continue
                    csvWriter.writerow(row)
                print("DONE!")
            
        resultFile.close()

    def getCarbyId(self, filename, unit_id):
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile)
            yield next(datareader)  # yield the header row
            for row in datareader:
                if row[1] == unit_id:
                    yield row

    def carByIdToDataframe(self, unit_id):
        csvFiles = os.listdir(self.dataPath)
        dataset = []
        for fileName in csvFiles:
            if not("road" in fileName) and not("delta" in fileName):
                print(fileName + " Reading...")
                subDataset = pd.read_csv(os.path.join(self.dataPath, fileName))
                subDataset = subDataset.loc[subDataset["unit_id"] == unit_id]
                dataset.append(subDataset)
                print("DONE!")
        return pd.concat(dataset)

if __name__ == "__main__":
    import time
    import sys, getopt

    t = time.time()
    roadNum = 2
    dateList = sys.argv[1:]
    force = False
    if "-f" in sys.argv[1:]:
        dateList = sys.argv[2:]
        force = True
    else:
        dateList = sys.argv[1:]

    print(dateList)
    if len(dateList) == 0:
        # If no specific date then use every date
        for date in os.listdir(os.path.join(os.getcwd(), "data")):
            print(date)
            gpsJammer = GPSJammer(date=date)
            # gpsJammer.carOnRoadToCsv()
            gpsJammer.allDeltaToCsv(force=force)
    else:
        # Use only sepcific date
        for date in dateList:
            print(date)
            gpsJammer = GPSJammer(date=date)
            # gpsJammer.carOnRoadToCsv()
            gpsJammer.allDeltaToCsv(force=force)

    print(time.time() - t)