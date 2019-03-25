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
        self.deltaPath = os.path.join(self.dataPath, "delta")
        self.cachePath = os.path.join(self.dataPath, "cache")
        self.unitDelta = {}
        self.rawColumns = ['time_stamp', 'unit_id', 'lat', 'lon', 'speed', 'unit_type']
        self.deltaColumns = ['unit_id', 'time_start', 'time_end', 'delta_time', 'lat_start', 'lon_start', 'lat_end', 'lon_end', 'delta_dist', 'speed_old', 'speed_new']

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
        
    def toDatetime(self, row):
        timeStamp = row['time_stamp']
        return datetime.strptime(timeStamp, "%Y-%m-%d %H:%M:%S")

#-------------------------------Calculate delta--------------------------------
    def getUnitDelta(self, filename):
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile) # columns = [time_stamp, unit_id, lat, lon, speed, unit_type]
            yield next(datareader)  # yield the header row
            for row in datareader:
                if row[1] in self.unitDelta:
                    pointNew = (float(row[2]), float(row[3]))
                    pointOld = (float(self.unitDelta[row[1]][0]), float(self.unitDelta[row[1]][1]))
                    
                    distDelta = haversine(pointNew, pointOld)
                    timeDelta = abs(datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S") - self.unitDelta[row[1]][2])
                    
                    speedOld = self.unitDelta[row[1]][3]
                    speedNew = row[4]

                    self.unitDelta[row[1]] = (row[2], row[3], datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"), speedNew)
                    # if timeDelta.seconds >= 60*60 and distDelta >= 80:
                    yield [
                            row[1],\
                            datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"),\
                            self.unitDelta[row[1]][2],\
                            str(timeDelta.seconds/3600),\
                            pointOld[0],\
                            pointOld[1],\
                            pointNew[0],\
                            pointNew[1],\
                            distDelta,\
                            speedOld,\
                            speedNew 
                        ]   
                else:
                    self.unitDelta[row[1]] = (row[2], row[3], datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"), row[4])

    def allDeltaToCsv(self, force):
        try:
            os.makedirs(self.deltaPath)
        except FileExistsError:
            if not force:
                print("(Delta is already created.)")
                return
            else:
                print("Forced to calculate delta.")
        csvFiles = os.listdir(self.dataPath)
        resultFile = open(os.path.join(self.deltaPath, self.date+".csv"), 'w', newline='')
        csvWriter = csv.writer(resultFile)
        csvWriter.writerow(self.deltaColumns)
        count = 0
        for fileName in csvFiles:
            if not("result" in fileName) and not("delta" in fileName):
                print("--> " + fileName + " Reading...")
                for i, row in enumerate(self.getUnitDelta(os.path.join(self.dataPath, fileName))):
                    if i == 0:
                        continue
                    count += 1
                    if count%100000 == 0:
                        print("-->", count, end='\r')
                    csvWriter.writerow(row)
                print()
                print("--> " + "DONE!")

        resultFile.close()

#-------------------------------Get delta by unit ID--------------------------------
    def deltaByIdToCsv(self, unit_id=None):
        deltaFile = os.path.join(self.deltaPath, self.date+".csv")
        
        if unit_id+".csv" in os.listdir(self.deltaPath): 
            print("This unit ID already has delta info.")
            return

        resultFile = open(os.path.join(self.deltaPath, unit_id+".csv"), 'w', newline='')
        csvWriter = csv.writer(resultFile)
        csvWriter.writerow(self.deltaColumns)
        count = 0
        for i, row in enumerate(self.getDeltaById(deltaFile, unit_id)):
            if i == 0:
                continue
            count += 1
            if count%5 == 0:
                print("-->", count, end='\r')
            csvWriter.writerow(row)

        resultFile.close()

    def getDeltaById(self, filename, unit_id):
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile)
            yield next(datareader)  # yield the header row
            for row in datareader:
                if row[0] == unit_id:
                    yield row

#-------------------------------Filter car by road--------------------------------
    def carOnRoadToCsv(self, roadNum):
        csvFiles = os.listdir(self.dataPath)
        try:
            os.makedirs(os.path.join(self.dataPath, "road"))
        except FileExistsError:
            pass
        resultFile = open(os.path.join(self.dataPath, "road", "road#" + str(roadNum) + ".csv"), 'w', newline='')
        csvWriter = csv.writer(resultFile)
        csvWriter.writerow(self.rawColumns)
        for fileName in csvFiles:
            if not("road" in fileName) and not("delta" in fileName):
                print(fileName + " Reading...")
                for i, row in enumerate(self.getCarOnRoad(os.path.join(self.dataPath, fileName))):
                    if i == 0:
                        continue
                    csvWriter.writerow(row)
                print("DONE!")
            
        resultFile.close()

    def getCarOnRoad(self, filename):
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile)
            yield next(datareader)  # yield the header row
            for row in datareader:
                if self.checkRoad(row[2], row[3]):
                    yield row

#-------------------------------Get car by unit ID--------------------------------
    def carByIdToDataframe(self, unit_id):
        try:
            os.makedirs(self.cachePath)
        except FileExistsError:
            pass

        result = None
        if unit_id+".csv" in os.listdir(self.cachePath):
            result = pd.read_csv(os.path.join(self.cachePath, unit_id+".csv"))
        else:
            csvFiles = os.listdir(self.dataPath)
            dataset = []
            for fileName in csvFiles:
                if not("result" in fileName) and not("delta" in fileName) and not("cache" in fileName):
                    print(fileName + " Reading...")
                    subDataset = pd.read_csv(os.path.join(self.dataPath, fileName))
                    subDataset = subDataset.loc[subDataset["unit_id"] == unit_id]
                    dataset.append(subDataset)
                    print("DONE!")
            result = pd.concat(dataset)

        result["time_stamp"] = pd.to_datetime(result["time_stamp"])
        result.sort_values(by=["time_stamp"], inplace=True)
        result.reset_index(inplace=True)
        result.drop("index", axis=1, inplace=True)
        print(result)
        result[self.rawColumns].to_csv(os.path.join(self.cachePath, unit_id+".csv"), sep=',')
        return result

#-------------------------------Filter function--------------------------------
    def filterDataToCsv(self, force):
        if "result.csv" in os.listdir(self.deltaPath):
            if not force:
                print("(Delta is already created.)")
                return
            else:
                print("Forced to calculate delta.")

        deltaFile = os.path.join(self.deltaPath, self.date+".csv")
        resultFile = open(os.path.join(self.dataPath, "delta", "result.csv"), 'w', newline='')
        csvWriter = csv.writer(resultFile)
        csvWriter.writerow(self.deltaColumns)
        count = 0
        for i, row in enumerate(self.getData(deltaFile)):
            if i == 0:
                continue
            count += 1
            if count%5 == 0:
                print("-->", count, end='\r')
            csvWriter.writerow(row)

        resultFile.close()

    def getData(self, filename):
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile)
            yield next(datareader)  # yield the header row
            for row in datareader:
                if float(row[8]) >= 100 and float(row[8]) < 400 and float(row[3]) > 0: # row[3] = delta_time, row[8] = delta_dist
                    yield row
if __name__ == "__main__":
    import time
    import sys

    t = time.time()
    # for date in os.listdir(os.path.join(os.getcwd(), "data")):
    #     print(date)
    #     gpsJammer = GPSJammer(date=date)
    #     gpsJammer.carByIdToDataframe("005000600000863835024605490")
        # gpsJammer.carOnRoadToCsv()
        # gpsJammer.allDeltaToCsv(force=force)
        # gpsJammer.filterDataToCsv(force=force)
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
            # gpsJammer.allDeltaToCsv(force=force)
            gpsJammer.filterDataToCsv(force=force)
    else:
        # Use only sepcific date
        for date in dateList:
            print(date)
            gpsJammer = GPSJammer(date=date)
            # gpsJammer.carOnRoadToCsv()
            # gpsJammer.allDeltaToCsv(force=force)
            gpsJammer.filterDataToCsv(force=force)

    print(time.time() - t)