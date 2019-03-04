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

def cutDecimal(num_float):
    return round(num_float - num_float % 0.01, 2)

class GPSJammer:
    def __init__(self, roadNum, date):
        kmn = pd.read_csv("km_n_new.csv", sep=",")
        kmn = kmn.loc[kmn['route'] == roadNum]
        kmn['order'] = kmn['km_label'].str.split("+")
        kmn['order'] = kmn['order'].str.get(0)
        kmn['order'] = kmn['order'].apply(int)
        kmn.sort_values(by='order', inplace=True)
        self.kmn_matrix = kmn.loc[:,['km_latitude', 'km_longitude']].values
        
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
        self.roadNum = roadNum
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
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile) # columns = [time_stamp, unit_id, lat, lon, speed, unit_type]
            yield next(datareader)  # yield the header row
            for row in datareader:
                if row[1] in self.unitDelta:
                    pointNew = (float(row[2]), float(row[3]))
                    pointOld = (float(self.unitDelta[row[1]][0]), float(self.unitDelta[row[1]][1]))
                    
                    distDelta = haversine(pointNew, pointOld)
                    timeDelta = abs(datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S") - self.unitDelta[row[1]][2])
                    
                    speedOld = self.unitDelta[3]
                    speedNew = row[4]

                    self.unitDelta[row[1]] = (row[2], row[3], datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"), speedNew)
                    if timeDelta.seconds > 60*60 and distDelta > 80:
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
                    self.unitDelta[row[1]] = (row[2], row[3], datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"))

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
        resultFile = open(os.path.join(self.dataPath, "delta", "delta_"+self.date+".csv"), 'w', newline='')
        csvWriter = csv.writer(resultFile)
        csvWriter.writerow(columns)
        count = 0
        for fileName in csvFiles:
            if not("road" in fileName) and not("delta" in fileName):
                print("--> " + fileName + " Reading...")
                for i, row in enumerate(self.getUnitDelta(os.path.join(self.dataPath, fileName))):
                    if i == 0:
                        continue
                    count += 1
                    if count%100 == 0:
                        print("--> ", count, end='\r')
                    csvWriter.writerow(row)
                print()
                print("--> " + "DONE!")

        resultFile.close()

    def getCarOnRoad(self, filename):
        with open(filename, "rt") as csvfile:
            datareader = csv.reader(csvfile)
            yield next(datareader)  # yield the header row
            for row in datareader:
                if self.checkRoad(row[2], row[3]):
                    yield row

    def carOnRoadToCsv(self):
        csvFiles = os.listdir(self.dataPath)
        try:
            os.makedirs(os.path.join(self.dataPath, "road"))
        except FileExistsError:
            pass
        columns = ['time_stamp', 'unit_id', 'lat', 'lon', 'speed', 'unit_type']
        resultFile = open(os.path.join(self.dataPath, "road", "road#" + str(self.roadNum) + ".csv"), 'w', newline='')
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
        try:
            os.makedirs(os.path.join(self.dataPath, "road"))
        except FileExistsError:
            pass
        columns = ['time_stamp', 'unit_id', 'lat', 'lon', 'speed', 'unit_type']
        dataset = []
        subDataset = pd.DataFrame(columns=columns)
        for fileName in csvFiles:
            if not("road" in fileName) and not("delta" in fileName):
                print(fileName + " Reading...")
                for i, row in enumerate(self.getCarbyId(os.path.join(self.dataPath, fileName), unit_id)):
                    if i == 0:
                        continue
                    subDataset = subDataset.append(pd.Series(row, index=columns ), ignore_index=True)
                dataset.append(subDataset)
                subDataset = pd.DataFrame(columns=columns)
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
        for date in os.listdir(os.path.join(os.getcwd(), "data")):
            print(date)
            gpsJammer = GPSJammer(roadNum=roadNum, date=date)
            # gpsJammer.carOnRoadToCsv()
            gpsJammer.allDeltaToCsv(force=force)
    else:
        for date in dateList:
            print(date)
            gpsJammer = GPSJammer(roadNum=roadNum, date=date)
            # gpsJammer.carOnRoadToCsv()
            gpsJammer.allDeltaToCsv(force=force)

    print(time.time() - t)