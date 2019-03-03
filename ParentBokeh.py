import os
import numpy as np
import pandas as pd
from abc import abstractmethod, ABCMeta
from bokeh.io import curdoc
from bokeh.plotting import figure, show, output_file
from bokeh.models import DatetimeTickFormatter, ColumnDataSource, HoverTool
from pyproj import Proj, transform

from GPSJammer import GPSJammer

# ======================== Bokeh Visualization ==================
class ParentBokeh(object):
    __metaclass__  = ABCMeta
    
    # ScatterBokeh sources
    source_delta = ColumnDataSource()
    source_c = ColumnDataSource()

    # MapBokeh sources
    source_map = ColumnDataSource()
    source_kmn = ColumnDataSource()

    # Histogram source
    source_vbar = ColumnDataSource()
    
    def __init__(self, date):
        self.date = date # "2019-01-04"

    @abstractmethod
    def loadData(self):
        pass

    @abstractmethod
    def prepareData(self):
        pass

    @abstractmethod
    def buildFigure(self):
        pass

    def latLonToMercator(self, row):
        return transform(Proj(init='epsg:4326'), Proj(init='epsg:3857'), row[1], row[0])  # longitude first, latitude second.

    # ------------------------ Callback ------------------------
    """def pointCallback(self, attr, old, new):
        '''
        Callback function for Scatter plot
        '''
        selectedUnitId = self.deltaResults['unit_id'].iloc[new[0]]
        carData = self.gpsJammer.carByIdToDataframe(selectedUnitId)
        newData = pd.DataFrame(columns=['coor'])
        newData['coor'] = carData[['lat', 'lon']].apply(latLonToMercator, axis=1)
        # newData['coor'] = carOnRoad.groupby(['unit_id']).get_group(selectedUnitId)[['lat', 'lon']].apply(latLonToMercator, axis=1)
        newData['lon'], newData['lat'] = zip(*newData.coor)
        self.source_map.data = newData[['lat', 'lon']].to_dict('list')"""

# roadNum = 2
# for date in os.listdir(os.path.join(os.getcwd(), "data")):
#     try:
#         scatterMap = ScatterMap(roadNum=roadNum, date=date)
#         scatterMap.prepareData()
#         curdoc().add_root(scatterMap.buildScatter())

#     except Exception as e:
#         print(date+ '\n' +str(e))
#         continue

