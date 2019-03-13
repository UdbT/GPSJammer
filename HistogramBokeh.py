import os
import pandas as pd
from math import pi
import numpy as np
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, curdoc

from ParentBokeh import ParentBokeh
from GPSJammer import GPSJammer

class HistogramBokeh(ParentBokeh):
    def __init__(self, date):
        super().__init__(date)

        self.deltaResults = pd.DataFrame()
        self.gpsJammer = GPSJammer(self.date)
    
    def loadData(self):
        dataPath = os.path.join(os.getcwd(), "data", self.date)

        # Check if there is any delta files
        try:
            self.deltaResults = pd.read_csv(os.path.join(dataPath, "delta", "delta_"+self.date+".csv"))
        except:
            print("Dose not have delta yet.")
            return

    def prepareData(self):
        # Load data
        self.loadData()

        # Histogram source
        detected = self.deltaResults
        detected = detected.loc[detected['delta_dist'] / detected['delta_time'] > 80]
        detectedCount = detected.groupby('unit_id').size()
        ParentBokeh.source_vbar.data = dict(unit_id=detectedCount.index.values, frequency=detectedCount.values)

    def buildFigure(self):
        plot = figure(x_range=ParentBokeh.source_vbar.data['unit_id'], sizing_mode='scale_width',\
                        plot_height=200,\
                        tools="pan,wheel_zoom,box_zoom,reset,tap,save")
        plot.xaxis.major_label_orientation = pi / 3
        plot.yaxis.axis_label = "Frequency"
        glyph_render = plot.vbar(x='unit_id', top='frequency', width=1, line_color ="black", source=ParentBokeh.source_vbar)
        glyph_render.data_source.selected.on_change('indices', self.histogramCallback)

        return plot
    
    # ------------------------ Callback ------------------------
    def histogramCallback(self, attr, old, new):
        selectedUnitId = pd.DataFrame(ParentBokeh.source_vbar.data)['unit_id'].iloc[new[0]]
        print(selectedUnitId)
        carData = self.gpsJammer.carByIdToDataframe(selectedUnitId)
        carData['coor'] = carData[['lat', 'lon']].apply(ParentBokeh.latLonToMercator, axis=1)
        carData['lon'], carData['lat'] = zip(*carData.coor)
        carData['color'] = carData['time_stamp'].map(pd.Series(data=np.arange(len(carData)), index=carData['time_stamp'].values).to_dict())
        print(carData['color'])
        ParentBokeh.source_map.data = carData[['unit_id', 'lat', 'lon', 'time_stamp', 'speed', 'color']].to_dict('list')

# histogramBokeh = HistogramBokeh("2019-01-01")
# histogramBokeh.prepareData()
# curdoc().add_root(histogramBokeh.buildFigure())