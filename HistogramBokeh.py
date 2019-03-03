import os
import pandas as pd
from math import pi
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, curdoc

from ParentBokeh import ParentBokeh

class HistogramBokeh(ParentBokeh):
    def __init__(self, date):
        super().__init__(date)

        self.deltaResults = pd.DataFrame()
    
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
        detected = detected.loc[detected['delta_dist'] / detected['delta_time'] > 100]
        detectedCount = detected.groupby('unit_id').size()
        ParentBokeh.source_vbar.data = dict(unit_id=detectedCount.index.values, frequency=detectedCount.values)

    def buildFigure(self):
        plot = figure(x_range=ParentBokeh.source_vbar.data['unit_id'], sizing_mode='scale_width', plot_height=200)
        plot.xaxis.major_label_orientation = pi / 3
        plot.yaxis.axis_label = "Frequency"
        plot.vbar(x='unit_id', top='frequency', width=1, source=ParentBokeh.source_vbar)

        return plot

# histogramBokeh = HistogramBokeh("2019-01-01")
# histogramBokeh.prepareData()
# curdoc().add_root(histogramBokeh.buildFigure())