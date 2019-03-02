import os
import csv
import numpy as np
import pandas as pd
from math import pi
from bokeh.io import curdoc
from bokeh.plotting import figure, show, output_file
from bokeh.models import ColumnDataSource, HoverTool

from GPSJammer import GPSJammer

roadNum = 2
date = "2019-01-04"
dataPath = os.path.join(os.getcwd(), date)
columns = ['unit_id', 'time_start', 'time_end', 'delta_time', 'lat_start', 'lon_start', 'lat_end', 'lon_end', 'delta_dist']
detected = pd.DataFrame(columns=columns)
count = 0
# ------------------------ Load data -------------------------
with open(os.path.join(dataPath, "delta", "delta_"+date+".csv"), "rt") as csvfile:
    datareader = csv.reader(csvfile)
    next(datareader)  # yield the header row
    for row in datareader:
        if float(row[8])/float(row[3]) > 100.0:
            count += 1
            print(count, end='\r')
            detected = detected.append(pd.Series(row, index=columns ), ignore_index=True)

detectedCount = detected.groupby('unit_id').size()
detectedCount = detectedCount[detectedCount > 3]
source = ColumnDataSource(dict(unit_id=detectedCount.index.values, frequency=detectedCount.values))

plot = figure(x_range=detectedCount.index.values, sizing_mode='scale_width', plot_height=200)
plot.xaxis.major_label_orientation = pi / 3
plot.yaxis.axis_label = "Frequency"
plot.vbar(x='unit_id', top='frequency', width=1, source=source)
show(plot)
