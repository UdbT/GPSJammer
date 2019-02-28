import os
import numpy as np
import pandas as pd
from bokeh.io import curdoc
from bokeh.plotting import figure, show, output_file, gmap
from bokeh.models import DatetimeTickFormatter, ColumnDataSource, HoverTool, GMapOptions
from bokeh.tile_providers import CARTODBPOSITRON_RETINA
from pyproj import Proj, transform

from GPSJammer import GPSJammer

roadNum = 2
date = "2019-01-04"
gpsJammer = GPSJammer(roadNum=roadNum, date=date)

# ------------------------ Load data -------------------------
dataPath = os.path.join(os.getcwd(), date)
# carOnRoad = pd.read_csv(os.path.join(dataPath, "road", "road#"+str(roadNum)+".csv"))

deltaResults = pd.read_csv(os.path.join(dataPath, "delta", "delta_"+date+".csv"))
deltaResults[['delta_dist', 'delta_time']] = deltaResults.loc[:, ['delta_dist', 'delta_time']].astype(float)

# ======================== Bokeh Visualization ==================
def latLonToMercator(row):
    return transform(Proj(init='epsg:4326'), Proj(init='epsg:3857'), row[1], row[0])  # longitude first, latitude second.

# ------------------------ Prepare data -------------------------
# Delta scatter source
deltaResults['avg_v'] = deltaResults[['delta_dist', 'delta_time']].apply( lambda x: float(x[0])/float(x[1]), axis=1)
source_delta = ColumnDataSource(deltaResults.to_dict('list'))

# Control velocity source
avg_c = {}
avg_c['dist'] = np.arange(min(deltaResults['delta_dist']), max(deltaResults['delta_dist']), 0.1)
avg_c['60'] = np.arange(min(deltaResults['delta_dist']), max(deltaResults['delta_dist']), 0.1)/60
avg_c['80'] = np.arange(min(deltaResults['delta_dist']), max(deltaResults['delta_dist']), 0.1)/80
avg_c['100'] = np.arange(min(deltaResults['delta_dist']), max(deltaResults['delta_dist']), 0.1)/100
source_c = ColumnDataSource(avg_c) 

# Map source
source_map = ColumnDataSource(dict(lat=[], lon=[]))

# kmn source
'''kmn = pd.read_csv("km_n_new.csv", sep=",")
kmn = kmn.loc[kmn['route'] == roadNum]
kmn['coor_mercator'] = kmn[['km_latitude', 'km_longitude']].apply(latLonToMercator, axis=1)
kmn['lon'], kmn['lat'] = zip(*kmn.coor_mercator)
source_kmn = ColumnDataSource(kmn[['lat', 'lon']].to_dict('list'))'''

# ------------------------ Callback ------------------------
def pointCallback(attr, old, new):
    selectedUnitId = deltaResults['unit_id'].iloc[new[0]]
    carData = gpsJammer.carByIdToDataframe(selectedUnitId)
    newData = pd.DataFrame(columns=['coor'])
    newData['coor'] = carData[['lat', 'lon']].apply(latLonToMercator, axis=1)
    # newData['coor'] = carOnRoad.groupby(['unit_id']).get_group(selectedUnitId)[['lat', 'lon']].apply(latLonToMercator, axis=1)
    newData['lon'], newData['lat'] = zip(*newData.coor)
    source_map.data = newData[['lat', 'lon']].to_dict('list')

# ------------------------ Build plot ------------------------
plot = figure(sizing_mode='scale_width', tools="pan,wheel_zoom,box_zoom,reset,tap", plot_height=200)
plot.toolbar.active_scroll = "auto"
plot.yaxis.axis_label = "Time Delta (Hours)"
plot.xaxis.axis_label = "Distance Delta (km)"

glyph_render = plot.circle(x = 'delta_dist', y = 'delta_time', source=source_delta, output_backend="webgl")
glyph_render.data_source.selected.on_change('indices', pointCallback)
plot.add_tools(HoverTool(renderers= [glyph_render],
tooltips=[
    ( 'Unit ID', '@unit_id'),
    ( 'Dist Delta',   '@delta_dist km'),
    ( 'Time Delta',  '@delta_time{0,0.000} hr' ),
    ( 'Average V', '@avg_v{0,0.000} km/hr')
]
))

plot.line(x = 'dist', y = '60', source=source_c, color='green', line_width=2, legend='60 km/hr')
plot.line(x = 'dist', y = '80', source=source_c, color = 'orange', line_width=2, legend='80 km/hr')
plot.line(x = 'dist', y = '100', source=source_c, color = 'red', line_width=2, legend='100 km/hr')

curdoc().add_root(plot)

# ------------------------ Build map ------------------------
_map = figure(sizing_mode='scale_width', plot_height=200, x_range=(11209948.82, 11289948.82), y_range=(1663269.74, 1743269.74),
           x_axis_type="mercator", y_axis_type="mercator")
_map.add_tile(CARTODBPOSITRON_RETINA)

# _map.circle(x='lon', y='lat', size=5, alpha=0.5, source=source_kmn)
_map.circle(x='lon', y='lat', size=10, color='red', source=source_map)

curdoc().add_root(_map)
# output_file("delta.html")