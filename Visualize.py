import folium
import pandas as pd
import matplotlib as mpl
import matplotlib.cm as cm

from bokeh.plotting import figure, show
from bokeh.models import DatetimeTickFormatter, ColumnDataSource, HoverTool
class Map():
    def __init__(self):
        self.map = folium.Map(location=[13.7563, 100.5018],
        #                  tiles='Stamen Toner',
                          zoom_start = 10)
    
    def convert_to_hex(self, rgba_color) :
        red = int(rgba_color[0]*255)
        green = int(rgba_color[1]*255)
        blue = int(rgba_color[2]*255)
        return '#{r:02x}{g:02x}{b:02x}'.format(r=red,g=green,b=blue)
    
    def mapColor(self, speed):
        norm = mpl.colors.Normalize(vmin=0, vmax=100)
        cmap = cm.hot_r
        m = cm.ScalarMappable(norm=norm, cmap=cmap)
        return self.convert_to_hex(m.to_rgba(speed))
    
    def addCircleKmn(self, road_num):
        kmn = pd.read_csv("km_n_new.csv", sep=",")
        roadNum = road_num
        kmn = kmn.loc[kmn['route'] == roadNum]
        for location, name in zip(kmn.iloc[:,1:3].astype('float').values, kmn.iloc[:,4].astype('str')):
            folium.Circle(
                    location=location,
                    popup='{}\n{}'.format(name, str(location)),
                    radius=100,
                    fill=True,
                ).add_to(self.map)
            
    def addCircleCars(self, car_df):
        for location, speed in zip(car_df.iloc[:,2:4].astype('float').values, car_df.iloc[:,4].astype('float').values):
            folium.Circle(
                location=location,
                popup='{}\n{}'.format(speed, str(location)),
                radius=50,
                color=self.mapColor(speed),
                fill=True,
            ).add_to(self.map) 
            
    def plotMap(self):
        self.map.save('index.html')

if __name__ == "__main__":
    from bokeh.io import output_file, show
    from bokeh.models import ColumnDataSource, GMapOptions
    from bokeh.plotting import gmap

    output_file("gmap.html")

    map_options = GMapOptions(lat=30.2861, lng=-97.7394, map_type="roadmap", zoom=11)

    # For GMaps to function, Google requires you obtain and enable an API key:
    #
    #     https://developers.google.com/maps/documentation/javascript/get-api-key
    #
    # Replace the value below with your personal API key:
    p = gmap("AIzaSyAS7BwB8OLUGwDNkjBVhNIbYUj0nTfs0gU", map_options, title="Austin")

    source = ColumnDataSource(
        data=dict(lat=[ 30.29,  30.20,  30.29],
                lon=[-97.70, -97.74, -97.78])
    )

    p.circle(x="lon", y="lat", size=15, fill_color="blue", fill_alpha=0.8, source=source)

    show(p)
    
#class Plot():
#    def __init__(self):
#        self.source = ColumnDataSource()
#        self.plot = figure()
#        xformatter = DatetimeTickFormatter(hours=["%H:%M:%S"])
#        self.plot.xaxis.formatter = xformatter
#plt.add_tools(HoverTool(
#    tooltips=[
#        ( 'date',   '@time_stamp{%F %T}'            ),
#        ( 'speed',  '@{speed}' )
#    ],
#
#    formatters={
#        'time_stamp' : 'datetime', # use 'datetime' formatter for 'date' field
#    },
#
#    # display a tooltip whenever the cursor is vertically in line with a glyph
#    mode='vline'
#))
#
#plt.line(x = 'time_stamp', y = 'speed', source=source)
#show(plt)
        