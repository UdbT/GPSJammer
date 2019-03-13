import pandas as pd
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.tile_providers import CARTODBPOSITRON_RETINA
from bokeh.plotting import figure, curdoc
from bokeh.transform import linear_cmap

from ParentBokeh import ParentBokeh

class MapBokeh(ParentBokeh):
    def __init__(self, date, roadNum):
        super().__init__(date)
        self.roadNum = roadNum
        self.kmnData = pd.DataFrame()

    def loadData(self):
        self.kmnData = pd.read_csv("km_n_new.csv", sep=",")
    
    def prepareData(self):
        '''
        Prepare data for all figure and glyph
        '''

        # Load data
        self.loadData()

        # Map source
        ParentBokeh.source_map.data = dict(unit_id=[], lat=[], lon=[], time_stamp=[], speed=[], color=[])

        # kmn source
        """kmn = self.kmnData
        kmn = kmn.loc[kmn['route'] == self.roadNum]
        kmn['coor_mercator'] = kmn[['km_latitude', 'km_longitude']].apply(self.latLonToMercator, axis=1)
        kmn['lon'], kmn['lat'] = zip(*kmn.coor_mercator)
        ParentBokeh.source_kmn = ColumnDataSource(kmn[['lat', 'lon']].to_dict('list'))"""

    def buildFigure(self):
        ''' 
        Return map figure
        ** Put retured figure into curdoc().add_root({figure})
            in your main program
        '''
        _map = figure(sizing_mode='scale_width', plot_height=200, x_range=(11209948.82, 11289948.82), y_range=(1663269.74, 1743269.74),
                x_axis_type="mercator", y_axis_type="mercator")
        _map.add_tile(CARTODBPOSITRON_RETINA)
        # _map.circle(x='lon', y='lat', size=5, alpha=0.5, source=ParentBokeh.source_kmn)
        glyph_render = _map.circle(x='lon', y='lat', size=10, color=linear_cmap('color', "Viridis256", 0, 255), source=ParentBokeh.source_map)
        _map.add_tools(HoverTool(renderers= [glyph_render],
                tooltips=[
                    ( 'Time', '@time_stamp{%F %T}'),
                    ( 'Speed',   '@speed km/hr'),
                    ( 'Unit ID', '@unit_id')
                ],
                formatters={'time_stamp': 'datetime'}
                ))

        return _map

# mapBokeh = MapBokeh(2, "2019-01-01")
# mapBokeh.prepareData()
# curdoc().add_root(mapBokeh.buildFigure())
