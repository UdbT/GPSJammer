import os
import numpy as np
import pandas as pd
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure, curdoc

from ParentBokeh import ParentBokeh

class ScatterBokeh(ParentBokeh):
    def __init__(self, date):
        super().__init__(date)

        self.deltaResults = pd.DataFrame()
    
    def loadData(self):
        dataPath = os.path.join(os.getcwd(), "data", self.date)

        # Check if there is any delta files
        try:
            self.deltaResults = pd.read_csv(os.path.join(dataPath, "delta", "delta_"+self.date+".csv"))
            self.deltaResults[['delta_dist', 'delta_time']] = self.deltaResults.loc[:, ['delta_dist', 'delta_time']].astype(float)
        except:
            print("Dose not have delta yet.")
            return

    def prepareData(self):        
        '''
        Prepare data for all figure and glyph
        '''
        # Load data 
        self.loadData()

        # Delta scatter source
        deltaResults = self.deltaResults
        deltaResults['avg_v'] = deltaResults[['delta_dist', 'delta_time']].apply( lambda x: float(x[0])/float(x[1]), axis=1)
        ParentBokeh.source_delta.data = deltaResults.to_dict('list')

        # Control velocity source
        avg_c = {}
        avg_c['dist'] = np.arange(min(deltaResults['delta_dist']), max(deltaResults['delta_dist']), 0.1)
        avg_c['60'] = np.arange(min(deltaResults['delta_dist']), max(deltaResults['delta_dist']), 0.1)/60
        avg_c['80'] = np.arange(min(deltaResults['delta_dist']), max(deltaResults['delta_dist']), 0.1)/80
        avg_c['100'] = np.arange(min(deltaResults['delta_dist']), max(deltaResults['delta_dist']), 0.1)/100
        ParentBokeh.source_c.data = avg_c

    def buildFigure(self):
        ''' 
        Return the complete scatter plot figure
        ** Put retured figure into curdoc().add_root({figure})
            in your main program
        '''
        plot = figure(sizing_mode='scale_width', tools="pan,wheel_zoom,box_zoom,reset,tap,save",\
                        plot_height=200, title=self.date, output_backend="webgl")
        plot.toolbar.active_scroll = "auto"
        plot.yaxis.axis_label = "Time Delta (Hours)"
        plot.xaxis.axis_label = "Distance Delta (km)"

        glyph_render = plot.circle(x = 'delta_dist', y = 'delta_time', source=ParentBokeh.source_delta)
        # glyph_render.data_source.selected.on_change('indices', self.pointCallback)
        plot.add_tools(HoverTool(renderers= [glyph_render],
        tooltips=[
            ( 'Unit ID', '@unit_id'),
            ( 'Dist Delta',   '@delta_dist km'),
            ( 'Time Delta',  '@delta_time{0,0.000} hr' ),
            ( 'Average V', '@avg_v{0,0.000} km/hr')
        ]
        ))

        plot.line(x = 'dist', y = '60', source=ParentBokeh.source_c, color='green', line_width=2, legend='60 km/hr')
        plot.line(x = 'dist', y = '80', source=ParentBokeh.source_c, color = 'orange', line_width=2, legend='80 km/hr')
        plot.line(x = 'dist', y = '100', source=ParentBokeh.source_c, color = 'red', line_width=2, legend='100 km/hr')

        return plot

# scatterBokeh = ScatterBokeh(2, "2019-01-01")
# scatterBokeh.prepareData()
# curdoc().add_root(scatterBokeh.buildFigure())