from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure

source = ColumnDataSource(data=dict(x=[1,2,3], y=[4,6,5]))

p = figure(title="select a circle", tools="tap")
p.circle('x', 'y', size=25, source=source)

def callback(attr, old, new):
    global source
    # This uses syntax for Bokeh >= 0.12.15
    print("Indices of selected circles: ", source.selected.indices)

source.selected.on_change('indices', callback)

curdoc().add_root(p)