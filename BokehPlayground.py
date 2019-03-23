from bokeh.plotting import curdoc

from MapBokeh import MapBokeh
from ScatterBokeh import ScatterBokeh
from HistogramBokeh import HistogramBokeh

date = "2019-01-01"
scatterBokeh = ScatterBokeh(date)
scatterBokeh.prepareData()
curdoc().add_root(scatterBokeh.buildFigure())

histogramBokeh = HistogramBokeh(date)
histogramBokeh.prepareData()
curdoc().add_root(histogramBokeh.buildFigure())

mapBokeh = MapBokeh(date, 7)
mapBokeh.prepareData()
curdoc().add_root(mapBokeh.buildFigure())
