from bokeh.plotting import curdoc

from MapBokeh import MapBokeh
from ScatterBokeh import ScatterBokeh
from HistogramBokeh import HistogramBokeh


# scatterBokeh = ScatterBokeh("2019-01-01")
# scatterBokeh.prepareData()
# curdoc().add_root(scatterBokeh.buildFigure())

histogramBokeh = HistogramBokeh("2019-01-01")
histogramBokeh.prepareData()
curdoc().add_root(histogramBokeh.buildFigure())

mapBokeh = MapBokeh("2019-01-01", 7)
mapBokeh.prepareData()
curdoc().add_root(mapBokeh.buildFigure())
