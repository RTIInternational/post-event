'''
build linked scatter plot-map-histograms dashboard
'''
import panel as pn
import holoviews as hv
hv.extension('bokeh', logo=False)

from bokeh.models import HoverTool

from . import legends
from . import common
from . import class_scatter
                 
def build(
    dash_class: class_scatter.ScatterExplorer, 
) -> pn.layout:   
    
    dash_class.initialize()
    
    scatter_variable_select = pn.Param(
        dash_class,
        widgets={
            "scatter_variable" : {"type" : pn.widgets.Select,  
            "options" : dash_class.scatter_variable_options,
            "name" : "Scatter Variable",
            "value" : dash_class.scatter_variable,
            "width" : 180, 
            "margin" : (20,10,10,10)}
        },
        parameters=["scatter_variable"],
        show_name=True,
    )
    scatter_widget = scatter_variable_select.widget('scatter_variable')
    
    color_variable_select = pn.Param(
        dash_class,
        widgets={
            "color_variable" : {"type" : pn.widgets.Select,  
                              "options" : dash_class.color_variable_options,
                              "name" : "Color Variable",
                              "value" : dash_class.color_variable,
                              "width" : 180, 
                              "margin" : (10,10,10,10)}
        },
        parameters=["color_variable"],
        show_name=True,
    )
    color_widget = color_variable_select.widget('color_variable')
    
    scatter_height=450
    scatter_width=450
    map_height = scatter_height   
    map_width = scatter_width + 50
    
    hist_opts = dict(
        height = 200,
        width = 220,
        margin=(0,0,0,0)
    )
    states = common.get_states(dash_class.geo).opts(
        width=map_width, 
        height=map_height
    )          
    basemap = common.get_basemap(dash_class.geo).opts(
        width=map_width, 
        height=map_height
    ) 
    scatter_hover = HoverTool(tooltips=[('Gage ID', '@primary_location_id'),
                                        ('Name', '@name'),
                                        ('Ref Time', '@reference_time{%m-%d %Hz}'),
                                        ('Stream Order','@stream_order')],
                            formatters={'@reference_time' : 'datetime'})

    scatter = hv.DynamicMap(dash_class.get_scatter)
    scatter.opts(
        height=scatter_height, 
        width=scatter_width,
        tools=[scatter_hover],
        #colorbar=False,
        toolbar='right',#'above'
        margin=(0,0,0,0),
    )
    diag = hv.DynamicMap(dash_class.get_scatter_diagonal)

    map_hover = HoverTool(tooltips=[('Gage ID', '@primary_location_id'),
                                        ('Name', '@name'),
                                        ('Stream Order','@stream_order')])
    points = hv.DynamicMap(dash_class.get_points)
    points.opts(
        height=map_height, 
        width=map_width,
        tools=[map_hover],
        colorbar=True,
        toolbar='right',#'above',
        margin=(0,0,0,0), 
    )
    colorbar_label = hv.DynamicMap(dash_class.get_colorbar_label)
    colorbar_label.opts(
        height=scatter_height, 
        width=35, 
        show_frame=False
    )
    peak_diff_hist = dash_class.get_histogram(
        'Peak % Error', 
        nbins=20
    )
    vol_diff_hist = dash_class.get_histogram(
        'Volume % Error', 
        nbins=20
    )
    peak_timediff_hist = dash_class.get_histogram(
        'Peak Timing Error', 
        nbins=20
    )
    area_hist = dash_class.get_histogram(
        'Drainage Area', 
        nbins=10
    )
    eco_hist = dash_class.get_categorical_histogram(
        'Ecoregion',
        labels=dash_class.ecoregion_labels
    )
    order_hist = dash_class.get_categorical_histogram(
        'Stream Order'
    )
    
    peak_diff_hist.opts(**hist_opts)
    vol_diff_hist.opts(**hist_opts)
    peak_timediff_hist.opts(**hist_opts)
    area_hist.opts(**hist_opts)
    eco_hist.opts(**hist_opts)
    order_hist.opts(**hist_opts)
    hist_layout = (peak_diff_hist + vol_diff_hist + peak_timediff_hist \
                   + order_hist + eco_hist).cols(5)
    hist_layout.opts(shared_axes=False)

    ls = hv.link_selections.instance()

    layout = pn.Column(
        pn.Row(
            pn.pane.HTML(
                "Explore Linked Plots",
                 styles={
                     'color' : 'royalblue', 
                     'font-size': '24px', 
                     'font-weight': 'bold'
                 }, 
                margin=(10,10,10,10)
            ), 
        ),
        pn.Row(
            pn.Column(
                scatter_widget,
                color_widget,
            ),
            pn.Column(ls(scatter)*diag + basemap*ls(points)),
            #pn.Column(basemap*ls(points)),
            pn.Column(colorbar_label)
        ),
        ls(hist_layout),
    )
    
    return layout

