'''
elements used by multiple dashboards with default styles
'''

import panel as pn
import holoviews as hv
import geoviews as gv
import cartopy.crs as ccrs
hv.extension('bokeh', logo=False)
gv.extension('bokeh', logo=False)

from . import class_explorer
from .. import config

def get_states(
    geo: config.Geo
) -> gv.Polygons:
    return gv.Polygons(
        geo.states.to_crs(3857), 
        vdims=['STUSPS'], 
        crs=ccrs.GOOGLE_MERCATOR
    ).opts(   
        xlim=geo.map_limits['xlims_mercator'], 
        ylim=geo.map_limits['ylims_mercator'], 
        color_index=None, 
        color=None, 
        line_color='darkgray',
        nonselection_alpha=1
    )
    
def get_basemap(
    geo: config.Geo
) -> hv.Tiles:
    return hv.element.tiles.CartoLight().opts(
        xlim=geo.map_limits['xlims_mercator'], 
        ylim=geo.map_limits['ylims_mercator'], 
    )    

def get_ts_plot_adjust(
    dash_class: class_explorer.ForecastExplorer
):

    if dash_class.flow_location_max < 10:
        ts_spacer = 10
        ts_size_adj = 20    
    elif 10 <= dash_class.flow_location_max < 100:
        ts_spacer = 10
        ts_size_adj = 20
    elif 100 <= dash_class.flow_location_max < 1000:
        ts_spacer = 10
        ts_size_adj = 20
    elif 1000 <= dash_class.flow_location_max < 10000:
        ts_spacer = 10
        ts_size_adj = 20        
    elif 10000 <= dash_class.flow_location_max < 100000:
        ts_spacer = 10
        ts_size_adj = 20
    else:
        ts_spacer = 10
        ts_size_adj = 20

    return [ts_spacer, ts_size_adj] 
        
def get_ref_time_widgets(
    dash_class: class_explorer.ForecastExplorer
):

    ref_time_slider = pn.Param(
        dash_class,
        widgets={
            "ref_time_str" : {"type" : pn.widgets.DiscreteSlider,  
                          "options" : dash_class.ref_time_list_str,
                          "name" : "Select forecast issue/reference time",
                          "value" : dash_class.ref_time_str,
                          "width" : 400, 
                          "margin" : (10,0,0,0)}
        },
        parameters=["ref_time_str"],
        show_name=False,
        width=400
    )
    slider = ref_time_slider.widget('ref_time_str')
    
    button_forward = pn.widgets.Button(
        name='\u25b6', 
        width=20, 
        height=30,
        button_type='primary', 
        margin=(20,20,0,20)
    )
    def increment_forward(
        event: config.Event
    ):
        index = dash_class.ref_time_list_str.index(slider.value)
        slider.value = dash_class.ref_time_list_str[index+1]
        dash_class.ref_time_str = slider.value
    button_forward.on_click(increment_forward) 
    
    button_backward = pn.widgets.Button(
        name='\u25c0', 
        width=20, 
        height=30,
        button_type='primary', 
        margin=(20,20,0,20)
    )
    def increment_backward(
        event
    ):
        index = dash_class.ref_time_list_str.index(slider.value)
        slider.value = dash_class.ref_time_list_str[index-1]
        dash_class.ref_time_str = slider.value
    button_backward.on_click(increment_backward)    

    return [slider, button_forward, button_backward]