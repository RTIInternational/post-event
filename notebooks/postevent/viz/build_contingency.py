'''
build contingency map layout
'''
import panel as pn
import colorcet as cc
import holoviews as hv
hv.extension('bokeh', logo=False)

from bokeh.models import HoverTool

from . import class_explorer
from . import common

def build(
    dash_class: class_explorer.ForecastExplorer,
    restrict_to_event_period: bool = False, 
) -> pn.layout:

    
    if 'flow_metrics_gdf' not in dir(dash_class):
        dash_class.initialize(restrict_to_event_period)  
    dash_class.get_forecasts = True

    title_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d %Hz')} to " \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d %Hz')}"    
    filename_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d_%Hz')}_" \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d_%Hz')}"      

    button_save_png = pn.widgets.Button(name='Save PNG', 
                                        button_type='success',
                                        width=40, 
                                        height=30, 
                                        margin=(10,0,0,20)
                                       )
    map_width = 400
    map_height = 350    
    states = common.get_states(dash_class.geo).opts(
        width=map_width, 
        height=map_height
    )
    basemap = common.get_basemap(dash_class.geo).opts(
                height=map_height, 
                width=map_width
    )
    opts = dict(
        width=map_width,
        height=map_height,
        border=0,
        labelled=[],
        colorbar = True,
        alpha = 1,
        xaxis = 'bare',
        yaxis = 'bare',
        size=6,
        line_color = 'gray',
        line_width = 0.5,
        toolbar = 'right'
    )   

    hover = [('Gage ID', '@primary_location_id'),('Name', '@name')]
    
    tp_hover = HoverTool(tooltips=hover + [('#', '@true_positive')]) 
    tp_points = dash_class.get_contingency_matrix_count(
        matrix_category = 'true_positive'
    ).opts(
        **opts, 
        cmap = ['#ffffff'] + cc.rainbow, #['#ffffff'] + cc.CET_L6[::-1],#[50:], # 'blues
        clim = (0, dash_class.flow_points_gdf['true_negative'].max()),
        title='YES - # true positives (all hits)',
        tools=[tp_hover]
    )
    
    fp_hover = HoverTool(tooltips=hover + [('#', '@false_positive')]) 
    fp_points = dash_class.get_contingency_matrix_count(
        matrix_category = 'false_positive'
    ).opts(
        **opts, 
        cmap = ['#ffffff'] + cc.rainbow, #['#ffffff'] + cc.CET_CBTL4[::-1][30:], #'reds', 
        clim = (0, dash_class.flow_points_gdf['true_negative'].max()),
        title='NO - # false positives (false alarms)',
        tools=[fp_hover]
    )
    
    fn_hover = HoverTool(tooltips=hover + [('#', '@false_negative')]) 
    fn_points = dash_class.get_contingency_matrix_count(
        matrix_category = 'false_negative'
    ).opts(
        **opts, 
        cmap = ['#ffffff'] + cc.rainbow, #['#ffffff'] + cc.CET_L18[50:], #'oranges', 
        clim = (0, dash_class.flow_points_gdf['true_negative'].max()),
        title='NO - # false negatives (misses)',
        tools=[fn_hover]
    ) 

    signal_hover = HoverTool(tooltips=hover + [('#', '@prior_hw_signal_count')])
    signal_count_points = dash_class.get_summary_points(
        column = 'prior_hw_signal_count'
    ).opts(
        **opts,
        cmap = ['#ffffff'] + cc.rainbow, #['#ffffff'] +  cc.CET_L6[::-1],
        clim = (0, dash_class.flow_points_gdf['true_negative'].max()),
        title='# of true positives issued ahead of event',
        tools=[signal_hover]
    )   
    
    time_hover = HoverTool(tooltips=hover + [('hours', '@max_hw_signal_time')])
    max_signal_time_points = dash_class.get_summary_points(
        column = 'max_hw_signal_time'
    ).opts(
        **opts,
        cmap = ['#ffffff'] + cc.rainbow,
        title='Maximum signal lead time (hrs)',
        tools=[time_hover]
    ) 

    map_layout = (
        basemap * states * tp_points + 
        basemap * states * fp_points + 
        basemap * states * fn_points 
    ).cols(2)    
    map_layout_2 = (
        basemap * states * signal_count_points +
        basemap * states * max_signal_time_points 
    ).cols(2)
    
    exported_layout = pn.Column(
        pn.pane.HTML(f"Forecast Reference Times: {title_dates}",
                     styles={
                         'color' : 'black', 
                         'font-size': '20px', 
                         'font-weight': 'bold'
                     }, margin=(10,0,10,0)),
        pn.Row(
            map_layout.opts(
                toolbar = 'right'
            ),
            map_layout_2.opts(
                toolbar = 'right'
            )
        ),
    )

    layout = pn.Column(   
        pn.Row(
            pn.pane.HTML(f"High Water Exceedance",
                         styles={
                             'color' : 'royalblue', 
                             'font-size': '20px', 
                             'font-weight': 'bold'
                         }, margin=(10,0,0,0)),
        ),
        exported_layout
    )
    
    return layout