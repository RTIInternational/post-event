'''
build observed summary dashboard
'''
import panel as pn
import holoviews as hv
hv.extension('bokeh', logo=False)

from pathlib import Path
from bokeh.models import HoverTool

from . import class_explorer
from . import common
                 
def build(
    dash_class: class_explorer.ForecastExplorer,
    restrict_to_event_period: bool = False, 
    precip_value_max: float = None, 
    flow_value_max: float = None, 
    roc_value_max: float = None, 
    ) -> pn.layout:
    '''
    build observed summary dashboard
    '''

    # initialized the dash_class to get the data, 
    # if not already for another dashboard
    if 'flow_metrics_gdf' not in dir(dash_class):
        dash_class.initialize(restrict_to_event_period)  

    # read usgs_basin precip for ROC calcs
    dash_class.get_usgs_basin_precip()

    title_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d %Hz')} to " \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d %Hz')}"    
    filename_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d_%Hz')}_" \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d_%Hz')}"  

    # prevent pulling forecasts upon location selection to save time 
    # for this dashboard
    dash_class.get_location_forecasts = False

    # get color limits based on data extents 
    # (consider replacing with fixed colormap from WPC)
    if precip_value_max:
        precip_value_clim = (0, precip_value_max)
    else:
        precip_value_clim = dash_class.precip_clims['value']

    if flow_value_max:
        flow_value_clim = (0, flow_value_max)
    else:
        flow_value_clim = dash_class.flow_clims['vol_norm']

    if roc_value_max:
        roc_value_clim = (0, roc_value_max)
    else:
        roc_value_clim = (0,2)

    # get states and basemap, set map size here
    map_width = 480
    map_height = 400
    states = common.get_states(dash_class.geo).opts(
        width=map_width, 
        height=map_height
    )        
    basemap = common.get_basemap(dash_class.geo).opts(
                height=map_height, 
                width=map_width
    )

    # set options per element type
    raster_opts = dict(  
        border=0,
        labelled=[],
        width=map_width, 
        height=map_height
    )
    ts_opts = dict(
        height=200,
        width=500,
        toolbar=None,
    )    
    point_opts = dict(
        width=map_width, 
        height=map_height,
        size=6,
        line_color = 'gray',
        line_width = 0.5,
        border=0,
        labelled=[],
        colorbar = True,
        nonselection_alpha=1,
    )    

    # get rasterized precip polygons   
    obs_raster = dash_class.get_precip_obs_polygons_total()
    obs_raster.opts(
        **raster_opts,
        cmap=dash_class.precip_cmap,
        clim=precip_value_clim, 
        title=f"Total QPE ({dash_class.precip_obs_source}) "\
              f"by HUC10 {dash_class.precip_unit_label}", 
        colorbar=True,
    )      
    polygon_centroids = dash_class.get_precip_polygon_centroids().opts(
        fill_color = 'none',
        line_color = 'lightgray',
        size = 4,
        tools=['hover'],
        alpha=0.1
    )

    # get obs flow points
    flow_hover = HoverTool(tooltips=[('Location', '@location_id'),
                                     ('Volume', '@sum_norm')])        
    flow_obs_points = dash_class.get_flow_volume_obs()
    flow_obs_points.opts(
        **point_opts, 
        clim=flow_value_clim, 
        title = f"Observed Flow Volume {dash_class.norm_vol_unit_label} " \
                f"(Normalized)",
        tools=['tap', flow_hover],
    )
    # assign flow points as the point stream (a parameter of dash_class)
    dash_class.point_stream.source = flow_obs_points    

    flow_obs_exceed_points = dash_class.get_flow_obs_exceed_points()
    flow_obs_exceed_points.opts(
        fill_color = 'none',
        line_color = 'black',        
        size=7,
        line_width=1,
        nonselection_alpha=1,
        width=map_width, 
        height=map_height
    )

    roc_hover = HoverTool(tooltips=[('Location', '@location_id'),
                                     ('Runoff Coeff.', '@roc')])   
    roc_title = f"Observed Runoff Coefficient"
    roc_points = dash_class.get_roc_obs().opts(
        **point_opts,
        title = roc_title,
        tools=[roc_hover],
        clim=roc_value_clim
    )

    nb_hover = HoverTool(tooltips=[('Location', '@location_id')])   
    no_basin_list = list(
        set(list(flow_obs_points['location_id'])) - \
        set(list(roc_points['location_id']))
    )
    no_basin_points = flow_obs_points.select(
        location_id=no_basin_list
    )

    # get black outline on the selected point (rather than using alpha)
    selected_point = hv.DynamicMap(dash_class.get_selected_point).opts(
        line_color='fuchsia',
        size=10
    )
    usgs_basin = hv.DynamicMap(dash_class.get_selected_usgs_basin)

    # define the precip map and flow map overlays
    precip_overlay \
        = obs_raster \
        * states \
        * polygon_centroids \
        * selected_point.relabel('')

    points_overlay \
        = basemap \
        * states \
        * flow_obs_points \
        * flow_obs_exceed_points.relabel('HW exceedance') \
        * usgs_basin \
        * selected_point

    roc_overlay \
        = basemap \
        * states \
        * no_basin_points.relabel('No basin boundary available').opts(
            color = None,
            fill_color = 'black',
            line_color = None,
            tools = [nb_hover],
            size = 4,
            title = roc_title) \
        * roc_points \
        * usgs_basin \
        * selected_point

    # get the precip time series and build the overlay with multi-y axis
    precip_hourly = hv.DynamicMap(
        dash_class.get_precip_obs_timeseries_hourly_bars
    ).relabel('hourly').opts(
        **ts_opts, 
        color='dodgerblue',
        ylim = (0, 2), #dash_class.precip_obs_max*1.05),
        shared_axes = False        
    )
    precip_cumul = hv.DynamicMap(
        dash_class.get_precip_obs_timeseries_cumulative
    ).relabel('cumul').opts(
        **ts_opts, 
        yaxis='right',
        ylim = (-0.1, dash_class.precip_obs_max*1.05),
        shared_axes = False,
    )
    precip_ts_overlay = (precip_hourly * precip_cumul).opts(
        multi_y=True
    )

    # get the flow time series, high water line and custom tick lables; 
    # build the overlay
    flow_ts = hv.DynamicMap(
        dash_class.get_flow_obs_timeseries
    ).opts(
        **ts_opts,
        line_dash = 'solid',
        line_color = 'blue'
    )
    flow_hw_line = hv.DynamicMap(
        dash_class.get_hw_threshold
    ).opts(
        **ts_opts
    )
    tick_labels = dash_class.get_xtick_date_labels_daily()
    flow_ts_overlay = (flow_ts * flow_hw_line).opts(
        xticks=tick_labels, 
        xrotation=0
    )

    # build layout to be exported with save-png button
    def get_exported_layout():
        return pn.Column(
            pn.pane.HTML(f"Summary Period: {title_dates}",
                 styles={
                     'color' : 'black', 
                     'font-size': '20px', 
                     'font-weight': 'bold'
                 }, margin=(0,0,10,35)),
            pn.Row(
                precip_overlay.opts(show_legend=False),
                points_overlay.opts(
                    show_legend=True, 
                    legend_position='bottom_left', 
                    legend_opts={'background_fill_alpha': 0.2, 
                                 'label_text_font_size': '9pt', 
                                 'padding':0}
                ),
                roc_overlay.opts(
                    show_legend=True, 
                    legend_position='bottom_left', 
                    legend_opts={'background_fill_alpha': 0.2, 
                                 'label_text_font_size': '9pt', 
                                 'padding':0}
                ),
            ),
            pn.Row(
                 precip_ts_overlay.opts(show_legend=False),
                 pn.Spacer(width=20),
                 flow_ts_overlay.opts(show_legend=False),
            )
        )    

    button_save_png = pn.widgets.Button(name='Save PNG', 
                                        button_type='success',
                                        width=40, 
                                        height=30, 
                                        margin=(10,0,20,20)
                                       )
    # build full layout
    layout = pn.Column(
        pn.Row(
            button_save_png
        ),       
        get_exported_layout()
    ) 
    def save_png(event):
        if not dash_class.paths.viz_dir.exists():
            dash_class.paths.viz_dir.mkdir(parents=True, exist_ok=True)
        pngfile = Path(
            dash_class.paths.viz_dir, 
            f"observed_summary_{filename_dates}_{dash_class.point_id}.png"
        )
        exported_layout = get_exported_layout()
        exported_layout.save(str(pngfile))
        print(f'exported {pngfile}')  
    button_save_png.on_click(save_png) 
    
    return layout       
            

