'''
build observed summary dashboard
'''
import panel as pn
import datashader as ds
import holoviews as hv
hv.extension('bokeh', logo=False)

from holoviews.operation.datashader import rasterize
from bokeh.models import HoverTool

from . import class_explorer, common, legends
                 
def build(
    dash_class: class_explorer.ForecastExplorer,
    restrict_to_event_period: bool = False, 
    precip_value_max: float = None, 
    precip_diff_max: float = None,
    flow_variable: str = 'volume',  #'peak'
    flow_value_max: float = None,
    flow_diff_max: float = None,
    ts_cmap: list = []
):   
    pn.config.throttled = True
    if 'precip_metrics_gdf' not in dir(dash_class):
        dash_class.initialize(restrict_to_event_period)  
    dash_class.get_forecasts = True
    dash_class.get_cumulative = True
    
    if precip_value_max:
        precip_value_clim = (0, precip_value_max)
    else:
        precip_value_clim = self.precip_clims['value']

    if precip_diff_max:
        precip_diff_clim = (-precip_diff_max, precip_diff_max)
    else:
        precip_diff_clim = self.precip_clims['sum_diff']

    if flow_value_max:
        flow_value_clim = (0, flow_value_max)
    else:
        if flow_variable == 'volume':
            flow_value_clim = dash_class.flow_clims['vol_norm']
        else:
            flow_value_clim = dash_class.flow_clims['peak_norm']

    if flow_diff_max:
        flow_diff_clim = (-flow_diff_max, flow_diff_max)
    else:
        if flow_variable == 'volume':
            flow_diff_clim = dash_class.flow_clims['vol_norm_norm']
        else:
            flow_diff_clim = dash_class.flow_clims['peak_norm']

    title_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d %Hz')} to " \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d %Hz')}"    
    filename_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d_%Hz')}_" \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d_%Hz')}"  

    if not ts_cmap:
        ts_cmap = ['gray']
    dash_class.ts_cmap = ts_cmap

    map_width = 450
    map_height = 350

    states = common.get_states(dash_class.geo).opts(
        width=map_width, 
        height=map_height
    )          
    basemap = common.get_basemap(dash_class.geo).opts(
        width=map_width, 
        height=map_height
    )
    raster_opts = dict(      
        border=0,
        labelled=[],
        width=map_width, 
        height=map_height,
        tools=['hover'],
        xaxis='bare',
    )
    point_opts = dict(
        border=0,
        labelled=[],
        size=7,
        line_color = 'gray',
        line_width = 0.5,
        width=map_width, 
        height=map_height
    )    
    exceed_opts = dict(
        fill_color = 'none',
        line_color = 'black',        
        labelled=[],
        tools=[],
        size=9,
        line_width=2,
        nonselection_alpha=1,
        width=map_width, 
        height=map_height
    )
    ts_opts = dict(
        height=300, 
        width=580,
        axiswise=True,
        framewise=True, 
        toolbar=None
        #shared_axes=False
    )

    usgs_basin = hv.DynamicMap(dash_class.get_selected_usgs_basin)
    selected_point = hv.DynamicMap(dash_class.get_selected_point)
    
    [slider, 
     button_forward, 
     button_backward] = common.get_ref_time_widgets(dash_class) 
    button_save_png = pn.widgets.Button(
        name='Save PNG', 
        button_type='success',
        width=40, 
        height=30, 
        margin=(20,20,0,20)
    )
    
    #### setup precip components
    
    precip_obs_raster = rasterize(
        hv.DynamicMap(dash_class.get_precip_obs_polygons_reftime), 
        aggregator=ds.mean('primary_sum'), 
        precompute=True
    )
    precip_fcst_raster = rasterize(
        hv.DynamicMap(dash_class.get_precip_fcst_polygons_reftime), 
        aggregator=ds.mean('secondary_sum'), 
        precompute=True
    )
    precip_diff_raster = rasterize(
        hv.DynamicMap(dash_class.get_precip_diff_polygons_reftime), 
        aggregator=ds.mean('sum_diff'), 
        precompute=True
    )    

    precip_obs_raster.opts(
        **raster_opts,
        cmap=dash_class.precip_cmap,
        clim=precip_value_clim, 
        title=f"{dash_class.precip_obs_source} Total by HUC10 "\
              f"{dash_class.precip_unit_label}", 
        colorbar=False,
    )
    precip_fcst_raster.opts(
        **raster_opts,
        cmap=dash_class.precip_cmap, 
        clim=precip_value_clim, 
        title=f"Forecast ({dash_class.precip_fcst_source}) Total "\
              f"{dash_class.precip_unit_label}", 
        toolbar=None,
        yaxis='bare',
        colorbar=True,
    )
    precip_diff_raster.opts(
        **raster_opts,
        cmap=dash_class.precip_diff_cmap,
        clim=precip_diff_clim, 
        title=f"Difference {dash_class.precip_unit_label}",
        toolbar=None,
        yaxis='bare',
        colorbar=True,
    )       
    precip_obs_raster_overlay = precip_obs_raster \
                                * states \
                                * selected_point.relabel('') \
                                * usgs_basin
    precip_fcst_raster_overlay = precip_fcst_raster \
                                * states \
                                * selected_point.relabel('') \
                                * usgs_basin
    precip_diff_raster_overlay = precip_diff_raster \
                                * states \
                                * selected_point.relabel('') \
                                * usgs_basin

    # precip time series
    precip_obs_ts_cumul = hv.DynamicMap(
        dash_class.get_precip_obs_timeseries_cumulative
    )
    precip_fcst_ts_all_cumul = hv.DynamicMap(
        dash_class.get_precip_fcst_timeseries_cumulative_all
    )
    precip_fcst_ts_cumul = hv.DynamicMap(
        dash_class.get_precip_fcst_timeseries_cumulative
    )
    precip_fcst_window = hv.DynamicMap(
        dash_class.get_precip_fcst_timeseries_cumulative_window
    )

    precip_ts_overlay = precip_fcst_ts_all_cumul * precip_obs_ts_cumul \
                        * precip_fcst_ts_cumul * precip_fcst_window
    precip_ts_overlay.opts(
        **ts_opts, 
        yaxis = 'left', 
        shared_axes=False
    )
    

    #### setup streamflow components

    if flow_variable == 'peak':
        
        obs_col = 'primary_maximum_norm'
        fcst_col = 'secondary_maximum_norm'
        diff_col = 'maximum_percdiff'

        obs_points = hv.DynamicMap(
            dash_class.get_peakflow_norm_obs_points_reftime
        )
        fcst_points = hv.DynamicMap(
            dash_class.get_peakflow_norm_fcst_points_reftime
        )
        diff_points = hv.DynamicMap(
            dash_class.get_peakflow_percdiff_points_reftime
        )
        obs_title = f"Observed Peak (Normalized) "\
                    f"{dash_class.norm_flow_unit_label}"
        fcst_title = f"Forecast Peak (Normalized) "\
                     f"{dash_class.norm_flow_unit_label}"
        diff_title = f"Percent Difference (%)"
        hover_label = 'Norm. Flow'
        diff_hover_label = "% Diff."

        value_cmap = dash_class.peakflow_cmap
        diff_cmap = dash_class.flow_diff_cmap
        
    elif flow_variable == 'volume':
        
        obs_col = 'primary_sum_norm'        
        fcst_col = 'secondary_sum_norm'    
        diff_col = 'vol_norm_diff'   
        
        obs_points = hv.DynamicMap(
            dash_class.get_obs_vol_norm_points_reftime
        )
        fcst_points = hv.DynamicMap(
            dash_class.get_fcst_vol_norm_points_reftime
        )
        diff_points = hv.DynamicMap(
            dash_class.get_vol_norm_diff_points_reftime
        )
        
        obs_title = f"Obs Flow Volume (Norm.) "\
                    f"{dash_class.norm_vol_unit_label}"
        fcst_title = f"Forecast Flow Volume (Norm.) "\
                     f"{dash_class.norm_vol_unit_label}"
        diff_title = f"Flow Volume Forecast Error (in)"        
        
        hover_label = 'Norm. Vol'
        diff_hover_label = "Diff. Vol"
        
        value_cmap = dash_class.vol_cmap
        diff_cmap = dash_class.flow_diff_cmap

    obs_hover = HoverTool(tooltips=[('Location', '@primary_location_id'),
                                    (hover_label, '@'+obs_col)])
    fcst_hover = HoverTool(tooltips=[('Location', '@primary_location_id'),
                                    (hover_label, '@'+fcst_col)])
    diff_hover = HoverTool(tooltips=[('Location', '@primary_location_id'),
                                     (diff_hover_label, '@'+diff_col)])
    obs_points.opts(
        **point_opts,
        color=obs_col,
        cmap=value_cmap,
        clim=flow_value_clim, 
        title=obs_title, 
        tools=['tap',obs_hover],
        nonselection_alpha=1,
        show_legend=False,
        colorbar=False
    )      
    fcst_points.opts(
        **point_opts,
        color=fcst_col,
        cmap=value_cmap,
        clim=flow_value_clim, 
        title=fcst_title,  
        tools=[fcst_hover],
        toolbar=None,
        yaxis='bare',
        colorbar=True
    ) 
    diff_points.opts(
        **point_opts,
        color=diff_col,
        cmap=diff_cmap,
        clim=flow_diff_clim, 
        title=diff_title, 
        tools=[diff_hover],
        toolbar=None,
        yaxis='bare',
        colorbar=True
    )  
    dash_class.point_stream.source = obs_points

    obs_exceed_points = hv.DynamicMap(
        dash_class.get_peakflow_obs_exceed_points_reftime
    )
    fcst_exceed_points = hv.DynamicMap(
        dash_class.get_peakflow_fcst_exceed_points_reftime
    )
    obs_exceed_points.opts(
        **exceed_opts,
        title=obs_title,
        legend_position='bottom_left',
    )
    fcst_exceed_points.opts(
        **exceed_opts,
        title=fcst_title,
    )
    obs_points_overlay = basemap \
                        * obs_points \
                        * obs_exceed_points \
                        * selected_point \
                        * usgs_basin
    fcst_points_overlay = basemap \
                        * fcst_points \
                        * fcst_exceed_points \
                        * selected_point \
                        * usgs_basin
    diff_points_overlay = basemap \
                        * diff_points \
                        * selected_point \
                        * usgs_basin

    # flow timeseries data
    flow_obs_ts = hv.DynamicMap(dash_class.get_flow_obs_timeseries)
    flow_noda_ts = hv.DynamicMap(dash_class.get_flow_noda_timeseries)   
    flow_fcst_ts = hv.DynamicMap(dash_class.get_flow_fcst_timeseries)
    flow_fcst_ts_all = hv.DynamicMap(
        dash_class.get_flow_fcst_timeseries_all
    ).opts(show_legend=True)    
    flow_fcst_window = hv.DynamicMap(
        dash_class.get_flow_fcst_timeseries_window
    )        
    flow_hw_line = hv.DynamicMap(dash_class.get_hw_threshold)
    flow_norm_axis = hv.DynamicMap(dash_class.get_norm_axis)

    flow_ts_overlay = flow_fcst_ts_all \
                      * flow_noda_ts \
                      * flow_obs_ts \
                      * flow_fcst_ts \
                      * flow_hw_line \
                      * flow_fcst_window
    flow_ts_overlay.opts(
        **ts_opts
    )

    def get_exported_layout():
        return pn.Row(
            pn.Column(
                pn.pane.HTML(dash_class.ref_time_text,
                             styles={
                                 'color' : 'black', 
                                 'font-size': '20px', 
                                 'font-weight': 
                                 'bold'
                             }, 
                             margin=(10,0,20,0)
                            ),     
                pn.Row(
                    precip_obs_raster_overlay, 
                    precip_fcst_raster_overlay, 
                    precip_diff_raster_overlay
                ),
                pn.Row(
                    obs_points_overlay.opts(
                        show_legend=True, 
                        legend_opts={
                            'background_fill_alpha': 0.2, 
                            'label_text_font_size': '9pt', 
                            'padding':0
                        }
                    ),
                    fcst_points_overlay.opts(show_legend=False),                 
                    diff_points_overlay.opts(show_legend=False),
                ),
                pn.Spacer(height=10),
                pn.Row(
                    precip_ts_overlay,
                    flow_ts_overlay,
                    #flow_norm_axis,
                )
            ),
            pn.Column(
                pn.Spacer(height=550),
                legends.get_combined_dashboard_legend()
            )
        )
    layout = pn.Column(      
        pn.Spacer(height=40),
        pn.pane.HTML(
            "Explore Streamflow and Precip by Forecast",
             styles={
                 'color' : 'royalblue', 
                 'font-size': '24px', 
                 'font-weight': 'bold'
             }, 
            margin=(10,0,0,0)
        ),   
        pn.Row(
            pn.Spacer(width=20),
            slider,
            button_backward,
            button_forward,
            button_save_png
        ),
        get_exported_layout()
    ) 
    def save_png(event):
        if not dash_class.paths.viz_dir.exists():
            dash_class.paths.viz_dir.mkdir(parents=True, exist_ok=True) 
        pngfile = Path(
            dash_class.paths.viz_dir, 
            'streamflow_diff_' + dash_class.ref_time_str.replace(' ','_') + ".png"
        )
        exported_layout = get_exported_layout()
        exported_layout.save(str(pngfile))
        print(f'exported {pngfile}')  
    button_save_png.on_click(save_png) 
    
    return layout

