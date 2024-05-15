'''
build observed summary dashboard
'''
import panel as pn
import datashader as ds
import holoviews as hv
hv.extension('bokeh', logo=False)

from holoviews.operation.datashader import rasterize

from . import class_explorer, common, legends
                 
def build(
    dash_class: class_explorer.ForecastExplorer,
    restrict_to_event_period: bool = False, 
    precip_value_max: float = None, 
    precip_diff_max: float = None,
    ts_cmap: list = []
):   
    pn.config.throttled = True
    if 'precip_metrics_gdf' not in dir(dash_class):
        dash_class.initialize(restrict_to_event_period)  
    dash_class.get_forecasts = True
    
    if precip_value_max:
        precip_value_clim = (0, precip_value_max)
    else:
        precip_value_clim = dash_class.precip_clims['value']

    if precip_diff_max:
        precip_diff_clim = (-precip_diff_max, precip_diff_max)
    else:
        precip_diff_clim = dash_class.precip_clims['sum_diff']
        
    title_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d %Hz')} to " \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d %Hz')}"    
    filename_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d_%Hz')}_" \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d_%Hz')}"  

    if not ts_cmap:
        ts_cmap = ['gray']
    dash_class.ts_cmap = ts_cmap

    map_width = 360
    map_height = 300
   
    states = common.get_states(dash_class.geo).opts(
        width=map_width, 
        height=map_height
    )        
    raster_opts = dict(  
        border=0,
        labelled=[],
        width=map_width, 
        height=map_height
    )
    ts_opts = dict(
        height=300,
        width=550
    )
    
    slider, button_forward, button_backward = common.get_ref_time_widgets(dash_class) 
    
    button_save_png = pn.widgets.Button(name='Save PNG', button_type='success',
                                        width=40, height=30, margin=(20,20,0,20))
    
    obs_raster = rasterize(
        hv.DynamicMap(dash_class.get_precip_obs_polygons_reftime), 
        aggregator=ds.mean('primary_sum'), 
        precompute=True
    )
    fcst_raster = rasterize(
        hv.DynamicMap(dash_class.get_precip_fcst_polygons_reftime), 
        aggregator=ds.mean('secondary_sum'), 
        precompute=True
    )    
    diff_raster = rasterize(
        hv.DynamicMap(dash_class.get_precip_diff_polygons_reftime), 
        aggregator=ds.mean('sum_diff'), 
        precompute=True
    )    
    
    obs_raster.opts(
        **raster_opts,
        cmap=dash_class.precip_cmap,
        clim=precip_value_clim, 
        title=f"{dash_class.precip_obs_source} "\
              f"Total by HUC10 {dash_class.precip_unit_label}", 
        tools=['tap','hover'],
        colorbar=False
    )
    fcst_raster.opts(
        **raster_opts,
        cmap=dash_class.precip_cmap, 
        clim=precip_value_clim, 
        title=f"Forecast ({dash_class.precip_fcst_source}) "\
              f"Total {dash_class.precip_unit_label}", 
        tools=['hover'],
        yaxis='bare',
        toolbar=None,
        colorbar=True
    )
    diff_raster.opts(
        **raster_opts,
        cmap=dash_class.precip_diff_cmap,
        clim=precip_diff_clim, 
        title=f"Difference {dash_class.precip_unit_label}",
        tools=['hover'],
        yaxis='bare',
        toolbar=None,
        colorbar=True
    )               
    
    dash_class.coord_stream.source = obs_raster
    
    selected_point = hv.DynamicMap(
        dash_class.get_selected_point_from_xy
    ).opts(
        legend_position='bottom_left'
    )
    obs_hourly = hv.DynamicMap(
        dash_class.get_precip_obs_timeseries_hourly_bars
    )
    fcst_hourly_all = hv.DynamicMap(
        dash_class.get_precip_fcst_timeseries_hourly_all_curve
    )
    fcst_hourly = hv.DynamicMap(
        dash_class.get_precip_fcst_timeseries_hourly_bars
    )
    window_hourly = hv.DynamicMap(
        dash_class.get_precip_fcst_timeseries_hourly_window
    )
    hourly_overlay = fcst_hourly_all * obs_hourly * fcst_hourly * window_hourly
    hourly_overlay.opts(**ts_opts)

    obs_cumul = hv.DynamicMap(dash_class.get_precip_obs_timeseries_cumulative)
    fcst_cumul_all = hv.DynamicMap(dash_class.get_precip_fcst_timeseries_cumulative_all)
    fcst_cumul = hv.DynamicMap(dash_class.get_precip_fcst_timeseries_cumulative)
    window_cumul = hv.DynamicMap(dash_class.get_precip_fcst_timeseries_cumulative_window)
    cumul_overlay = fcst_cumul_all  * obs_cumul * fcst_cumul * window_cumul
    cumul_overlay.opts(**ts_opts)

    def get_exported_layout():
        return pn.Column(
            pn.pane.HTML(
                dash_class.ref_time_text,
                styles={
                    'color' : 'black', 
                    'font-size': '20px', 
                    'font-weight': 'bold'
                }, 
                margin=(10,0,0,0)
            ),   
            pn.Spacer(height=20),
            pn.Row(
                pn.Spacer(width=20),
                (obs_raster * states * selected_point).opts(
                    show_legend=True, 
                    legend_opts={
                        'background_fill_alpha': 0.2, 
                        'label_text_font_size': '9pt', 
                        'padding':0
                    }
                ),
                fcst_raster * states,
                diff_raster * states
            ),
            pn.Spacer(height=10),
            pn.Row(
                pn.Column(
                    pn.pane.HTML(
                        "Hourly", 
                        styles={
                            'color' : 'black', 
                            'font-size': '18px',
                            'font-weight': 'bold'
                        }, 
                        margin=(0,0,0,60)
                    ),   
                    hourly_overlay,
                ),
                pn.Column(
                    pn.pane.HTML(
                        "Cumulative", 
                        styles={
                            'color' : 'black', 
                            'font-size': '18px',
                            'font-weight': 'bold'
                        }, 
                        margin=(0,0,0,50)
                    ),   
                    cumul_overlay,
                ),
                pn.Column(
                    pn.Spacer(height=50),
                    legends.get_precip_timeseries_legend()
                )
            )
        )
    layout = pn.Column(
        pn.Spacer(height=40),    
        pn.pane.HTML(
            "Explore Precip by Forecast",
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
            f"precip_diff_{filename_dates}_{dash_class.ts_poly_id}_"\
            f"{dash_class.ref_time_str.replace(' ','_')}.png"
        )
        exported_layout = get_exported_layout()
        exported_layout.save(str(pngfile))
        print(f'exported {pngfile}') 
    button_save_png.on_click(save_png)     
    
    return layout   
            

