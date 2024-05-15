'''
build event error summary dashboard
'''
import panel as pn
import holoviews as hv
hv.extension('bokeh', logo=False)

from pathlib import Path
from bokeh.models import HoverTool

from . import class_explorer
from . import common
from . import legends
                 
def build(
    dash_class: class_explorer.ForecastExplorer,
    restrict_to_event_period: bool = False, 
    precip_value_max: float = None, 
    precip_diff_max: float = None,
    flow_value_max: float = None,
    flow_diff_max: float = None,
    ts_cmap: list = []
):   
    if 'flow_metrics_gdf' not in dir(dash_class):
        dash_class.initialize(restrict_to_event_period)  
    dash_class.get_forecasts = True

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
        flow_value_clim = dash_class.flow_clims['vol_norm']

    if flow_diff_max:
        flow_diff_clim = (-flow_diff_max, flow_diff_max)
    else:
        flow_diff_clim = dash_class.flow_clims['vol_diff_norm']

    title_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d %Hz')} to " \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d %Hz')}"    
    filename_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d_%Hz')}_" \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d_%Hz')}"  

    if not ts_cmap:
        ts_cmap = ['gray']
    dash_class.ts_cmap = ts_cmap

    # get states and basemap, set map size here
    map_width = 450
    map_height = 350
    states = common.get_states(dash_class.geo).opts()        
    basemap = common.get_basemap(dash_class.geo).opts()
    map_opts = dict(
        border=0,
        labelled=[],
        colorbar = True
    )
    point_opts = dict(
        size=6,
        line_color = 'gray',
        line_width = 0.5
    )   
    ts_height = 300
    ts_width = 600
    ts_opts = dict(
        height=ts_height,
        width=ts_width,
        toolbar='right',
    )   

    # precip maps - observed and average difference
    precip_obs_raster = dash_class.get_precip_obs_polygons_total()
    precip_obs_raster.opts(
        **map_opts, 
        width = map_width-50,
        height = map_height,
        clim=precip_value_clim,
        title=f"Total QPE ({dash_class.precip_obs_source}) by HUC10 " \
              f"{dash_class.precip_unit_label}", 
        toolbar = None,
        show_legend=False
    )
    precip_diff_raster = dash_class.get_precip_ave_difference()
    precip_diff_raster.opts(
        **map_opts, 
        width = map_width,
        height = map_height,
        clim=precip_diff_clim,
        title=f"Average Precip Forecast Error " \
              f"({dash_class.precip_fcst_source}) " \
              f"{dash_class.precip_unit_label}",
        show_legend=False
    )

    # flow maps - observed and average difference (normalized volume)
    flow_obs_points = dash_class.get_flow_volume_obs()
    flow_obs_points.opts(
        **map_opts, 
        **point_opts,
        width = map_width-50,
        height = map_height,
        clim=flow_value_clim, 
        title = f"Observed Flow Volume " \
                f"{dash_class.norm_vol_unit_label} (Normalized)",
        toolbar = None,
    )
    flow_error_hover = HoverTool(tooltips=[('Gage ID', '@primary_location_id'),
                                           ('Name', '@name'),
                                           ('Volume Error (Norm.)', '@mean_vol_norm_diff')])  
    
    #flow_error_points = dash_class.get_flow_volume_ave_difference()
    flow_error_points = dash_class.get_summary_points('mean_vol_norm_diff')
    flow_error_points.opts(
        **map_opts, 
        **point_opts,
        width = map_width,
        height = map_height,
        tools=['tap', flow_error_hover],
        cmap=dash_class.flow_diff_cmap,
        clim=flow_diff_clim,
        nonselection_alpha=1,
        title = f"Average Flow Volume Forecast Error "\
                f"{dash_class.norm_vol_unit_label}",
    )
    dash_class.point_stream.source = flow_error_points

    flow_obs_exceed_points = dash_class.get_flow_obs_exceed_points()
    flow_obs_exceed_points.opts(
        fill_color = 'none',
        line_color = 'black',        
        labelled=[],
        tools=[],
        size=7,
        line_width=2,
        nonselection_alpha=1,
        width=map_width, 
        height=map_height
    )
    # get black outline on the selected point (rather than using alpha)
    selected_point = hv.DynamicMap(dash_class.get_selected_point).opts(
        line_color='fuchsia' #black'
    )
    # get usgs drainage basin boundary, if available
    usgs_basin = hv.DynamicMap(dash_class.get_selected_usgs_basin)
    
    # precip time series
    precip_obs_ts_cumul = hv.DynamicMap(
        dash_class.get_precip_obs_timeseries_cumulative
    )
    precip_fcst_ts_all_cumul = hv.DynamicMap(
        dash_class.get_precip_fcst_timeseries_cumulative_all
    )
    precip_ts_overlay = precip_fcst_ts_all_cumul * precip_obs_ts_cumul
    precip_ts_overlay.opts(
        **ts_opts, 
        yaxis = 'left', 
        shared_axes=False
    )
    # flow time series
    flow_obs_ts = hv.DynamicMap(dash_class.get_flow_obs_timeseries).opts(
        line_color='blue',
        line_dash='solid',
        #title= f"Streamflow: {dash_class.point_id} -{dash_class.point_name} ", 
    )
    flow_noda_ts = hv.DynamicMap(dash_class.get_flow_noda_timeseries).opts(
        line_color='black',
        line_dash='dashed'
        #title= f"Streamflow: {dash_class.point_id} -{dash_class.point_name} ",
    )
    flow_fcst_ts_all = hv.DynamicMap(
        dash_class.get_flow_fcst_timeseries_all
    ).opts(show_legend=True)       
    flow_hw_line = hv.DynamicMap(dash_class.get_hw_threshold)
    flow_ts_overlay = flow_fcst_ts_all * flow_noda_ts \
                         * flow_obs_ts * flow_hw_line
    flow_ts_overlay.opts(
        **ts_opts, 
        shared_axes=False,
        #title= f"Streamflow: {dash_class.point_id} - {dash_class.point_name} ",
    )

    [ts_shift, ts_size_adj] = common.get_ts_plot_adjust(dash_class)

    # build layout to be exported with save-png button
    def get_exported_layout_maps():
        return pn.Column(
            pn.pane.HTML(
                f"Summary Period: {title_dates}",
                styles={
                    'color' : 'black', 
                    'font-size': '20px', 
                    'font-weight': 'bold'
                }, margin=(0,0,10,35)
            ),
            pn.Row(
                pn.Column(
                    pn.Row(
                        (precip_obs_raster * states \
                         * selected_point * usgs_basin).opts(show_legend=False),
                        (precip_diff_raster * states \
                         * selected_point * usgs_basin).opts(show_legend=False),
                    ),
                    pn.Row(
                        (basemap * states * flow_obs_points \
                         * selected_point.relabel('') * usgs_basin \
                         * flow_obs_exceed_points.relabel('HW exceedance')).opts(
                            show_legend=True, 
                            legend_position='bottom_left',
                            legend_opts={
                                'background_fill_alpha': 0.2, 
                                'label_text_font_size': '9pt', 
                                'padding':0})
                        ,
                        (basemap * states * flow_error_points \
                         * selected_point * usgs_basin).opts(
                            show_legend=True, 
                            legend_position='bottom_left',
                            legend_opts={
                                'background_fill_alpha': 0.2, 
                                'label_text_font_size': '9pt', 
                                'padding':0})
                    )
                )
            )
        )

    def get_exported_layout_tsplots():
        return pn.Column(
                pn.Spacer(height=40),
                pn.Row(
                    pn.Spacer(width = ts_shift), 
                    precip_ts_overlay.opts(width = ts_width-ts_size_adj)
                ),
                pn.Row(flow_ts_overlay),
                pn.Row(
                    pn.Spacer(width=100),
                    legends.get_streamflow_timeseries_summary_legend()
                )
            )
        
    button_save_png = pn.widgets.Button(name='Save PNG', 
                                        button_type='success',
                                        width=40, 
                                        height=30, 
                                        margin=(10,0,20,20)
                                       )
    # layout with button
    layout = pn.Column(
        pn.Row(
            button_save_png
        ),    
        pn.Row(
            get_exported_layout_maps(), 
            get_exported_layout_tsplots()
        )
    )
    def save_png(event):
        if not dash_class.paths.viz_dir.exists():
            dash_class.paths.viz_dir.mkdir(parents=True, exist_ok=True)

        if dash_class.point_id:
            exported_layout = pn.Row(
                get_exported_layout_maps(), 
                get_exported_layout_tsplots()
            )
            pngfile = Path(
                dash_class.paths.viz_dir, 
                f"forecast_error_summary_{filename_dates}_{dash_class.point_id}.png"
            )
        else:
            exported_layout = pn.Row(get_exported_layout_maps())
            pngfile = Path(
                dash_class.paths.viz_dir, 
                f"forecast_error_summary_{filename_dates}.png"
            )
        exported_layout.save(str(pngfile))
        print(f'exported {pngfile}')  
            
    button_save_png.on_click(save_png) 

    return layout