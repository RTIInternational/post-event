'''
build qaqc map layout
'''
import panel as pn
import colorcet as cc
import geoviews as gv
import holoviews as hv
hv.extension('bokeh', logo=False)

from bokeh.models import HoverTool

from . import common

def build(
    dash_class, 
    restrict_to_event_period = False, 
):
    # initialize data
    if 'flow_metrics_gdf' not in dir(dash_class):
        dash_class.initialize(restrict_to_event_period)  
    dash_class.get_forecasts = True

    # get date strings for plot titles and filenames
    title_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d %Hz')} to " \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d %Hz')}"    
    filename_dates \
      = f"{dash_class.dates.analysis_time_start.strftime('%Y-%m-%d_%Hz')}_" \
        f"{dash_class.dates.analysis_time_end.strftime('%Y-%m-%d_%Hz')}"      

    # set up basemaps and global plot options
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
    
    # get regular threshold value polygons
    hover = [('Gage ID', '@primary_location_id'),('Name', '@name')]
    thresh_hover = HoverTool(
        tooltips=hover + [('Flow', '@hw_threshold')]
    )
    gdf = dash_class.flow_points_gdf.copy()
    gdf['hw_threshold'] = gdf['hw_threshold'].replace(0.0,0.1)
    threshold_points = gv.Points(
        gdf, 
        vdims=['hw_threshold','primary_location_id','name']
    ).opts(
        **opts,
        color = 'hw_threshold',
        cmap = cc.rainbow,
        title=f'Threshold {dash_class.flow_unit_label} (log scale)',
        tools=[thresh_hover],
        logz=True
    ) 
    
    # get normalized threshold value polygons
    thresh_norm_hover = HoverTool(
        tooltips = hover + [('Flow (Norm.)', '@hw_threshold_norm')]
    )
    threshold_norm_points = dash_class.get_summary_points(
        column = 'hw_threshold_norm'
    ).opts(
        **opts,
        cmap = ['#ffffff'] + cc.rainbow,
        clim = (0,0.1),
        title=f'Normalized Threshold {dash_class.norm_flow_unit_label}',
        tools=[thresh_norm_hover]
    ) 
    
    # get points with threshold < 1
    gdf = dash_class.flow_points_gdf.copy()
    gdf_zeros = gdf[gdf['hw_threshold']<1]
    zero_thresh_points = gv.Points(
        gdf_zeros, 
        vdims=['hw_threshold_norm','primary_location_id','name'],
        label=f'threshold < 1 {dash_class.flow_unit_label}'
    ).opts(
        color = 'white',
        size = 9,
        line_color = 'black',
        line_width = 2,
        show_legend=True, 
    )       

    # get points with drainage area < 1
    gdf_zero_area = gdf[gdf['drainage_area']<1]
    zero_area_points = gv.Points(
        gdf_zero_area, 
        vdims=['drainage_area','primary_location_id','name'],
        label=f'drainage area < 1 {dash_class.area_label}'
    ).opts(
        fill_color = None,
        size = 9,
        line_color = 'black',
        line_width = 2,
        show_legend=True, 
    )    
    # get points where nwm output is nan
    nan_hover = HoverTool(
        tooltips = [('Gage ID', '@primary_location_id'),('Name', '@name')]
    )
    nwm_nan_points = gv.Points(
        dash_class.nan_gdf[['primary_location_id','geometry','name']].drop_duplicates(),
        vdims=['primary_location_id','name'],
        label=f'NWM produces NaN'
    ).opts(
        fill_color = 'red',
        size = 9,
        line_color = 'black',
        line_width = 2,
        show_legend=True, 
        xaxis = 'bare',
        yaxis = 'bare',
        tools=[nan_hover],
    )    

    # get threshold vs drainage area scatter
    df = dash_class.flow_points_gdf[
        ['drainage_area','hw_threshold','hw_threshold_norm',
         'primary_location_id','name']]
    thresh_scatter = hv.Scatter(
                df, 
                kdims=['drainage_area'], 
                vdims=['hw_threshold','hw_threshold_norm','primary_location_id','name']
            ).opts(
        color='hw_threshold_norm',
        cmap=cc.rainbow,
        clim=(0,0.1),
        width=map_width+70,
        height=map_height+50,
        toolbar = 'right',
        tools=['hover'],
        size = 6,
        xrotation = 45,
        title='Threshold v. Area',
        colorbar = True,
        clabel=f"Normalized Threshold {dash_class.norm_flow_unit_label}",
        xlabel=f"Drainage Area {dash_class.area_label}",
        ylabel=f"High water threshold {dash_class.flow_unit_label}"
    )

    layout = pn.Column( 
        pn.Row(
            pn.pane.HTML(f"High Water Threshold QA",
                         styles={
                             'color' : 'royalblue', 
                             'font-size': '20px', 
                             'font-weight': 'bold'
                         }, margin=(10,0,0,0)),
        ),
        pn.Row(
            pn.Spacer(width=60),
            (basemap * states * threshold_points).opts(toolbar=None),
            pn.Spacer(width=60),
            (basemap * states * threshold_norm_points * zero_thresh_points).opts(
                width=map_width+20,
                show_legend=True, 
                legend_position='bottom_left',
                legend_opts={
                    'background_fill_alpha': 0.2, 
                    'label_text_font_size': '9pt', 
                    'padding':0}
            ),
            #pn.Spacer(width=20),
            (basemap * states * zero_area_points * nwm_nan_points).opts(
                width=map_width-20,
                title="Area, NaN issues",
                show_legend=True, 
                legend_position='bottom_left',
                legend_opts={
                    'background_fill_alpha': 0.2, 
                    'label_text_font_size': '9pt', 
                    'padding':0}
            )
        ),  
        pn.Spacer(height=20),
        pn.Row(
            (thresh_scatter +
            thresh_scatter[:1e5,:4e5].opts(
                title = 'Threshold v. Area (outliers removed)'
            )).opts(
                shared_axes=False,
                toolbar = 'right'
            )
        )
    )
    
    return layout