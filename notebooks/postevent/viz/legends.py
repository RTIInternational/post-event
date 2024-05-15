'''
Custom legends due to limitations/issues with holoviews legends
'''

import holoviews as hv

def get_precip_timeseries_legend() -> hv.Overlay:
    '''

    '''
    opts = dict(
        fontscale=0.5, 
        xlim=(-0.2,5),
        ylim=(-1,4), 
        toolbar=None, 
        height=130, 
        width=240, 
        xaxis=None, 
        yaxis=None
    )
    
    text_opts = dict(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    
    all_fcsts=hv.Curve([(0.2,3),(.7,3)]).opts(
        **opts, 
        color='gray', 
        line_width=1
    )
    curr_fcst=hv.Curve([(0.2,2),(.7,2)]).opts(
        **opts, 
        color='red', 
        line_width=2
    )    
    ana=hv.Curve([(0.2,1),(.7,1)]).opts(
        **opts, 
        color='blue', 
        line_width=2
    )
    window=hv.Rectangles((0.2,-0.5,0.7,0.3)).opts(
        color='gray', 
        alpha=0.3
    )
    text_fcsts=hv.Text(1,3,'All Forecasts').opts(
        **opts, 
        **text_opts
    )
    text_curr=hv.Text(1,2,'Selected Forecast').opts(
        **opts, 
        **text_opts
    )    
    text_ana=hv.Text(1,1,'Analysis').opts(
        **opts, 
        **text_opts
    )
    text_window=hv.Text(1,0,'Selected Forecast Window').opts(
        **opts, 
        **text_opts
    )

    return all_fcsts * curr_fcst * ana * window * \
           text_fcsts * text_curr * text_ana * text_window

def get_streamflow_timeseries_legend() -> hv.Overlay:
    '''

    '''
    opts = dict(
        fontscale=0.5, 
        xlim=(-0.2,5),
        ylim=(-1,6), 
        toolbar=None, 
        height=250, 
        width=240, 
        xaxis=None, 
        yaxis=None
    )
    text_opts = dict(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )

    all_fcsts=hv.Curve([(0.2,5),(.7,5)]).opts(
        **opts, 
        color='gray', 
        line_width=1
    )
    curr_fcst=hv.Curve([(0.2,4),(.7,4)]).opts(
        **opts, 
        color='red', 
        line_width=2
    )    
    open_loop_ana=hv.Curve([(0.2,3),(.7,3)]).opts(
        **opts, 
        color='blue', 
        line_width=2
    ) 
    obs=hv.Curve([(0.2,2),(.5,2)]).opts(
        **opts, 
        color='black', 
        line_dash='dashed', 
        line_width=2
    )
    hw_thresh=hv.Curve([(0.2,1),(.7,1)]).opts(
        **opts, 
        color='darkturquoise', 
        line_dash='dotted', 
        line_width=2
    )
    window=hv.Rectangles((0.2,-0.5,0.7,0.3)).opts(
        color='gray', 
        alpha=0.3
    )

    text_fcsts=hv.Text(1,5,'All Forecasts').opts(
        **opts, 
        **text_opts
    )
    text_curr=hv.Text(1,4,'Selected Forecast').opts(
        **opts, 
        **text_opts
    )    
    text_open_loop_ana=hv.Text(1,3,'Open Loop Analysis').opts(
        **opts, 
        **text_opts
    )
    text_obs=hv.Text(1,2,'Observed').opts(
        **opts, 
        **text_opts
    )
    text_hw_thresh=hv.Text(1,1,'High Water Threshold').opts(
        **opts, 
        **text_opts
    )
    text_window=hv.Text(1,0,'Selected Forecast Window').opts(
        **opts, 
        **text_opts
    )

    return all_fcsts * curr_fcst * open_loop_ana * \
           obs * hw_thresh * window * \
           text_fcsts * text_curr * text_open_loop_ana * \
           text_obs * text_hw_thresh * text_window

def get_streamflow_timeseries_summary_legend() -> hv.Overlay:
    '''

    '''
    opts = dict(
        fontscale=0.5, 
        xlim=(-0.4,8.4),
        ylim=(0,3), 
        toolbar=None, 
        height=80, 
        width=420, 
        xaxis=None, 
        yaxis=None
    )
    text_opts = dict(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    all_fcsts=hv.Curve([(0,2),(.7,2)]).opts(
        **opts, 
        color='darkorange', 
        line_width=1
    )
    open_loop_ana=hv.Curve([(0,1),(.7,1)]).opts(
        **opts, 
        color='black', 
        line_dash='dashed', 
        line_width=2
    ) 
    text_fcsts=hv.Text(1,2,'All Forecasts').opts(
        **opts, 
        **text_opts
    )
    text_open_loop_ana=hv.Text(1,1,'Open Loop Analysis').opts(
        **opts, 
        **text_opts
    )
    
    obs=hv.Curve([(4.2,2),(4.9,2)]).opts(
        **opts, 
        color='blue', 
        line_width=2
    )
    hw_thresh=hv.Curve([(4.2,1),(4.9,1)]).opts(
        **opts, 
        color='dimgray', 
        line_dash='dotted', 
        line_width=2
    )
    text_obs=hv.Text(5.2,2,'Observed').opts(
        **opts, 
        **text_opts
    )
    text_hw_thresh=hv.Text(5.2,1,'High Water Threshold').opts(
        **opts, 
        **text_opts
    )

    return all_fcsts * open_loop_ana * obs * hw_thresh *  \
           text_fcsts * text_open_loop_ana * text_obs * text_hw_thresh 

def get_streamflow_map_legend() -> hv.Overlay:
    '''

    '''
    opts = dict(
        fontscale=0.5, 
        xlim=(-0.2,5),
        ylim=(-1,3), 
        toolbar=None, 
        height=150, 
        width=240, 
        xaxis=None, 
        yaxis=None
    )
    text_opts = dict(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    exceeds = hv.Scatter([(0.2,2)]).opts(
        **opts, 
        fill_color='none',
        line_color='black', 
        line_width=1.5, 
        size=7
    )
    selected_pt = hv.Scatter([(0.2,1)]).opts(
        **opts, 
        fill_color='none', 
        line_color='fuchsia', 
        line_width=2, 
        size=7
    )
    basin = hv.Rectangles((0.1,-0.5,0.3,0.3)).opts(
        fill_color='none', 
        line_color = 'dimgray', 
        line_width=1
    )
    text_exceeds = hv.Text(0.5,2,'Exceeds High Water Threshold').opts(
        **opts, 
        **text_opts
    )
    text_selected = hv.Text(0.5,1,'Selected Point').opts(
        **opts, 
        **text_opts
    )
    text_basin = hv.Text(0.5,0,'Upstream Basin (if available)').opts(
        **opts, 
        **text_opts
    )

    return exceeds * selected_pt * basin * \
           text_exceeds * text_selected * text_basin

def get_combined_dashboard_legend() -> hv.Overlay:
    '''

    '''
    opts = dict(
        fontscale=0.5, 
        xlim=(-0.2,5),
        ylim=(-1,12), 
        toolbar=None, 
        height=300, 
        width=280, 
        xaxis=None, 
        yaxis=None
    )
    text_opts = dict(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    selected_pt=  hv.Scatter([(0.5,10)]).opts(
        **opts, 
        fill_color='none', 
        line_color='fuchsia', 
        line_width=3, 
        size=10
    )
    exceeds = hv.Scatter([(0.5,9)]).opts(
        **opts, 
        fill_color='none',
        line_color='black', 
        line_width=2, 
        size=10
    )
    basin = hv.Rectangles((0.2,7.5,0.7,8.3)).opts(
        fill_color='none', 
        line_color='dimgray', 
        line_width=1
    )
    
    all_fcsts = hv.Curve([(0.2,5),(.7,5)]).opts(
        **opts, 
        color='gray', 
        line_width=1
    )
    curr_fcst = hv.Curve([(0.2,4),(.7,4)]).opts(
        **opts, 
        color='red', 
        line_width=2
    )    
    open_loop_ana = hv.Curve([(0.2,3),(.7,3)]).opts(
        **opts, 
        color='blue', 
        line_width=2
    ) 
    obs = hv.Curve([(0.2,2),(.7,2)]).opts(
        **opts, 
        color='black', 
        line_dash='dashed', 
        line_width=2
    )
    hw_thresh = hv.Curve([(0.2,1),(.7,1)]).opts(
        **opts, 
        color='darkturquoise', 
        line_dash='dotted',
        line_width=2
    )
    window = hv.Rectangles((0.2,-0.5,0.7,0.3)).opts(
        color='gray', 
        alpha=0.3
    )
    
    text_maps = hv.Text(0.2,11,'Map Legend:').opts(
        **opts, 
        **text_opts, 
        text_font_style='bold'
    )
    text_selected = hv.Text(1,10,'Selected Gage').opts(
        **opts, 
        **text_opts
    )
    text_exceeds = hv.Text(1,9,'Exceeds High Water Threshold').opts(
        **opts, 
        **text_opts
    )
    text_basin = hv.Text(1,8,'Upstream Basin (if available)').opts(
        **opts, 
        **text_opts
    )
    
    text_ts = hv.Text(0.2,6,'Timeseries Legend:').opts(
        **opts, 
        **text_opts,
        text_font_style='bold'
    )
    text_fcsts = hv.Text(1,5,'All Forecasts').opts(
        **opts, 
        **text_opts
    )
    text_curr = hv.Text(1,4,'Selected Forecast').opts(
        **opts, 
        **text_opts
    )    
    text_open_loop_ana = hv.Text(1,3,'Open Loop Analysis').opts(
        **opts, 
        **text_opts
    )
    text_obs = hv.Text(1,2,'Observed').opts(
        **opts, 
        **text_opts
    )
    text_hw_thresh = hv.Text(1,1,'High Water Threshold').opts(
        **opts, 
        **text_opts
    )
    text_window = hv.Text(1,0,'Selected Forecast Window').opts(
        **opts, 
        **text_opts
    )

    return  text_maps * exceeds * selected_pt * basin * \
            text_exceeds * text_selected * text_basin * \
            text_ts * all_fcsts * curr_fcst * open_loop_ana * \
            obs * hw_thresh * window * \
            text_fcsts * text_curr * text_open_loop_ana * \
            text_obs * text_hw_thresh * text_window

def get_extents_map_legend() -> hv.Overlay:
    '''

    '''
    opts = dict(
        xlim=(-0.2,5),
        ylim=(-1,3), 
        toolbar=None, 
        height=110, 
        width=450, 
        xaxis=None, 
        yaxis=None
    )
    text_opts = dict(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    
    huc2=hv.Curve([(0,2),(.5,2)]).opts(
        **opts, 
        color='red', 
        line_width=2
    )
    event_poly=hv.Curve([(0,1),(.5,1)]).opts(
        **opts, 
        line_color='darkorange', 
        line_dash='dashed', 
        line_width=3
    )
    zoom_box=hv.Curve([(0,0),(.5,0)]).opts(
        **opts, 
        line_color='darkgreen', 
        line_dash='solid', 
        line_width=5
    )
    
    text_huc2=hv.Text(0.7,2,'Event HUC2s '\
                            '(if any, defined at loading stage)').opts(
        **text_opts
    )
    text_event_poly=hv.Text(0.7,1,'Event Region Polygon '\
                                  '(if any, defined at loading stage)').opts(
        **text_opts
    )
    text_zoom_box=hv.Text(0.7,0,'Zoom Region (draw in map if desired)').opts(
        **text_opts
    )
    
    return huc2 * event_poly * zoom_box * \
           text_huc2 * text_event_poly * text_zoom_box   

def get_event_selector_legend() -> hv.Overlay:
    '''
    separate legend explaining layers in the region map 
    (for more control than possible in holoviews automatic legends)
    '''
    
    huc2=hv.Curve([(0,3),(.5,3)]).opts(
        fontscale=0.5, 
        xlim=(-0.2,3),
        ylim=(-1,4), 
        toolbar=None, 
        height=120, 
        width=300, 
        color='black', 
        line_width=1, 
        xaxis=None, 
        yaxis=None
    )
    prior_huc2=hv.Curve([(0,2),(.5,2)]).opts(
        color='red', 
        line_width=2
    )
    current_huc2=hv.Curve([(0,1),(.5,1)]).opts(
        color='black', 
        line_width=2
    )
    latlon_box=hv.Curve([(0,0),(.5,0)]).opts(
        color='orange', 
        line_color='darkorange', 
        line_dash='dashed', 
        line_width=3
    )
    text_huc2=hv.Text(0.7,3,'All HUC2s').opts(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    text_sel_huc2=hv.Text(0.7,2,'Prior-selected HUC2s (if any)').opts(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    text_curr_huc2=hv.Text(0.7,1,'Newly-selected HUC2s (if any)').opts(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    text_latlon=hv.Text(0.7,0,'Polygon bounday (if any)').opts(
        color='black', 
        text_align='left', 
        text_font_size='10pt'
    )
    
    return huc2 * prior_huc2 * current_huc2 * latlon_box * \
           text_huc2 * text_sel_huc2 * text_curr_huc2 * text_latlon    


