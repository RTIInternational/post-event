'''
build event selector dashboard
''' 

import datetime as dt
import panel as pn
import holoviews as hv
import param

from . import class_event

def build(
    dash_class: class_event.EventSelector,
) -> pn.layout:

    dates_view = pn.Param(
        dash_class,
        widgets={
            "dir_name" : {
                "type": pn.widgets.TextInput, 
                "name" : "Event Name (YYYYMM_name):", 
                "value": dash_class.dir_name
            },
            "event_start_date": {
                "type": pn.widgets.DatePicker, 
                "name" : "Event Start Date:", 
                "value" : dash_class.event_start_date
            },
            "event_end_date": {
                "type": pn.widgets.DatePicker, 
                "name" : "Event End Date:", 
                "value" : dash_class.event_end_date
            },
        },
        parameters=["dir_name", "event_start_date", "event_end_date"],
        show_name=False,
        default_layout=pn.Row,
        width=600
    )
    button_view = pn.Param(
        dash_class,
        widgets={
            "button" : {
                "type": pn.widgets.Button, 
                "name" : "Update event definitions file", 
                "button_type":"primary"
            },
        },
        parameters=["button"],
        show_name=False,
        width=200
    )
    huc2_instructions = "Use Tap Tool to select HUC2 polygons "\
                        "(hold shift to select multiple)"
    draw_instructions = "Use Polygon Draw Tool to draw region boundary "\
                         "(double click to start and end)"
    styles = {'font-size':'12pt', 'font-weight':'bold', 'color':'black'}
    region_view = pn.Row(
        pn.Column(
            pn.pane.Markdown("### Event Region*:"), 
            pn.Row(pn.Spacer(width=10),
                   pn.pane.Markdown(
                       "### *Analysis region will be the intersection "\
                       "of selected HUC2s (if any) and lat/lon limits",
                       width=250)),                   
            get_event_selector_legend(), 
            pn.Spacer(height=30),
            button_view,
        ),
        pn.Column(
            pn.Spacer(height=20),
            pn.widgets.StaticText(
                value=huc2_instructions,
                margin=(0,0,0,50),
                styles=styles
            ),
            pn.widgets.StaticText(
                value=draw_instructions,
                margin=(0,0,0,50),
                styles=styles
            ),
            pn.panel(
                dash_class.region.map_overlay, 
                margin=0
            )
        ),
        sizing_mode='stretch_width'
    )
    return pn.Column(
                pn.Spacer(height=50),
                pn.pane.Markdown(
                    f"## Define (or update) event name, dates, "\
                    f"and region; then click the ```Update/Store "\
                    f"Event Specs``` button."),
                pn.pane.Markdown("### Event Name and Dates:"), 
                dates_view, 
                region_view,
                sizing_mode='stretch_width',
            )

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
    
    return huc2*prior_huc2*current_huc2*latlon_box* \
           text_huc2*text_sel_huc2*text_curr_huc2*text_latlon   




