'''
building blocks for dashboard to select space/time extents 
(full or subsets of event extents) for evaluation dashboards
'''
import panel as pn
import param
import datetime as dt
import holoviews as hv
hv.extension('bokeh', logo=False)

from pathlib import Path
from typing import List, Union
from bokeh.models import HoverTool

from . import class_extents
from . import common
from . import legends
from .. import utils

def build(
    dash_class: class_extents.Extents
) -> pn.layout:
    '''
    build and combine panel components
    ''' 
    ref_date_start = pn.widgets.DatePicker(
        name='First Reference/Issue Date to Evaluate:', 
        value=dash_class.dates.ref_time_start.date(),
    )
    ref_date_end = pn.widgets.DatePicker(
        name='Last Reference/Issue Date to Evaluate:', 
        value=dash_class.dates.ref_time_end.date(),
    )
    ref_dates_text = pn.widgets.StaticText(
        value='   (Select a subset of forecast dates if desired)',
        styles={'font-size':'10pt', 'color':'black'}, 
        margin=(0,10),
    ) 

    map_width = 400
    map_height = 350
    basemap = common.get_basemap(dash_class.geo).opts(
                height=map_height, 
                width=map_width,   
            )
    huc2s = dash_class.get_selected_huc2s()
    event_poly = dash_class.get_event_polygon()
    zoom_poly = dash_class.get_zoom_polygon()
    dash_class.zoom_stream = hv.streams.PolyDraw(
        source=zoom_poly, 
        vertex_style={'color': 'darkgreen'}, 
        num_objects=1
    )

    layout =  pn.Column(
        pn.Spacer(height=10),
        f"# Reviewing \
            ```{dash_class.dates.forecast_config}``` forecasts for event: \
            ```{dash_class.event.dir_name}``` and event dates: \
            ```{dash_class.event.event_start_date}``` to \
            ```{dash_class.event.event_end_date}``` ",
        pn.Row(
            pn.Column(
                "## Visualization time/space extent options:",
                ref_date_start, 
                ref_date_end,
                ref_dates_text
            ),
            pn.Column(
                (basemap * huc2s * event_poly * zoom_poly).opts(
                    labelled=[]
                )
            ),
            pn.Column(
                legends.get_extents_map_legend(),
            )
        )
    )
    @pn.depends(ref_date_start.param.value, 
                ref_date_end.param.value, 
                watch=True
               )
    def validate_dates(
        ref_date_start, 
        ref_date_end
    ):
    
        now = dt.datetime.utcnow().replace(
            second=0, 
            microsecond=0, 
            minute=0, 
            hour=0
        )
        
        if ref_date_start > ref_date_end:
            print("!! WARNING - Invalid dates.  Reference start date must \
                   be before reference end date.  Choose different dates.")
        elif ref_date_start > now.date():
            print("!! WARNING - Invalid dates.  Reference start date cannot \
                  be in the future.  Choose different dates.")
        else:                
            dash_class.dates.ref_time_start = dt.datetime.combine(
                ref_date_start, 
                dt.time(hour=0)
            )
            if dash_class.paths.forecast_config in [
                'medium_range','medium_range_mem1']:
                dash_class.dates.ref_time_end = dt.datetime.combine(
                    ref_date_end, 
                    dt.time(hour=18)
                )
            elif dash_class.paths.forecast_config in ['short_range']:
                dash_class.dates.ref_time_end = dt.datetime.combine(
                    ref_date_end, 
                    dt.time(hour=23)
                )   
                
            if ref_date_end > now.date():
                print('!! Warning - End Date is in the future. \
                       Any missing or future datetimes will be \
                       ignored and will not crash the loading process.')

            [value_time_start, value_time_end] = utils.nwm.get_value_times_for_ref_time_range(
                forecast_config = dash_class.paths.forecast_config, 
                ref_time_start = dash_class.dates.ref_time_start, 
                ref_time_end = dash_class.dates.ref_time_end
            )
            dash_class.dates.data_value_time_start = value_time_start
            dash_class.dates.data_value_time_end = value_time_end

        if dash_class.dates.data_value_time_end > now:
            dash_class.dates.data_value_time_end = now 
            
    return layout            
            

