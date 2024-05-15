'''
build data selector dashboard
'''

import datetime as dt
import panel as pn
import param
from .. import utils
from .. import config
from . import class_data

def build(
    dash_class: class_data.DataSelector_NWMOperational, 
    geo: config.Geo,
) -> pn.layout:

    source = pn.Param(
            dash_class,
            widgets={
                "forecast_config": {
                    "name" : "NWM Forecast Configuration (select one)"
                },
                "verify_config": {
                    "name" : "Analysis/Obs Source (Ctrl to select multiple)"
                },
            },                
            parameters=["forecast_config", "verify_config"],
            show_name=False,
            default_layout=pn.Column,
        )
    fcst_widget = source.widget('forecast_config')
    obs_widget = source.widget('verify_config')

    date_start = pn.widgets.DatePicker(
        name='First Reference/Issue Date to Load:', 
        value=dash_class.dates.ref_time_start.date(),
    )
    date_end = pn.widgets.DatePicker(
        name='Last Reference/Issue Date to Load:', 
        value=dash_class.dates.ref_time_end.date(),
    )
    var = pn.Param(
            dash_class,
            widgets={
                "variable": {"name" : "Variable (Ctrl to select both)"},
            },
            parameters=["variable"],
            show_name=False,
            default_layout=pn.Column,
        )
    options = pn.Param(
            dash_class,
            widgets={
                "reach_set": {"name" : "NWM Reach Set (for streamflow):"},
                "map_polygons": {"name" : "MAP Polygons (for precipitation)"},
                "overwrite_flag": {"name" : "Overwrite Existing Data"},
            },
            parameters=["reach_set", "map_polygons"],
            show_name=False,
            default_layout=pn.Column,
        )
    reach_widget = options.widget('reach_set')
    map_widget = options.widget('map_polygons')
    overwrite_widget = options.widget('overwrite_flag')
    
    forecast_selected_footnote1 = ' - Default dates are the first and '\
                                  'last reference/issue dates of forecasts '\
                                  'that overlap the event dates.'
    forecast_selected_footnote2 = ' - All timesteps of the selected '\
                                  'forecasts will be loaded.'
    forecast_selected_footnote3 = ' - Observed/analysis data for '\
                                  'corresponding valid dates will be loaded.'
    footnote4 = ''
    no_forecast_selected_footnote1 = '- Default dates are the analysis/obs '\
                                     'valid dates that overlap the '\
                                     'event dates (no forecasts are selected).'
    footnote_styles = {
        'font-size':'10pt', 
        'font-weight':'bold', 
        'color':'black'
    }
    warning_styles = {
        'font-size':'12pt', 
        'font-weight':'bold', 
        'color':'red'
    }
    
    footnote1 = pn.widgets.StaticText(
        value=forecast_selected_footnote1,
        styles=footnote_styles, margin=(0,0),
    )
    footnote2 = pn.widgets.StaticText(
        value=forecast_selected_footnote2,
        styles=footnote_styles, margin=(0,0),
    )
    footnote3 = pn.widgets.StaticText(
        value=forecast_selected_footnote3,
        styles=footnote_styles, margin=(0,0),
    )
    footnote4 = pn.widgets.StaticText(
        value=footnote4,
        styles=footnote_styles, margin=(10,10),
    )
    if reach_widget.value == 'all reaches':
        location_footnote_text = f" ({len(dash_class.event.nwm_id_list)} "\
                                 f"NWM reaches in selected region)"
    else:
        # if no usgs gages in the region, reset to 'all reaches'
        if len(dash_class.event.usgs_id_list) == 0:
            reach_widget.value = 'all reaches'
            location_footnote_text = f" ({len(dash_class.event.nwm_id_list)} "\
                                     f"NWM reaches in selected region)"
        else:
            location_footnote_text = f" ({len(dash_class.event.nwm_id_list)} "\
                                     f"NWM gaged reaches in selected region)"
    
    location_footnote = pn.widgets.StaticText(
        value=location_footnote_text,
        styles={'font-size':'10pt'},
        margin=(0,0,20,20),
    )
    if map_widget.value == 'HUC10':
        map_footnote_text = f" ({len(dash_class.event.huc10_list)} "\
                            f"HUC10 polygons in selected region)"
    else:
        map_footnote_text = f" ({len(dash_class.event.usgs_id_list)} "\
                            f"USGS basins in selected region)"    
    
    map_footnote = pn.widgets.StaticText(
        value=map_footnote_text,
        styles={'font-size':'10pt'}, 
        margin=(0,0,20,20),
    )    
    obs_footnote = pn.widgets.StaticText(
        value='*streamflow only',
        styles={'font-size':'10pt'}, 
        margin=(0,0,20,20),
    )  

    layout=pn.Column(
        pn.Spacer(height=40),
        pn.pane.Markdown(f"## Select data to load for event \
                         ```{dash_class.event.dir_name}``` and event dates: \
                         ```{dash_class.dates.event_value_time_start.date()}```\
                         to \
                         ```{dash_class.dates.event_value_time_end.date()}```."),
        pn.pane.Markdown(f"### Event Dates: "\
                         f"{dash_class.dates.event_value_time_start.date()} "\
                         f"to {dash_class.dates.event_value_time_end.date()}"), 
        pn.pane.Markdown("### Source Selections:"), 
        pn.Row(
            pn.Column(
                fcst_widget, 
                obs_widget, 
                obs_footnote
            ), 
            var, 
            pn.Column(
                reach_widget, 
                location_footnote, 
                map_widget, 
                map_footnote)
        ),
        pn.pane.Markdown(f"### Dates of Data to Load:"),
        pn.Row(date_start, date_end, overwrite_widget),
        pn.Spacer(height=20),
        footnote1,footnote2,footnote3,footnote4,
        pn.Spacer(height=100),
        pn.pane.Markdown("#### ------ (blank space added so date selectors "\
                        "are visible without scrolling the cell) ------")
    )   
    
    @pn.depends(fcst_widget.param.value, obs_widget.param.value, watch=True)
    def update_footnote_text(
        fcst_widget, 
        obs_widget
    ):
        
        dash_class.dates.initialize_dates(
            dash_class.paths, 
            dash_class.event, 
            forecast_config=fcst_widget
        )

        if fcst_widget != 'none':
            date_start.value = dash_class.dates.ref_time_start.date()
            date_start.name = 'First Reference/Issue Date to Load:'
            date_end.value = dash_class.dates.ref_time_end.date()
            date_end.name = 'Last Reference/Issue Date to Load:'   
            footnote1.value = forecast_selected_footnote1
            footnote2.value = forecast_selected_footnote2
            if obs_widget == ['none']:
                footnote3.value = ''
            else:
                footnote3.value = forecast_selected_footnote3
            footnote1.styles = footnote_styles
            footnote2.styles = footnote_styles
            footnote3.styles = footnote_styles
        elif fcst_widget == 'none' and obs_widget != ['none']:    
            date_start.value = dash_class.dates.event_value_time_start.date()
            date_start.name = 'First Valid Date to Load:'
            date_end.value = dash_class.dates.event_value_time_end.date()
            date_end.name = 'Last Valid Date to Load:'
            footnote1.value = no_forecast_selected_footnote1
            footnote2.value = ''
            footnote3.value = ''
            footnote1.styles = footnote_styles
        else:
            footnote1.value = '!!Forecast and observed selections cannot '\
                              'both be \'none\', choose at least one '\
                              'configuration to load'
            footnote2.value = ''
            footnote3.value = ''
            footnote1.styles = warning_styles
  
    @pn.depends(reach_widget.param.value, watch=True)
    def update_reach_count(reach_widget):
        if reach_widget == 'all reaches':
            dash_class.event.nwm_id_list = utils.locations.get_nwm_id_list_as_int(
                dash_class.paths, 
                geo, 
                nwm_version = dash_class.event.nwm_version, 
                huc10_list = dash_class.event.huc10_list
            )
            location_footnote.value = f" ({len(dash_class.event.nwm_id_list)} "\
                                      f"NWM reaches in selected region)"
        else:
            dash_class.event.nwm_id_list = utils.locations.nwm.get_nwm_id_list_as_int(
                dash_class.paths, 
                geo, 
                nwm_version = dash_class.event.nwm_version, 
                usgs_id_list = dash_class.event.usgs_id_list
            )
            location_footnote.value = f" ({len(dash_class.event.nwm_id_list)} "\
                                      f"NWM gaged reaches in selected region)"
            
    @pn.depends(map_widget.param.value, watch=True)
    def update_map_count(map_widget):
        if map_widget == 'HUC10':
            map_footnote.value = f" ({len(dash_class.event.huc10_list)} "\
                                 f"HUC10 polygons in selected region)"
        else:
            map_footnote.value = f" ({len(dash_class.event.usgs_id_list)} "\
                                 f"USGS basins in selected region)"

    @pn.depends(date_start.param.value, date_end.param.value, watch=True)
    def update_dates(
        date_start, 
        date_end
    ):
    
        now = dt.datetime.utcnow().replace(
            second=0, 
            microsecond=0, 
            minute=0, 
            hour=0
        )
        
        if date_start > date_end:
            footnote1.value = '!! WARNING - Invalid dates. '\
                              'Start date must be before end date. '\
                              'Choose different dates.'
            footnote2.value = ''
            footnote3.value = ''
            footnote1.styles = warning_styles
        elif date_start > now.date():
            footnote1.value = '!! WARNING - Invalid dates. '\
                              'Start date cannot be in the future. '\
                              'Choose different dates.'
            footnote2.value = ''
            footnote3.value = ''
            footnote1.styles = warning_styles
        else:                
            if fcst_widget.value != 'none':                
                footnote1.value = forecast_selected_footnote1
                footnote2.value = forecast_selected_footnote2
                if obs_widget == ['none']:
                    footnote3.value = ''
                else:
                    footnote3.value = forecast_selected_footnote3
                footnote1.styles = footnote_styles
                footnote2.styles = footnote_styles
                footnote3.styles = footnote_styles
                dash_class.dates.ref_time_start = dt.datetime.combine(
                    date_start, 
                    dt.time(hour=0)
                )
                dash_class.dates.ref_time_end = dt.datetime.combine(
                    date_end, 
                    dt.time(hour=23)
                )
                dash_class.dates.get_data_value_times()

            elif fcst_widget.value == 'none' and obs_widget.value != ['none']:
                footnote1.value = no_forecast_selected_footnote1
                footnote2.value = ''
                footnote3.value = ''
                footnote1.styles = footnote_styles 
                dash_class.dates.data_value_time_start = dt.datetime.combine(
                    date_start, 
                    dt.time(hour=0)
                )
                dash_class.dates.data_value_time_end = dt.datetime.combine(
                    date_end, 
                    dt.time(hour=23)
                )
                
            if date_end > now.date():
                footnote4.value = '!! NOTE - End Date is in the future. '\
                                  'Any missing or future datetimes will be '\
                                  'ignored and will not crash the loading process.'
                footnote4.styles = warning_styles
            else:
                footnote4.value = ''

        if dash_class.dates.data_value_time_end > now:
            dash_class.dates.data_value_time_end = now 
            
    return layout