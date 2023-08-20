
import teehr.queries.duckdb as tqd
import teehr.queries.utils as tqu
import duckdb as ddb
import pandas as pd
import spatialpandas as spd
import panel as pn
import geopandas as gpd
import numpy as np
import datetime as dt
from typing import List, Union
from pathlib import Path
import json
import holoviews as hv
import geoviews as gv
import cartopy.crs as ccrs
import colorcet as cc
import datashader as ds
from bokeh.models import Range1d, LinearAxis, DatetimeTickFormatter
from shapely.geometry import Polygon, MultiPoint

'''
misc utilities for TEEHR dashboards
''' 
    
def run_teehr_query(
    query_type: str,
    scenario: dict,
    location_id: Union[str, List[str], None] = None,    
    huc_id: Union[str, List[str], None] = None,
    order_limit: Union[int, None] = None,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None,
    include_metrics: Union[List[str], None] = None,
    group_by: Union[List[str], None] = None,
    order_by: Union[List[str], None] = None,
    attribute_paths: Union[List[Path], None] = None,
    return_query: Union[bool, None] = False,
    include_geometry: Union[bool, None] = None,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    
    #print('running query, huc_id=', huc_id)
    
    primary_filepath=scenario["primary_filepath"]
    secondary_filepath=scenario["secondary_filepath"]
    crosswalk_filepath=scenario["crosswalk_filepath"]
    
    if "geometry_filepath" in scenario.keys():
        geometry_filepath=scenario["geometry_filepath"]
    else:
        geometry_filepath=None
        
    if include_geometry is None:
        if geometry_filepath is None:
            include_geometry = False
        else:
            include_geometry = True
        
    # initialize group_by and order_by lists
    if group_by is None:
        group_by=["primary_location_id","measurement_unit"]
    else:
        group_by.extend(["primary_location_id","measurement_unit"])
        group_by = list(set(group_by))
        
    if order_by is None:
        if query_type == "timeseries":
            order_by=["primary_location_id", "reference_time", "value_time"]
        else:
            order_by=["primary_location_id"]

    # build the filters
    filters=[]    
    
    # region (HUC) id
    if huc_id is not None:
        if huc_id != 'all':
            huc_level = len(huc_id)

            # if usgs, get the crosswalk (for now huc level must be 10 or smaller)
            if primary_filepath.parent.name == 'usgs':
                location_list = get_usgs_locations_within_huc(huc_level, huc_id, attribute_paths)
                # print('len loc list: ', len(location_list))
                # print(location_list[220:230])
                filters.append(
                    {
                        "column": "primary_location_id",
                        "operator": "in",
                        "value": location_list
                    }
                )
            elif primary_filepath.parent.name in ['analysis_assim','analysis_assim_extend']:
                location_list = get_nwm_locations_within_huc(huc_level, huc_id, attribute_paths)
                filters.append(
                    {
                        "column": "primary_location_id",
                        "operator": "in",
                        "value": location_list
                    } 
                )
            elif primary_filepath.parent.name in ['forcing_analysis_assim','forcing_analysis_assim_extend']:
                    filters.append(
                        {
                            "column": "primary_location_id",
                            "operator": "like",
                            "value": f"huc10-{huc_id}%"
                        }
                    )

    # location id (not using in current notebook example, using huc_id instead, leaving in for future use)
    if location_id is not None:    
        if type(location_id) is str:
            filters.append(
                {
                    "column": "primary_location_id",
                    "operator": "like",
                    "value": f"{location_id}%"
                }
            )
        elif type(location_id) is list:
            filters.append(
                {
                    "column": "primary_location_id",
                    "operator": "in",
                    "value": location_id
                }
            )       
            
    # stream order
    if order_limit is not None:
        if type(order_limit) is int:
            location_list = get_usgs_locations_below_order_limit(order_limit, attribute_paths)
            filters.append(
                {
                    "column": "primary_location_id",
                    "operator": "in",
                    "value": location_list
                }  
            )
         
    # reference times
    if reference_time_single is not None:
        filters.append(
            {
                "column": "reference_time",
                "operator": "=",
                "value": f"{reference_time_single}"
            }
        )
    elif reference_time_start is not None:
        filters.append(
            {
                "column": "reference_time",
                "operator": ">=",
                "value": f"{reference_time_start}"
            }
        )
        if reference_time_end is not None:
            filters.append(
                {
                    "column": "reference_time",
                    "operator": "<=",
                    "value": f"{reference_time_end}"
                } 
            )
        else:             
            print('set up an error catch here')
        
    # value times
    if value_time_start is not None:
        filters.append(
            {
                "column": "value_time",
                "operator": ">=",
                "value": f"{value_time_start}"
            }
        )
        if value_time_end is not None:
            filters.append(
                {
                    "column": "value_time",
                    "operator": "<=",
                    "value": f"{value_time_end}"
                } 
            )
        else:             
            print('set up an error catch here')        

    if value_min is not None:  
        if type(value_min) is str:
            value_min = float(value_min.split(' ')[0])       
        filters.extend(
            [{
                "column": "primary_value",
                "operator": ">=",
                "value": value_min
            },
            {
                "column": "secondary_value",
                "operator": ">=",
                "value": value_min
            }]
        )
            
    if value_max is not None:
        if type(value_min) is str:
            value_min = float(value_min.split(' ')[0])  
        filters.extend(
            [{
                "column": "primary_value",
                "operator": "<=",
                "value": value_max
            },
            {
                "column": "secondary_value",
                "operator": "<=",
                "value": value_max
            }]
        )
        
    if query_type == "metrics":

        if include_metrics is None:
            include_metrics = "all"

        # get metrics
        gdf = tqd.get_metrics(
            primary_filepath,
            secondary_filepath,
            crosswalk_filepath,        
            group_by=group_by,
            order_by=order_by,
            filters=filters,
            return_query=return_query,
            geometry_filepath=geometry_filepath,       
            include_geometry=include_geometry,
            include_metrics=include_metrics,
        )    
        
    else:
        # get timeseries
        gdf = tqd.get_joined_timeseries(
            primary_filepath,
            secondary_filepath,
            crosswalk_filepath,
            order_by=order_by,
            filters=filters,
            return_query=return_query,
            geometry_filepath=geometry_filepath,       
            include_geometry=include_geometry,
        )     
        
    gdf = gdf.sort_values(order_by)

    return gdf


################################## get info about the parquet files


def get_parquet_date_range_across_scenarios(
    pathlist: List[Path] = [],
    date_type: str = "value_time",
) -> List[pd.Timestamp]:
        
    print(f"Checking {date_type} range in the parquet files")    
      
    for source in pathlist:
        
        if date_type == "reference_time":
            [source_start_date, source_end_date] = get_parquet_reference_time_range(source)
        elif date_type == "value_time":
            [source_start_date, source_end_date] = get_parquet_value_time_range(source)            
        # elif date_type == "lead_time":
        #     [source_start_date, source_end_date] = get_parquet_lead_time_range(source) 
        else:
            print('add error catch')
            
        if source == pathlist[0]:
            start_date = source_start_date
            end_date = source_end_date
        else:
            if type(source_start_date) is pd.Timestamp and type(start_date) is pd.Timestamp:
                if source_start_date > start_date:
                    start_date = source_start_date
            elif type(source_start_date) is pd.Timestamp and type(start_date) != pd.Timestamp:
                start_date = source_start_date
                
            if type(source_end_date) is pd.Timestamp and type(end_date) is pd.Timestamp:                    
                if source_end_date < end_date:
                    end_date = source_end_date
            elif type(source_end_date) is pd.Timestamp and type(end_date) is not pd.Timestamp:
                end_date = source_end_date
                
    return [start_date, end_date]


def get_parquet_date_list_across_scenarios(
    pathlist: List[Path] = [],
    date_type: str = "value_time",
) -> List[pd.Timestamp]:
        
    print(f"Getting list of {date_type}s in the parquet files")   
    
    date_list = []
    for source in pathlist:

        if date_type == "reference_time":
            source_times = get_parquet_reference_time_list(source)
        elif date_type == "value_time":
            source_times = get_parquet_value_time_list(source)            
        else:
            print('add error catch')

        if source_times is not None:
            if len(date_list) == 0:
                date_list = source_times
            else:
                date_list = sorted(list(set(date_list).intersection(source_times)))
    return date_list


def get_parquet_value_time_range(source) -> List[pd.Timestamp]:
    '''
    Query parquet files for defined fileset (source directory) and
    return the min/max value_times in the files
    '''    
    query = f"""
        SELECT count(distinct(value_time)) as count,
        min(value_time) as start_time,
        max(value_time) as end_time
        FROM read_parquet('{source}')
    ;"""
    df = ddb.query(query).to_df()
    return [df.start_time[0], df.end_time[0]]


def get_parquet_reference_time_range(source: Path) -> List[pd.Timestamp]:
    '''
    Query parquet files for defined fileset (source directory) and
    return the min/max reference times in the files
    '''    
    query = f"""
        SELECT count(distinct(reference_time)) as count,
        min(reference_time) as start_time,
        max(reference_time) as end_time
        FROM read_parquet('{source}')
    ;"""
    df = ddb.query(query).to_df()
    return [df.start_time[0], df.end_time[0]] 


def get_parquet_reference_time_list(
    source: Path
) -> List[pd.Timestamp]:
    
    query = f"""select distinct(reference_time)as time
        from '{source}'
        order by reference_time asc"""
    df = ddb.query(query).to_df()
    if any(df.time.notnull()):
        return df.time.tolist()
    else:
        return None
    
def get_parquet_value_time_list(
    source: Path
) -> List[pd.Timestamp]:
    
    query = f"""select distinct(value_time)as time
        from '{source}'
        order by value_time asc"""
    df = ddb.query(query).to_df()
    if any(df.time.notnull()):
        return df.time.tolist()
    else:
        return None

########### get info from inputs - location subset and scenario stuff


def get_usgs_locations_within_huc(
    huc_level: int = 2,
    huc_id: str = '01', 
    attribute_paths: dict = {},
) -> List[str]:
    
    location_list=[]
    if "usgs_huc_crosswalk" in attribute_paths.keys():
        df = pd.read_parquet(attribute_paths["usgs_huc_crosswalk"])
        df['huc'] = df['secondary_location_id'].str[6:6+huc_level]
        
        if huc_id != 'all':
            location_list = df[df['huc']==huc_id]['primary_location_id'].to_list()
        else:
            location_list = df['primary_location_id'].to_list()
        
        if not location_list:
           print(f"Warning, no usgs locations found in {huc_id}")     
    else:
        print("Warning, no usgs-huc crosswalk found, location list empty")
        
    return location_list


def get_nwm_locations_within_huc(
    huc_level: int = 2,
    huc_id: str = '01', 
    attribute_paths: dict = {},
    version: float = 2.2,
) -> List[str]:
    
    nwm_version = 'nwm' + str(version).replace('.','')
    crosswalk_name = "_".join([nwm_version, 'huc','crosswalk'])
    
    location_list=[]
    if crosswalk_name in attribute_paths.keys():
        df = pd.read_parquet(attribute_paths[crosswalk_name])
        df['huc'] = df['secondary_location_id'].str[6:6+huc_level]
        location_list = df[df['huc']==huc_id]['primary_location_id'].to_list()
        if not location_list:
           print(f"Warning, no nwm reaches locations found in {huc_id}")     
    else:
        print(f"Warning, no {crosswalk_name} found, location list empty")
        
    return location_list

def get_usgs_locations_below_order_limit(
    order_limit: int = 10,
    attribute_paths: dict = {},
) -> List[str]:
    
    location_list=[]
    if "usgs_stream_order" in attribute_paths.keys():
        df = pd.read_parquet(attribute_paths["usgs_stream_order"])
        location_list = df[df['attribute_value']<=order_limit]['location_id'].to_list()
        if not location_list:
           print(f"Warning, no usgs locations found with stream order <= {order_limit}")     
    else:
        print("Warning, no usgs stream order attribute table found, location list empty")
        
        
    return location_list
                

def get_scenario(
    scenario_definitions: List[dict],
    scenario_name: Union[str, None] = None,
    variable: Union[str, None] = None,
) -> Union[List[dict], dict]:
    
    scenario=[]
    for scenario_i in scenario_definitions:
        if scenario_name is not None and variable is not None:
            if scenario_i["scenario_name"] == scenario_name and scenario_i["variable"] == variable:
                scenario = scenario_i
        elif scenario_name is not None and variable is None:
            if scenario_i["scenario_name"] == scenario_name:
                scenario.append(scenario_i) 
        elif scenario_name is None and variable is not None:
            if scenario["variable"] == variable:
                scenario.append(scenario_i)                             
        
    return scenario


def get_scenario_names(scenario_definitions: List[dict]) -> List[str]:
    
    scenario_name_list = []
    for scenario in scenario_definitions:
        if scenario not in scenario_name_list:
            scenario_name_list.append(scenario["scenario_name"])
    
    return scenario_name_list


def get_scenario_variables(scenario_definitions: List[dict]) -> List[str]:
    
    variable_list = []
    for scenario in scenario_definitions:
        variable_list.append(scenario["variable"])
    
    return list(set(variable_list))


def get_parquet_pathlist_from_scenario(
    scenario_definitions: List[dict],
    scenario_name: str,
    variable: str,
) -> List[Path]:
    
    pathlist=[]
    for scenario in scenario_definitions:
        if scenario["scenario_name"] == scenario_name and scenario["variable"] == variable:
            pathlist.append(scenario["primary_filepath"])
            pathlist.append(scenario["secondary_filepath"])

    return list(set(pathlist))


################################################# widgets

def get_reference_time_text(reference_time) -> pn.pane.HTML:
    return pn.pane.HTML(f"Current Reference Time:   {reference_time}", 
                        sizing_mode = "stretch_width", 
                        styles={'font-size': '15px', 'font-weight': 'bold'})


def get_reference_time_player_all_dates(
    scenario: Union[dict, List[dict]] = {},
    opts: dict = {},
) -> pn.Column:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
        
    reference_times = get_parquet_date_list_across_scenarios(pathlist, date_type = "reference_time")
        
    reference_time_player = pn.widgets.DiscretePlayer(name='Discrete Player', 
                                                      options=list(reference_times), 
                                                      value=reference_times[0], 
                                                      interval=5000)   
    return reference_time_player


def get_reference_time_player_selected_dates(
    scenario: Union[dict, List[dict]] = {},
    start: pd.Timestamp = None,
    end: pd.Timestamp = None,
    opts: dict = {},
) -> pn.widgets.DiscretePlayer:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
        
    reference_times = get_parquet_date_list_across_scenarios(pathlist, date_type = "reference_time")

    reference_times_in_event = [t for t in reference_times if t >= start and t <= end]
    reference_time_player = pn.widgets.DiscretePlayer(name='Discrete Player', 
                                                      options=list(reference_times_in_event), 
                                                      value=reference_times_in_event[0], 
                                                      interval=5000,
                                                      show_loop_controls = False,
                                                      width_policy="fit",
                                                      margin=0,
                                                      **opts,
                                                      )   
    return reference_time_player



def get_reference_time_slider(
    scenario: Union[dict, List[dict]] = {},
    opts: dict = dict(width = 700, bar_color = "red", step=3600000*6),
) -> pn.Column:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
    
    reference_time_slider = get_date_range_slider_with_range_as_title(
        pathlist=pathlist,
        date_type='reference_time',
        opts = opts,
    )   
    return reference_time_slider


def get_value_time_slider(
    scenario: Union[dict, List[dict]] = {},
    opts: dict = dict(width = 700, bar_color = "green", step=3600000),
) -> pn.Column:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
    
    value_time_slider = get_date_range_slider_with_range_as_title(
        pathlist=pathlist,
        date_type='value_time', 
        opts = opts,
    )    
    return value_time_slider

def get_value_time_slider_selected_scenario_name(
    scenario_definitions: List[dict],
    scenario_name: str,
) -> pn.Column:
    
    scenario = get_scenario(scenario_definitions, scenario_name)
    value_time_slider = get_value_time_slider(scenario)
        
    return value_time_slider


def get_date_range_by_scenario(
    scenario: dict,
    date_type: str = "value_time",
) -> tuple:
    
    pathlist=[scenario["primary_filepath"], scenario["secondary_filepath"]]
    range_start, range_end = get_parquet_date_range_across_scenarios(pathlist, date_type = date_type)
    
    return range_start, range_end


def get_date_range_slider_with_range_as_title(
    pathlist: List[Path] = [],
    date_type: str = "value_time",
    opts: dict = {}
) -> pn.Column:
    '''
    Date range slider to select start and end dates of the event
    '''
    
    if date_type == "reference_time":
        range_start, range_end = get_parquet_date_range_across_scenarios(pathlist, date_type = "reference_time") 
        # temporary for workshop
        start_slider = dt.datetime(2023, 1, 1)
        #start_slider=range_start
        value_end = start_slider+dt.timedelta(days=10)
        
    else:
        range_start, range_end = get_parquet_date_range_across_scenarios(pathlist, date_type = "value_time")  
        value_end = range_end
        # temporary for workshop
        start_slider = dt.datetime(2023, 1, 1)
        #start_slider=range_start
        
    dates_text = get_minmax_dates_text(range_start, range_end, date_type)
    dates_slider = pn.widgets.DatetimeRangeSlider(
        name='Selected start/end dates for evaluation',
        start=range_start, 
        end=range_end,
        value=(start_slider, value_end),
        **opts,
    )
    return pn.Column(dates_text, dates_slider)


def get_minmax_dates_text(
    min_date: pd.Timestamp, 
    max_date: pd.Timestamp,
    date_type: str,
    opts: dict = {},
) -> pn.pane:
    return pn.pane.HTML(f"Range of {date_type} available in cache: {min_date} - {max_date}", 
                        sizing_mode = "stretch_width",
                        styles={'font-size': '15px', 'font-weight': 'bold'})

def get_scenario_text(scenario: str) -> pn.pane:
    
    if any([s in scenario for s in ['medium_range','MRF']]):
        scenario_text = "NWM Medium Range Forecasts"
    elif any([s in scenario for s in ['short_range','SRF']]):
        scenario_text = "NWM Short Range Forecasts"
        
    return scenario_text


def get_lead_time_selector() -> pn.widgets.Select:

    lead_time_options = ['all','1 day','3 day','5 day','10 day']
    lead_time_selector = pn.widgets.Select(name="Forecast lead times (coming soon)", 
                                           options=lead_time_options, value="all", width_policy="fit")

    return lead_time_selector


def get_huc2_selector() -> pn.widgets.Select:
    '''
    HUC2 region to explore, enables smaller region for faster responsiveness
    '''
    huc2_list = ["all"] + [str(huc2).zfill(2) for huc2 in list(range(1,19))]
    huc2_selector = pn.widgets.Select(name='HUC-2 Subregion', options=huc2_list, value="all", width_policy="fit")
    
    return huc2_selector      


def get_variable_selector(variable_list: List[str]) -> pn.widgets.Select:
    
    if 'streamflow' in variable_list:
        value='streamflow'
    else:
        value=variable_list[0]
    
    variable_selector = pn.widgets.Select(name='Evaluation variable', 
                                          options=variable_list, 
                                          value=value, 
                                          width_policy="fit")
    
    return variable_selector


def get_postevent_metric_selector(
    variable: Union[str, None] = None,
    metric_list: Union[List[str], None] = None,
) -> pn.widgets.MultiSelect:
    
    if metric_list is None:
        if variable is None: 
            variable = 'streamflow'
            
        if variable == 'streamflow':
            metric_list = [
            "primary_maximum",              
            "secondary_maximum", 
            "max_value_delta",
            "primary_max_value_time",
            "secondary_max_value_time"  ,      
            "max_value_timedelta",
            "primary_count",
            "secondary_count",
            "primary_average",
            "secondary_average",
            "primary_minimum",
            "secondary_minimum",
            ]
            values = [
            "primary_maximum",              
            "secondary_maximum", 
            "max_value_delta",
            "primary_max_value_time",
            "secondary_max_value_time"  ,      
            "max_value_timedelta",
            ]

        elif variable == 'precipitation':
            metric_list = [
            "secondary_sum",
            "primary_sum",
            "secondary_average",
            "primary_average",
            "secondary_variance",
            "primary_variance",        
            ] 
            values=["primary_sum","secondary_sum"]
            
    metric_selector = pn.widgets.MultiSelect(name='Metrics/Characteristics Included', 
                                          options=metric_list, 
                                          value=values, 
                                          width_policy="fit")
    return metric_selector    


def get_scenario_selector(scenario_name_list: List[str]) -> pn.widgets.Select:
    
    scenario_selector = pn.widgets.Select(name='Evaluation scenario', 
                                          options=scenario_name_list, 
                                          value=scenario_name_list[0], 
                                          width_policy="fit")
    
    return scenario_selector


def get_multi_metric_selector(
    variable: Union[str, None] = None,
    metric_list: Union[List[str], None] = None,
) -> pn.widgets.MultiSelect:
    
    if metric_list is None:
        if variable is None: 
            variable = 'streamflow'
            
        if variable == 'streamflow':
            metric_list = [
            "primary_maximum",              
            "secondary_maximum", 
            "max_value_delta",
            "bias",
            "nash_sutcliffe_efficiency",
            "kling_gupta_efficiency",
            "mean_squared_error",
            "root_mean_squared_error",        
            "max_value_timedelta",     
            "primary_max_value_time",
            "secondary_max_value_time"  ,              
            "secondary_count",
            "primary_count",
            "secondary_average",
            "primary_average",
            "secondary_minimum",
            "primary_minimum",             
            ]
            values=["primary_maximum","secondary_maximum","max_value_delta", 
                    "primary_max_value_time", "secondary_max_value_time", "max_value_timedelta"]

        elif variable == 'precipitation':
            metric_list = [
            "bias",
            "secondary_sum",
            "primary_sum",
            "secondary_average",
            "primary_average",
            "secondary_variance",
            "primary_variance",        
            ] 
            values=["primary_sum","secondary_sum"]
            
    metric_selector = pn.widgets.MultiSelect(name='Evaluation metrics', 
                                          options=metric_list, 
                                          value=values, 
                                          width_policy="fit")
    return metric_selector            
            

def get_single_metric_selector(
    variable: Union[str, None] = None,
    metrics: Union[List[str], dict[str], None] = None,
) -> pn.widgets.Select:
    
    if metrics is None:
        if variable is None: 
            variable = 'streamflow'
            
        if variable == 'streamflow':
            metrics = [
            "bias",
            "nash_sutcliffe_efficiency",
            "kling_gupta_efficiency",
            "mean_squared_error",
            "root_mean_squared_error",   
            "secondary_count",
            "primary_count",
            "secondary_average",
            "primary_average",
            "secondary_minimum",
            "primary_minimum",            
            "primary_maximum",              
            "secondary_maximum",    
            "max_value_delta",            
            "primary_max_value_time",
            "secondary_max_value_time",
            "max_value_timedelta",             
            ]
        elif variable == 'precipitation':
            metrics = [
            "bias",
            "secondary_sum",
            "primary_sum",
            "secondary_average",
            "primary_average",
            "secondary_variance",
            "primary_variance",        
            ] 
      
    elif type(metrics) is dict:
        value = list(metrics.values())[0]
    else:
        value = [metrics[0]]
    
    metric_selector = pn.widgets.Select(name='Evaluation metric', 
                                          options=metrics, 
                                          value=value, 
                                          width_policy="fit")
    return metric_selector


def get_order_limit_selector() -> pn.widgets.Select:
    
    order_list = ['None'] + list(range(1,11))
    order_limit_selector = pn.widgets.Select(name='Stream order upper limit', 
                                          options=order_list, 
                                          value=order_list[0], 
                                          width_policy="fit")
    
    return order_limit_selector

def get_threshold_selector(variable: str = 'streamflow') -> pn.widgets.Select:
    
    if variable == 'streamflow':
        threshold_list = [0, '0.1 cms', '1 cms', '2-year (coming soon)','10-year (coming soon)','100-year (coming soon)']
    elif variable == 'precipitation':
        threshold_list = ['0 mm/hr']
        
    threshold_selector = pn.widgets.Select(name='Value threshold (lower limit)', 
                                          options=threshold_list, 
                                          value=threshold_list[0], 
                                          width_policy="fit")
    return threshold_selector

def get_filter_widgets(
    scenario: dict = {},
    include_widgets: List[str] = None
) -> dict:
    
    return_widgets = {}
    if 'value_time' in include_widgets:
        value_time_slider = get_date_range_slider_with_range_as_title(
            pathlist=[scenario["primary_filepath"], scenario["secondary_filepath"]],
            date_type='value_time', 
            opts = dict(width = 700, bar_color = "green", step=3600000)
        )
        return_widgets['value_time'] = value_time_slider
        
    if 'reference_time' in include_widgets:    
        reference_time_slider = get_date_range_slider_with_range_as_title(
            pathlist=[scenario["primary_filepath"], scenario["secondary_filepath"]],
            date_type='reference_time',
            opts = dict(width = 700, bar_color = "red", step=3600000*6)
        )
        return_widgets['reference_time'] = reference_time_slider

    if 'lead_time' in include_widgets:         
        lead_time_selector = get_lead_time_selector()
        return_widgets['lead_time'] = lead_time_selector
        
    if 'huc2' in include_widgets: 
        huc2_selector = get_huc2_selector()
        return_widgets['huc2'] = huc2_selector
        
    if 'stream_order' in include_widgets:
        order_limit_selector = get_order_limit_selector()
        return_widgets['stream_order'] = order_limit_selector
        
    if 'threshold' in include_widgets:        
        threshold_selector = get_threshold_selector(scenario['variable'])
        return_widgets['threshold'] = threshold_selector
        
    if 'metrics' in include_widgets:
        metric_selector = get_multi_metric_selector(scenario['variable'])    
        return_widgets['metrics'] = metric_selector
        
    return return_widgets


def get_widgets_by_scenario(
    scenario_definitions: dict = {},
    scenario_name: Union[str, None] = None,
    variable: Union[str, None] = None,
    include_widgets: List[str] = None
) -> pn.Column:
    
    scenario = get_scenario(scenario_definitions, scenario_name=scenario_name, variable=variable)
            
    return_widgets = {}            
    if scenario is None:
        print('add error catch')       
    else:
        return_widgets = get_filter_widgets(scenario, include_widgets)
        
    return return_widgets


##############################################  attributes

def combine_attributes(
    attribute_paths: dict[Path], 
    viz_units: str
) -> pd.DataFrame:
    
    # note that any locations NOT included in all selected attribute tables will be excluded
    attr_df=pd.DataFrame()
    for key in attribute_paths:
        df = pd.read_parquet(attribute_paths[key])
        if 'attribute_name' in df.columns:
            attribute_name = df['attribute_name'].iloc[0]
            attribute_unit = df['attribute_unit'].iloc[0]
            if attribute_unit != 'none':
                df = convert_attr_to_viz_units(df, viz_units)      
                df = df.rename(columns = {'attribute_value': attribute_name + '_value', 
                                              'attribute_unit': attribute_name + '_unit'})
            else:
                df = df.drop('attribute_unit', axis=1)   
                df = df.rename(columns = {'attribute_value': attribute_name + '_value'})
                
            df = df.drop('attribute_name', axis=1)
            if attr_df.empty:
                attr_df = df.copy()
            else:
                attr_df = attr_df.merge(df, left_on='location_id',right_on='location_id')
            
    return attr_df

    
def merge_attr_to_gdf(
    gdf: gpd.GeoDataFrame, 
    attr_df: pd.DataFrame
)-> gpd.GeoDataFrame:
    
    value_columns = [col for col in attr_df.columns if col.find('value')>=0]
    gdf = gdf.merge(attr_df[['location_id'] + value_columns], 
                     left_on='primary_location_id', right_on='location_id')
    gdf = gdf.drop('location_id', axis=1)
    
    #remove 'value' from column header
    for col in value_columns:
        newcol = col.replace('_value','')
        gdf = gdf.rename(columns = {col: newcol})
    
    return gdf


def merge_df_with_gdf(
    gdf, 
    geom_id_header: str, 
    df,
    location_id_header: str, 
) -> gpd.GeoDataFrame:
    '''
    merge data df (result of DDB query) with geometry, return a geodataframe
    '''
    # merge df with geodataframe
    merged_gdf = gdf.merge(df, left_on=geom_id_header, right_on=location_id_header)    

    # if IDs are HUC codes, convert to type 'category'
    if any(s in location_id_header for s in ["HUC","huc"]):
        print(f"converting column {location_id_header} to category")
        merged_gdf[location_id_header] = merged_gdf[location_id_header].astype("category")
    
    return merged_gdf


##################################### unit conversion stuff

def convert_area_to_ft2(
    units: str, 
    values: pd.Series
) -> pd.Series:
    
    if units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2) * (3.28**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values * (3.28**2)
    elif units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values
        
    return converted_values

def convert_area_to_mi2(
    units: str, 
    values: pd.Series
) -> pd.Series:
    
    if units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2) * (3.28**2) / (5280**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values * (3.28**2) / (5280**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (5280**2)
    elif units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values
        
    return converted_values
    
def convert_flow_to_cfs(
    units: str, 
    values: pd.Series
) -> pd.Series:
    
    if units in ['cms','m3/s']:
        converted_values = values * (3.28**3)
    elif units in ['cfs','ft3/s']:
        converted_values = values
        
    return converted_values 

def convert_area_to_m2(
    units: str, 
    values: pd.Series
) -> pd.Series:
    
    if units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2) / (3.28**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (3.28**2)
    elif units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values
        
    return converted_values

def convert_area_to_km2(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    if units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2) / (3.28**2) / (1000**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (3.28**2) / (1000**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values / (1000**2)        
    elif units in ['km2','sqkm','km**2','km^2']:
        converted_values = values
        
    return converted_values
    
def convert_flow_to_cms(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    if units in ['cfs','ft3/s']:
        converted_values = values / (3.28**3)
    elif units in ['cms','m3/s']:
        converted_values = values
        
    return converted_values 

def convert_depth_to_mm(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    if units in ['in','inches','in/hr']:
        converted_values = values * 25.4
    elif units in ['ft','feet','ft/hr']:
        converted_values = values * 12 * 25.4
    elif units in ['cm','cm/hr']:
        converted_values = values * 10
    elif units in ['m','m/hr']:
        converted_values = values * 1000
    elif units in ['mm','mm/hr']:
        converted_values = values        
    return converted_values 

def convert_depth_to_in(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    if units in ['in','inches','in/hr']:
        converted_values = values
    elif units in ['ft','feet','ft/hr']:
        converted_values = values * 12
    elif units in ['cm','cm/hr']:
        converted_values = values / 2.54
    elif units in ['m','m/hr']:
        converted_values = values / .254
    elif units in ['mm','mm/hr']:
        converted_values = values / 25.4       
    return converted_values 

        
def convert_query_to_viz_units(
    gdf: Union[pd.DataFrame, gpd.GeoDataFrame], 
    viz_units: 'str',
    variable: Union['str', None] = None,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    
    # need a metric library to look up units and ranges
    
    convert_columns = [
        "bias",
        "secondary_average",
        "primary_average",
        "secondary_minimum",
        "primary_minimum",
        "primary_maximum",
        "secondary_maximum",
        "max_value_delta",
        "secondary_sum",
        "primary_sum",
        "secondary_variance",
        "primary_variance"]
    
    if 'variable_name' in gdf.columns:
        variable=gdf['variable_name'][0]
    
    measurement_unit = gdf['measurement_unit'][0]
    if variable in ['streamflow','flow','discharge']:
        if viz_units == 'english':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_flow_to_cfs(measurement_unit, gdf[col])
            gdf['measurement_unit'] = 'ft3/s'
        elif viz_units == 'metric':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_flow_to_cms(measurement_unit, gdf[col])  
            gdf['measurement_unit'] = 'm3/s'
            
    elif variable in ['precip','precipitation','precipitation_rate']:
        if viz_units == 'english':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_depth_to_in(measurement_unit, gdf[col])
            gdf['measurement_unit'] = 'in/hr'
        elif viz_units == 'metric':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_depth_to_mm(measurement_unit, gdf[col])  
            gdf['measurement_unit'] = 'mm/hr'        

    return gdf
                    
    
def convert_attr_to_viz_units(
    df: pd.DataFrame, 
    viz_units: 'str',
) -> pd.DataFrame:
    
    attr_units = df['attribute_unit'][0]
    attr_name = df['attribute_name'][0]
    
    if viz_units == 'english':
        if attr_name.find('area')>=0:
            df['attribute_value'] = convert_area_to_mi2(attr_units, df['attribute_value'])
            df['attribute_unit'] = 'mi2'
        elif attr_name.find('flow')>=0:
            df['attribute_value'] = convert_area_to_cfs(attr_units, df['attribute_value'])
            df['attribute_unit'] = 'cfs'   
            
    elif viz_units == 'metric':
        if attr_name.find('area')>=0:
            df['attribute_value'] = convert_area_to_km2(attr_units, df['attribute_value'])
            df['attribute_unit'] = 'km2'
        elif attr_name.find('flow')>=0:
            df['attribute_value'] = convert_area_to_cms(attr_units, df['attribute_value'])
            df['attribute_unit'] = 'cms'   
        
    return df

def get_normalized_streamflow(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    area_col: str = 'upstream_area',
    include_metrics: Union[List[str], None] = None,
    units: str = 'metric',
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    
    if include_metrics is None:
        print('Metric list empty for normalization')
        return df
    else:
        flow_metrics = get_flow_metrics(include_metrics)
        if flow_metrics is not None:
            for metric in flow_metrics:
                norm_metric = '_'.join([metric, 'norm'])
                if units == 'metric':
                    df[norm_metric] = df[metric]/df[area_col]/(1000)*3600
                else:
                    df[norm_metric] = df[metric]/df[area_col]/(5280**2)*12*3600

            return df

def get_flow_metrics(metric_list: Union[List[str], None]) -> List[str]:

    flow_metrics = [
        "secondary_average",
        "primary_average",
        "secondary_minimum",
        "primary_minimum",            
        "primary_maximum",              
        "secondary_maximum",    
        "max_value_delta",                        
        "secondary_sum",
        "primary_sum",
        "secondary_variance",
        "primary_variance",
    ]
    if metric_list is not None:               
        return [metric for metric in metric_list if metric in flow_metrics]


def get_metric_dict(
    metric: str,
    scenario_name: Union[str, None] = None,
    units: str = 'metric',
) -> dict:

    duration = 240
    if metric in ['max_time_diff']:
        if scenario_name is not None:
            if scenario_name == 'short_range':
                duration = 18
            elif scenario_name == 'medium_range':
                duration = 240
        #else:
            #print(f"add error catch... scenario name required for metric {metric}")
    
    if units == 'metric':
        area_unit = '(km^2)'
        flow_unit = '(cms)'
        pcpn_unit = '(mm)'
    else:
        area_unit = '(mi^2)'
        flow_unit = '(cfs)'
        pcpn_unit = '(in)'
    
    metric_dicts = dict(
        max_perc_diff = dict(
            label="Peak Error (%)",
            unit='%',
            histlims=(-100, 1000),
            sort_ascending=True,
            color_opts=dict(
                cnorm='linear',
                clim=(-100,100),
                cmap=cc.CET_D1A[::-1], 
            )
        ),
        max_time_diff = dict(
            label="Peak Timing Error (hrs)",
            unit='hours',
            histlims=(-duration,duration),
            sort_ascending=True,            
            color_opts=dict(        
                cnorm='linear',
                clim=(-duration,duration),
                cmap=cc.CET_D1A[::-1],  
            )
        ),
        stream_order = dict(
            label="Stream Order",
            unit=None,
            histlims=(1,7),
            sort_ascending=False,
            color_opts=dict(
                clim=(1,7),
                cnorm='linear',
                cmap=cc.rainbow,
            )
        ),
        # ecoregion_num = dict(
        #     label="Ecoregion",
        #     unit=None,
        #     color_opts=dict(
        #         clim=(None,None),
        #         cnorm='linear',
        #         cmap=cc.rainbow,    
        #         sort_ascending=True,
        #     )
        # ),
        ecoregion_int = dict(
            label="Ecoregion (Level II)",
            unit=None,
            histlims=(None,None),
            sort_ascending=False,
            color_opts=dict(
                clim=(None,None),
                cnorm='linear',
                cmap=cc.rainbow,
            )
        ),
        upstream_area = dict(
            label="".join(["Drainage Area ", area_unit]),
            unit=area_unit,
            histlims=(0,None),
            sort_ascending=True,
            color_opts=dict(
                clim=(0,None),
                cnorm='linear',
                cmap=cc.rainbow,    
            )
        ),
        latitude = dict(
            label="Latitude",
            unit='decimal degree',
            histlims=(None,None),
            sort_ascending=False,
            color_opts=dict(
                clim=(None,None),
                cnorm='linear',
                cmap=cc.rainbow,  
            )
        )
    )
    return metric_dicts[metric]
    
    
def get_metric_selector_dict(
    metrics: List[str],
    scenario_name: Union[str, None] = None,
) -> dict:
    
    metric_selector_dict = {}
    for metric in metrics:
        metric_dict = get_metric_dict(metric,scenario_name)    
        metric_selector_dict[metric_dict['label']] = metric
    return metric_selector_dict
    
def get_separate_legend() -> hv.Overlay:
    blue=hv.Curve([2, 2]).opts(fontscale=1.2, xlim=(-0.4,3),ylim=(0,3), 
                               toolbar=None, height=100, width=200, color='dodgerblue', xaxis=None, yaxis=None)
    orange=hv.Curve([1, 1]).opts(color='orange')
    text_obs=hv.Text(1.2,2,'Observed').opts(color='black', text_align='left')
    text_fcst=hv.Text(1.2,1,'Forecast').opts(color='black', text_align='left')
    
    return orange*blue*text_obs*text_fcst

def get_precip_colormap():
    ''' 
    build custom precip colormap 
    '''
    cmap1 = cc.CET_L6[85:]
    cmap2 = [cmap1[i] for i in range(0, len(cmap1), 3)]
    ext = [cmap2[-1] + aa for aa in ['00','10','30','60','99']]
    cmap = ext + cmap2[::-1] + cc.CET_R1
    
    return cmap

def get_recurr_colormap():
    ''' 
    build explicit colormap for 2, 5, 10, 25, 50, 100 recurrence intervals
    based on OWP High Flow Magnitude product
    '''    
    cmap = {0: 'lightgray', 
            2: 'dodgerblue', 
            5: 'yellow', 
            10: 'darkorange', 
            25: 'red', 
            50: 'fuchsia', 
            100: 'darkviolet'}
    
    return cmap

# These hook functions currently rescale the original plot to the data extent - needs to be rewritten to
# read the maintain a ylim max that is set in opts on the plot

def plot_secondary_bars_curve(plot, element):
    """
    Hook to plot data on a secondary (twin) axis on a Holoviews Plot with Bokeh backend.
    More info:
    - http://holoviews.org/user_guide/Customizing_Plots.html#plot-hooks
    - https://docs.bokeh.org/en/latest/docs/user_guide/plotting.html#twin-axes
    """
    fig: Figure = plot.state
    glyph_first: GlyphRenderer = fig.renderers[0]  # will be the original plot
    glyph_last: GlyphRenderer = fig.renderers[-1] # will be the new plot
    right_axis_name = "twiny"
    # Create both axes if right axis does not exist
    if right_axis_name not in fig.extra_y_ranges.keys():
        # Recreate primary axis (left)
        y_first_name = glyph_first.glyph.top
        y_first_min = glyph_first.data_source.data[y_first_name].min()
        y_first_max = glyph_first.data_source.data[y_first_name].max()
        y_first_offset = (y_first_max - y_first_min) * 0.1
        fig.y_range = Range1d(
            start=0,
            end=max(y_first_max,1) + y_first_offset
       )
        fig.y_range.name = glyph_first.y_range_name
        # Create secondary axis (right)
        y_last_name = glyph_last.glyph.y
        y_last_min = glyph_last.data_source.data[y_last_name].min()
        y_last_max = glyph_last.data_source.data[y_last_name].max()
        y_last_offset = (y_last_max - y_last_min) * 0.1
        fig.extra_y_ranges = {right_axis_name: Range1d(
            start=0,
            end=max(y_last_max,1) + y_last_offset
        )}
        fig.add_layout(LinearAxis(y_range_name=right_axis_name, axis_label=glyph_last.glyph.y), "right")
    # Set right axis for the last glyph added to the figure
    glyph_last.y_range_name = right_axis_name
    
    
def plot_secondary_curve_curve(plot, element):
    """
    Hook to plot data on a secondary (twin) axis on a Holoviews Plot with Bokeh backend.
    More info:
    - http://holoviews.org/user_guide/Customizing_Plots.html#plot-hooks
    - https://docs.bokeh.org/en/latest/docs/user_guide/plotting.html#twin-axes
    """
    fig: Figure = plot.state
    glyph_first: GlyphRenderer = fig.renderers[0]  # will be the original plot
    glyph_last: GlyphRenderer = fig.renderers[-1] # will be the new plot
    right_axis_name = "twiny"
    # Create both axes if right axis does not exist
    if right_axis_name not in fig.extra_y_ranges.keys():
        # Recreate primary axis (left)
        y_first_name = glyph_first.glyph.y
        y_first_min = glyph_first.data_source.data[y_first_name].min()
        y_first_max = glyph_first.data_source.data[y_first_name].max()
        y_first_offset = (y_first_max) * 0.1
        fig.y_range = Range1d(
            start=0,
            end=y_first_max + y_first_offset
       )
        # Create secondary axis (right)
        y_last_name = glyph_last.glyph.y
        y_last_min = glyph_last.data_source.data[y_last_name].min()
        y_last_max = glyph_last.data_source.data[y_last_name].max()
        y_last_offset = (y_last_max) * 0.1
        fig.extra_y_ranges = {right_axis_name: Range1d(
            start=0,
            end=y_last_max + y_last_offset
        )}
        fig.add_layout(LinearAxis(y_range_name=right_axis_name, axis_label=glyph_last.glyph.y), "right")
    # Set right axis for the last glyph added to the figure
    glyph_last.y_range_name = right_axis_name

def update_map_extents(plot, element):

    bkplot = plot.handles['plot']
    xdata = element.dataset.data[element.dataset.kdims[0].name]   
    x_range = xdata.min(), xdata.max()
    buffer_x = (x_range[1] - x_range[0])*0.1
    x_range = x_range[0] - buffer_x, x_range[1] + buffer_x
    
    old_x_range_reset = bkplot.x_range.reset_start, bkplot.x_range.reset_end  
    if old_x_range_reset != x_range:
        bkplot.x_range.start, bkplot.x_range.end = x_range
        bkplot.x_range.reset_start, bkplot.x_range.reset_end = x_range    
    
    ydata = element.dataset.data[element.dataset.kdims[1].name]
    y_range = ydata.min(), ydata.max()
    buffer_y = (y_range[1] - y_range[0])*0.1
    y_range = y_range[0] - buffer_y, y_range[1] + buffer_y
    
    old_y_range_reset = bkplot.y_range.reset_start, bkplot.y_range.reset_end  
    if old_y_range_reset != y_range:
        bkplot.y_range.start, bkplot.y_range.end = y_range
        bkplot.y_range.reset_start, bkplot.y_range.reset_end = y_range
    
####################### holoviews

def get_aggregator(measure):
    '''
    datashader aggregator function
    '''
    return ds.mean(measure)

def build_points_dmap_from_query(
    scenario: dict,
    location_id: Union[str, List[str], None] = None,    
    huc_id: Union[str, List[str], None] = None,
    order_limit: Union[int, None] = None,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None,
    group_by: Union[List[str], None] = None,
    order_by: Union[List[str], None] = None,       
    include_metrics: Union[List[str], None] = None,
    metric_limits: Union[dict, None] = None,  
    plot_metric: Union[str, None] = None,
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
) -> hv.Points:
    
    # temporary
    include_metrics_query=include_metrics.copy()
    if 'max_perc_diff' in include_metrics_query:
        include_metrics_query[include_metrics_query.index('max_perc_diff')]='primary_maximum'
        include_metrics_query.append('max_value_delta')
    if 'max_time_diff'in include_metrics_query:
        include_metrics_query[include_metrics_query.index('max_time_diff')]='max_value_timedelta'    
    
    gdf = run_teehr_query(
        query_type="metrics",
        scenario=scenario,
        location_id=location_id,
        huc_id=huc_id,
        order_limit=order_limit,
        value_time_start=value_time_start,
        value_time_end=value_time_end,
        reference_time_single=reference_time_single,
        reference_time_start=reference_time_start,
        reference_time_end=reference_time_end,
        value_min=value_min,
        value_max=value_max,
        include_metrics=include_metrics_query,
        group_by=group_by,
        order_by=order_by,
        attribute_paths=attribute_paths
    )
    #print(len(gdf))
    
    # convert units if necessary, add attributes, normalize
    gdf = convert_query_to_viz_units(gdf, units, scenario['variable'])
    attribute_df = combine_attributes(attribute_paths,units)
    gdf = merge_attr_to_gdf(gdf, attribute_df)
    
    # temporary - to be generalized
    include_metrics = include_metrics + ['upstream_area','ecoregion_L2','stream_order']    
    
    if 'max_perc_diff' in include_metrics:
        gdf['max_perc_diff'] = gdf['max_value_delta']/gdf['primary_maximum']*100
    if 'max_time_diff'in include_metrics:
        gdf['max_time_diff'] = (gdf['max_value_timedelta'] / np.timedelta64(1, 'h')).astype(int)
    
    # gdf = get_normalized_streamflow(gdf, include_metrics=include_metrics)
    # norm_metrics = [col for col in gdf.columns if col[-4:]=='norm']
    if metric_limits is not None:
        for metric in gdf.columns:
            if metric in metric_limits.keys():
                limits = metric_limits[metric]
                gdf = gdf[(gdf[metric] >= limits[0]) & (gdf[metric] <= limits[1])]       
    
    # leave out geometry - easier to work with the data
    df = gdf.loc[:,[c for c in gdf.columns if c!='geometry']] 
    df['easting']=gdf.to_crs("EPSG:3857").geometry.x
    df['northing']=gdf.to_crs("EPSG:3857").geometry.y
    
#     if plot_metric is not None:
#         metric_dict = get_metric_dict(plot_metric, scenario_name=scenario['scenario_name'])
#     else:
#         metric_dict = get_metric_dict(vdims[0][0], scenario_name=scenario['scenario_name'])    
    
#     # for now limit vdims to peaks, % peak diff, and id
#     #vdim_metrics = list(set(include_metrics + norm_metrics))
#     vdims = [(plot_metric, metric_dict['label']),
#              ('primary_location_id','id'),
#              ('upstream_area','drainage_area')]
#     kdims = ['easting','northing']    
        
#     points = hv.Points(df, kdims=kdims, vdims=vdims)#.redim.range(
        # easting=(df['easting'].min(), df['easting'].max()),
        # northing=(df['northing'].min(), df['northing'].max()))
#     points = points.opts(color=hv.dim(plot_metric), **metric_dict['color_opts'], title=metric_dict['label'], hooks=[update_map_extents])
    
    points_bind=pn.bind(create_points, df, plot_metric, scenario)
    
    points_dmap = (hv.DynamicMap(points_bind)).opts(hooks=[update_map_extents]).redim.range(
        easting=(df['easting'].min(), df['easting'].max()),
        northing=(df['northing'].min(), df['northing'].max()))
    
    return points_dmap

def create_points(df, plot_metric, scenario):
    if plot_metric is not None:
        metric_dict = get_metric_dict(plot_metric, scenario_name=scenario['scenario_name'])
    else:
        metric_dict = get_metric_dict(vdims[0][0], scenario_name=scenario['scenario_name'])  
    vdims = [(plot_metric, metric_dict['label']),
             ('primary_location_id','id'),
             ('upstream_area','drainage_area')]
    kdims = ['easting','northing']    
    points = hv.Points(df, kdims=kdims, vdims=vdims)    
    
    return points

def build_points_from_query(
    scenario: dict,
    location_id: Union[str, List[str], None] = None,    
    huc_id: Union[str, List[str], None] = None,
    order_limit: Union[int, None] = None,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None,
    group_by: Union[List[str], None] = None,
    order_by: Union[List[str], None] = None,       
    include_metrics: Union[List[str], None] = None,
    metric_limits: Union[dict, None] = None,  
    plot_metric: Union[str, None] = None,
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
    return_query: bool = False,
) -> hv.Points:
    
    # temporary
    include_metrics_query=include_metrics.copy()
    if 'max_perc_diff' in include_metrics_query:
        include_metrics_query[include_metrics_query.index('max_perc_diff')]='primary_maximum'
        include_metrics_query.append('max_value_delta')
    if 'max_time_diff'in include_metrics_query:
        include_metrics_query[include_metrics_query.index('max_time_diff')]='max_value_timedelta'    
        
    #print(include_metrics_query)
    
    gdf = run_teehr_query(
        query_type="metrics",
        scenario=scenario,
        location_id=location_id,
        huc_id=huc_id,
        order_limit=order_limit,
        value_time_start=value_time_start,
        value_time_end=value_time_end,
        reference_time_single=reference_time_single,
        reference_time_start=reference_time_start,
        reference_time_end=reference_time_end,
        value_min=value_min,
        value_max=value_max,
        include_metrics=include_metrics_query,
        group_by=group_by,
        order_by=order_by,
        attribute_paths=attribute_paths,
        return_query=return_query,
    )
    
    # convert units if necessary, add attributes, normalize
    gdf = convert_query_to_viz_units(gdf, units, scenario['variable'])
    attribute_df = combine_attributes(attribute_paths,units)
    gdf = merge_attr_to_gdf(gdf, attribute_df)
    
    # temporary - to be generalized
    include_metrics = include_metrics + ['upstream_area','ecoregion_L2','stream_order']    
    
    if 'max_perc_diff' in include_metrics:
        gdf['max_perc_diff'] = gdf['max_value_delta']/gdf['primary_maximum']*100
    if 'max_time_diff'in include_metrics:
        gdf['max_time_diff'] = (gdf['max_value_timedelta'] / np.timedelta64(1, 'h')).astype(int)
    
    # gdf = get_normalized_streamflow(gdf, include_metrics=include_metrics)
    # norm_metrics = [col for col in gdf.columns if col[-4:]=='norm']
    if metric_limits is not None:
        for metric in gdf.columns:
            if metric in metric_limits.keys():
                limits = metric_limits[metric]
                gdf = gdf[(gdf[metric] >= limits[0]) & (gdf[metric] <= limits[1])]     
                
    # print(len(gdf))
    # print(gdf.loc[220:230,'primary_location_id'])
    
    # leave out geometry - easier to work with the data
    df = gdf.loc[:,[c for c in gdf.columns if c!='geometry']] 
    df['easting']=gdf.to_crs("EPSG:3857").geometry.x
    df['northing']=gdf.to_crs("EPSG:3857").geometry.y
    # xlim=(df['easting'].min(), df['easting'].max())
    # ylim=(df['northing'].min(), df['northing'].max())
    
    if plot_metric is not None:
        metric_dict = get_metric_dict(plot_metric, scenario_name=scenario['scenario_name'])
    else:
        metric_dict = get_metric_dict(vdims[0][0], scenario_name=scenario['scenario_name'])    
    
#     # for now limit vdims to peaks, % peak diff, and id
    #vdim_metrics = list(set(include_metrics + norm_metrics))
    vdims = [(plot_metric, metric_dict['label']),
             ('primary_location_id','id'),
             ('upstream_area','drainage_area')]
    kdims = ['easting','northing']    
        
    points = hv.Points(df, kdims=kdims, vdims=vdims)
    points = points.opts(color=hv.dim(plot_metric), **metric_dict['color_opts'], title=metric_dict['label'], hooks=[update_map_extents])  
                         #xlim=xlim, ylim=ylim) 
    
    return points.opts(framewise=True)


def build_polygons_from_query(
    scenario: dict,
    location_id: Union[str, List[str], None] = None,    
    huc_id: Union[str, List[str], None] = None,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None,
    group_by: Union[List[str], None] = None,
    order_by: Union[List[str], None] = None,       
    include_metrics: Union[List[str], None] = None,
    metric_limits: Union[dict, None] = None,  
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
) -> hv.Points:
    
    gdf = run_teehr_query(
        query_type="metrics",
        scenario=scenario,
        location_id=location_id,
        huc_id=huc_id,
        value_time_start=value_time_start,
        value_time_end=value_time_end,
        reference_time_single=reference_time_single,
        reference_time_start=reference_time_start,
        reference_time_end=reference_time_end,
        value_min=value_min,
        value_max=value_max,
        include_metrics=include_metrics,
        group_by=group_by,
        order_by=order_by,
        attribute_paths=attribute_paths
    )

    
    # convert units if necessary, add attributes, normalize
    gdf = gdf.to_crs("EPSG:3857")
    gdf = convert_query_to_viz_units(gdf, units, scenario['variable'])
    gdf['sum_diff'] = gdf['secondary_sum']-gdf['primary_sum']
    include_metrics = include_metrics + ['sum_diff']

    if metric_limits is not None:
        for metric in gdf.columns:
            if metric in metric_limits.keys():
                limits = metric_limits[metric]
                gdf = gdf[(gdf[metric] >= limits[0]) & (gdf[metric] <= limits[1])]       
    
    #convert to spatialpandas object
    sdf = spd.GeoDataFrame(gdf)
    
    #vdim_metrics = list(set(include_metrics + norm_metrics))
    vdims = include_metrics + [('primary_location_id','id')] 
    polygons = gv.Polygons(sdf, crs=ccrs.GOOGLE_MERCATOR, vdims=vdims)

    return polygons


def build_hyetograph_from_query_selected_point(
    index: List[int],
    points_dmap: hv.DynamicMap,
    scenario: dict,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None, 
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
    opts = {},
) -> hv.Layout:
    
    if len(index) > 0 and len(points_dmap.dimensions('value')) > 0:  
        point_id = points_dmap.dimension_values('primary_location_id')[index][0]
        cross = pd.read_parquet(attribute_paths['usgs_huc_crosswalk'])
        huc12_id = cross.loc[cross['primary_location_id']==point_id, 'secondary_location_id'].iloc[0]
        huc10_id = "-".join(['huc10', huc12_id.split("-")[1][:10]])
        title = f"{huc10_id} (Contains Gage: {point_id})"# {reference_time_single} {index}"     
        
        df = run_teehr_query(
            query_type="timeseries",
            scenario=scenario,
            location_id=huc10_id,
            value_time_start=value_time_start,
            value_time_end=value_time_end,
            reference_time_single=reference_time_single,
            reference_time_start=reference_time_start,
            reference_time_end=reference_time_end,
            value_min=value_min,
            value_max=value_max,
            order_by=['primary_location_id','reference_time','value_time'],
            attribute_paths=attribute_paths,
            include_geometry=False,
        )            
        df = convert_query_to_viz_units(df, units, 'precipitation')
        
        df['value_time_str'] = df['value_time'].dt.strftime('%Y-%m-%d-%H')
        time_start = df['value_time'].min()
        time_end = df['value_time'].max()
        t = time_start + (time_end - time_start)*0.01
        text_x = t.replace(second=0, microsecond=0, minute=0).strftime('%Y-%m-%d-%H')

        if units == 'metric':
            unit_rate_label = 'mm/hr'
            unit_cum_label = 'mm'
            min_yaxis = 25
        else:
            unit_rate_label = 'in/hr'
            unit_cum_label = 'in'
            min_yaxis = 1
        
        if 'value' in df.columns:  #single timeseries
            df['cumulative'] = df['value'].cumsum()

            data_max = df['value'].max()
            ymax_bars = max(data_max*1.1,min_yaxis)
            ymax_curve = max(data_max*1.1,min_yaxis)
            text_y = ymax_bars*0.9   
            text_label = hv.Text(text_x, text_y, title).opts(text_align='left', text_font_size='10pt', 
                            text_color='#57504d', text_font_style='bold')               

            bars = hv.Bars(df, kdims = [('value_time_str','Date')], vdims = [('value', 'Precip Rate ' + unit_rate_label)])
            curve = hv.Curve(df, kdims = [('value_time_str', 'Date')], vdims = [('cum', 'Precip ' + unit_cum_label)])

            bars.opts(**opts, fill_color = 'blue', line_color = None, ylim=(0, ymax_bars))
            curve.opts(**opts, color='orange', hooks=[plot_secondary_bars_curve])
            ts_layout = (bars * curve * text_label).opts(show_title=False)  

        else:

            df['primary_cumulative'] = df['primary_value'].cumsum()
            df['secondary_cumulative'] = df['secondary_value'].cumsum()
            data_max = max(df['primary_value'].max(), df['secondary_value'].max())
            data_max_cum = max(df['primary_cumulative'].max(), df['secondary_cumulative'].max())
            
            ###  need to fix ymax so both cumulative 
            
            ymax_bars = max(data_max*1.1,min_yaxis)
            ymax_curve = max(data_max_cum*1.1,min_yaxis) 
            
            bars_prim_max = df['primary_value'].max()
            bars_sec_max = df['secondary_value'].max()
            curve_prim_max = df['primary_cumulative'].max()
            curve_sec_max = df['secondary_cumulative'].max()
            text_y = ymax_bars*0.85   
            text_label = hv.Text(text_x, text_y, title).opts(text_align='left', text_font_size='10pt', 
                            text_color='#57504d', text_font_style='bold')  
            
            bars_prim = hv.Bars(df, kdims = [('value_time_str','Date')], 
                                vdims = [('primary_value', 'Precip Rate ' + unit_rate_label)])          
            curve_prim = hv.Curve(df, kdims = [('value_time_str', 'Date')], 
                                  vdims = [('primary_cumulative', 'Accum. Precip ' + unit_cum_label)])
            
            bars_sec = hv.Bars(df, kdims = [('value_time_str','Date')], 
                               vdims = [('secondary_value', 'Precip Rate ' + unit_rate_label)])
            curve_sec = hv.Curve(df, kdims = [('value_time_str', 'Date')], 
                                 vdims = [('secondary_cumulative', 'Accum. Precip ' + unit_cum_label)])
            
            bars_prim.opts(**opts, fill_color = 'dodgerblue', line_color = None, ylim=(0, ymax_bars))
            bars_sec.opts(**opts, fill_color = 'orange', line_color = None, ylim=(0, ymax_bars))            
            
            # comment out secondary plot hook for now, still remaining issues with scale since the hook function resets
            # the y-axis limits.  Need to rewrite the hook function.
            curve_prim.opts(**opts, color='dodgerblue', ylim=(0, ymax_curve))#, hooks=[plot_secondary_bars_curve])                
            curve_sec.opts(**opts, color='orange', ylim=(0, ymax_curve))#, hooks=[plot_secondary_bars_curve])    
                   
            ###  code below is necessary as a work around to a scale issue when using hooks to plot on a secondary y-axis
            # if bars_prim_max > bars_sec_max and curve_prim_max > curve_sec_max:
            #     #ts_layout = hv.Layout(bars_prim * bars_sec * curve_prim * curve_sec * text_label).opts(show_title=False)
            #     ts_layout = hv.Layout(bars_prim * bars_sec * text_label + curve_prim * curve_sec).cols(1).opts(show_title=False)
            # elif bars_prim_max > bars_sec_max and curve_prim_max < curve_sec_max:
            #     ts_layout = hv.Layout(bars_prim * bars_sec * text_label + curve_sec * curve_prim).cols(1).opts(show_title=False)
            #     #ts_layout = (bars_prim * bars_sec * curve_sec * curve_prim * text_label).opts(show_title=False)
            # elif bars_prim_max < bars_sec_max and curve_prim_max < curve_sec_max:
            #     ts_layout = (bars_sec * bars_prim * curve_sec * curve_prim * text_label).opts(show_title=False)
            # elif bars_prim_max < bars_sec_max and curve_prim_max > curve_sec_max:
            #     ts_layout = (bars_sec * bars_prim * curve_prim * curve_sec * text_label).opts(show_title=False)
            
            ts_layout = hv.Layout(bars_prim * bars_sec * text_label + curve_prim * curve_sec).cols(1).opts(show_title=False)            
                
    else:        
        df = pd.DataFrame([[0,0],[1,0]], columns = ['Date','value'])
        curve = hv.Curve(df, "Date", "value").opts(**opts)
        text_label = hv.Text(0.01, 0.9, "No Selection").opts(text_align='left', text_font_size='10pt', 
                                                       text_color='#57504d', text_font_style='bold')
        ts_layout = hv.Layout(curve * text_label + curve * text_label).cols(1).opts(show_title=False)
            
    return ts_layout 


def build_hydrograph_from_query_selected_point(
    index: List[int],
    points_dmap: hv.DynamicMap,
    scenario: dict,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None, 
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
    opts = {},
) -> hv.Layout:
    
    if len(index) > 0 and len(points_dmap.dimensions('value')) > 0:  
        point_id = points_dmap.dimension_values('primary_location_id')[index][0]
        area = points_dmap.dimension_values('upstream_area')[index][0]
        title = f"Gage ID: {point_id}"# {reference_time_single} {index}"
        
        df = run_teehr_query(
            query_type="timeseries",
            scenario=scenario,
            location_id=point_id,
            value_time_start=value_time_start,
            value_time_end=value_time_end,
            reference_time_single=reference_time_single,
            reference_time_start=reference_time_start,
            reference_time_end=reference_time_end,
            value_min=value_min,
            value_max=value_max,
            order_by=['primary_location_id','reference_time','value_time'],
            attribute_paths=attribute_paths,
            include_geometry=False,
        )      
        if len(df) == 0:
            df = pd.DataFrame([[0,0],[1,0]], columns = ['Date','value'])
            label = "No Data"
            curve = hv.Curve(df, "Date", "value").opts(**opts)
            text = hv.Text(0.01, 0.9, "No Data").opts(text_align='left', text_font_size='10pt', 
                                                           text_color='#57504d', text_font_style='bold')
            ts_layout = (curve * text).opts(show_title=False)
            return ts_layout
        
        df = convert_query_to_viz_units(df, units, 'streamflow')  

        
        time_start = df['value_time'].min()
        time_end = df['value_time'].max()
        t = time_start + (time_end - time_start)*0.05
        text_x = t.replace(second=0, microsecond=0, minute=0)

        if units == 'metric':
            unit_label = 'cms'
            unit_norm_label = 'mm/hr'
            conversion = 3600*1000/1000**2
        else:
            unit_label = 'cfs'
            unit_norm_label = 'in/hr'
            conversion = 3600*12/5280**2
        
        if 'value' in df.columns:  #single timeseries
            df['normalized'] = df['value']/area*conversion

            data_max = df['value'].max()
            data_max_norm = df['normalized'].max()
            ymax_flow = max(data_max*1.1,100)
            ymax_norm = max(data_max_norm*1.1,1)
            text_y = ymax_flow*0.9   
            text_label = hv.Text(text_x, text_y, title).opts(text_align='left', text_font_size='10pt', 
                                           text_color='#57504d', text_font_style='bold')  

            flow = hv.Curve(df, kdims = [('value_time', 'Date')], vdims = [('value', 'Flow ' + unit_label)])
            #norm = hv.Curve(df, kdims = [('value_time', 'Date')], vdims = [('normalized', 'Normalized Flow ' + unit_norm_label)])

            flow.opts(**opts, color='blue', ylim=(0, ymax_flow))
            #norm.opts(**opts, color='orange', alpha=0, ylim=(0, ymax_norm), hooks=[plot_secondary_bars_curve])

        else:

            df['primary_normalized'] = df['primary_value']/area*conversion
            df['secondary_normalized'] = df['secondary_value']/area*conversion
            prim_max = df['primary_value'].max()
            sec_max = df['secondary_value'].max()
            data_max = max(prim_max, sec_max)
            data_max_norm = max(df['primary_normalized'].max(), df['secondary_normalized'].max())
            
            ymax_flow = max(data_max*1.1,100)
            ymax_norm = max(data_max_norm*1.1,1)
            text_y = ymax_flow*0.9  
            
            text_label = hv.Text(text_x, text_y, title).opts(text_align='left', text_font_size='10pt', 
                                           text_color='#57504d', text_font_style='bold')  

            flow_prim = hv.Curve(df, kdims = [('value_time', 'Date')], 
                                 vdims = [('primary_value', 'Flow ' + unit_label)])
            norm_prim = hv.Curve(df, kdims = [('value_time', 'Date')], 
                                 vdims = [('primary_normalized', 'Normalized Flow ' + unit_norm_label)])
            
            flow_sec = hv.Curve(df, kdims = [('value_time', 'Date')], 
                                vdims = [('secondary_value', 'Flow ' + unit_label)])
            norm_sec = hv.Curve(df, kdims = [('value_time', 'Date')], 
                                vdims = [('secondary_normalized', 'Normalized Flow ' + unit_norm_label)])

            flow_prim.opts(**opts, color='dodgerblue', ylim=(0, ymax_flow))
            norm_prim.opts(**opts, color='blue', alpha=0, ylim=(0, ymax_norm), hooks=[plot_secondary_curve_curve])  
            
            flow_sec.opts(**opts, color='orange', ylim=(0, ymax_flow))
            norm_sec.opts(**opts, color='orange', alpha=0, ylim=(0, ymax_norm), hooks=[plot_secondary_curve_curve]) 
          
            if prim_max > sec_max:
                #ts_layout = (flow_prim * flow_sec * norm_prim * norm_sec * text_label).opts(show_title=False)  
                ts_layout = (flow_prim * flow_sec * text_label).opts(show_title=False)  
            else:
                #ts_layout = (flow_sec * flow_prim * norm_sec * norm_prim * text_label).opts(show_title=False)
                ts_layout = (flow_sec * flow_prim * text_label).opts(show_title=False)

    else:        
        df = pd.DataFrame([[0,0],[1,0]], columns = ['Date','value'])
        label = "Nothing Selected"
        curve = hv.Curve(df, "Date", "value").opts(**opts)
        text = hv.Text(0.01, 0.9, "No Selection").opts(text_align='left', text_font_size='10pt', 
                                                       text_color='#57504d', text_font_style='bold')
        ts_layout = (curve * text).opts(show_title=False)
            
    return ts_layout  

def get_gage_basin_selected_point(
    index: List[int],
    points_dmap: hv.DynamicMap,     
    gage_basins_gdf: dict = {},  
    opts: dict = {},
) -> Union[hv.Polygons, None]:
    
    if len(index) > 0 and len(points_dmap.dimensions('value')) > 0:  
        point_id = points_dmap.dimension_values('primary_location_id')[index][0]
        selected_polygon = gage_basins_gdf.loc[gage_basins_gdf['id']==point_id,:]
        polygon = gv.Polygons(selected_polygon, crs=ccrs.GOOGLE_MERCATOR)
        polygon.opts(line_color='k', line_width=2, line_alpha=1, fill_alpha=0)
        
    else:
        point_id = points_dmap.dimension_values('primary_location_id')[0]
        selected_polygon = gage_basins_gdf.loc[gage_basins_gdf['id']==point_id,:]
        polygon = gv.Polygons(selected_polygon, crs=ccrs.GOOGLE_MERCATOR)
        polygon.opts(line_color='k', line_width=2, line_alpha=0, fill_alpha=0)
        
    return polygon

def get_all_points(scenario: dict) -> hv.Points:

    df = pd.read_parquet(scenario["geometry_filepath"])
    gdf = tqu.df_to_gdf(df)
    gdf['easting'] = gdf.geometry.x
    gdf['northing'] = gdf.geometry.y
    
    return hv.Points(gdf, kdims = ['easting','northing'], vdims = ['id'])


def get_points_in_huc(
    scenario: dict,
    huc_id: str,
    attribute_paths: Union[List[Path], None] = None,
) -> hv.Points:

    path = scenario['geometry_filepath']
    gdf = gpd.read_parquet(path).to_crs("EPSG:3857")
#    gdf = tqu.df_to_gdf(df)
    location_list = get_usgs_locations_within_huc(huc_level=2, huc_id=huc_id, attribute_paths=attribute_paths)
    gdf=gdf[gdf['id'].isin(location_list)]
    gdf['easting'] = gdf.geometry.x
    gdf['northing'] = gdf.geometry.y
    
    return hv.Points(gdf, kdims = ['easting','northing'], vdims = ['id'])


def get_histogram(
    df: pd.DataFrame, 
    column: str,
    scenario_name: str = 'medium_range',
    units: str = 'metric',
    nbins: int = 50,
    opts: dict = {},
) -> hv.Histogram:

    variable_dict = get_metric_dict(metric=column, scenario_name=scenario_name, units=units)   
    label = variable_dict['label']
    lims = variable_dict['histlims']
    
    if lims == (None, None):
        lims = (df[column].min(), df[column].max())
    elif lims[0] is None:
        lims = (df[column].min(), lims[1])
    elif lims[1] is None:
        lims = (lims[0], df[column].max())

    hist = df.hvplot.hist(y=column, bins=nbins, bin_range=lims, xlabel=label)

    return hist.opts(**opts)

def get_categorical_histogram(
    df: pd.DataFrame(),
    column: str,
    scenario_name: str = 'medium_range',
    units: str = 'metric',
    labels: Union[List[str],None] = None, 
    opts: dict = {},
) -> hv.Histogram:
    
    variable_dict = get_metric_dict(metric=column, scenario_name=scenario_name, units=units)   
    label = variable_dict['label']
  
    nbins=len(df[column].unique())
    frequencies, edges = np.histogram(df[column], nbins)
    centers = list(edges[:-1] + (edges[1:] - edges[:-1])/2)
    if labels is None:
        labels = list(range(1,nbins+1))
    hist = df.hvplot.hist(y=column, bins=nbins, xlabel=label)
    hist = hist.opts(**opts, xticks=[(centers[i], labels[i]) for i in range(0,nbins)])
    
    return hist


def get_diag_line(
    df: pd.DataFrame, 
    scatter_variable: str,
) -> hv.Curve:
    
    if scatter_variable == 'Peak Flow':
        maxpeak = max(df['primary_maximum'].max(), df['secondary_maximum'].max())
        lims=[maxpeak*-0.1,maxpeak*1.1]
    elif scatter_variable == 'Peak Time':   
        mintime = min(df['primary_max_value_time'].min(), df['secondary_max_value_time'].min())
        maxtime = max(df['primary_max_value_time'].max(), df['secondary_max_value_time'].max())
        dt = (maxtime - mintime)*0.1
        lims=[mintime - dt, maxtime + dt]          
        
    return hv.Curve((lims,lims)).opts(color="lightgray", alpha=0.5)        

    
def get_scatter(
    df: pd.DataFrame, 
    scatter_variable: str,
    color_variable: str,
    scenario_name: str = 'medium_range',
    units: str = 'metric',
    opts: dict = {},
) -> hv.Scatter:
    
    color_variable_dict = get_metric_dict(metric=color_variable, scenario_name=scenario_name, units=units)    
    
    clim = color_variable_dict['color_opts']['clim']
    cnorm = color_variable_dict['color_opts']['cnorm']
    cmap = color_variable_dict['color_opts']['cmap']
    csort = color_variable_dict['sort_ascending']    
    clabel = color_variable_dict['label']

    if clim == (None, None):
        clim = (df[color_variable].min(), df[color_variable].max())
    elif clim[0] is None:
        clim = (df[color_variable].min(), clim[1])
    elif clim[1] is None:
        clim = (clim[0], df[color_variable].max())
        
    # if categorical attribute, get colormap subset
    if color_variable in ['ecoregion_int', 'stream_order']:
        ncats = df[color_variable].nunique()
        cstep = int(np.floor(len(cmap) / ncats))
        cmap = [cmap[cstep * t] for t in range(0,ncats)]
        
    df = df.sort_values(color_variable, ascending=csort)
    
    if scatter_variable == 'Peak Flow':
        kdims=['primary_maximum']
        vdims=['secondary_maximum'] + [c for c in df.columns if c not in ['primary_maximum','secondary_maximum']]
        if units == 'metric':
            unit = '(cms)'
        else:
            unit = '(cfs)'
        xlabel='Observed Peak ' + unit 
        ylabel='Forecast Peak ' + unit
        maxpeak = max(df['primary_maximum'].max(), df['secondary_maximum'].max())
        lims=(maxpeak*-0.1,maxpeak*1.1)      
        formatter='%f'
        
        
    elif scatter_variable == 'Peak Time':
        kdims=['primary_max_value_time']
        vdims=['secondary_max_value_time'] + [c for c in df.columns if c not in ['primary_max_value_time','secondary_max_value_time']] 
        xlabel='Observed Peak Time'
        ylabel='Forecast Peak Time'
        mintime = min(df['primary_max_value_time'].min(), df['secondary_max_value_time'].min())
        maxtime = max(df['primary_max_value_time'].max(), df['secondary_max_value_time'].max())
        dt = (maxtime - mintime)*0.1
        lims=(mintime - dt, maxtime + dt)  
        formatter=DatetimeTickFormatter(months=["%b %d"])
    
    scatter = hv.Scatter(df, kdims=kdims, vdims=vdims).opts(**opts,
        color=color_variable, cmap=cmap, cnorm=cnorm, clim=clim, clabel=clabel, 
        ylim=lims, xlim=lims, xlabel=xlabel, ylabel=ylabel, xformatter=formatter, yformatter=formatter)
    
    return scatter.opts(framewise=True)

def get_points(
    df: pd.DataFrame, 
    color_variable: str,
    scenario_name: str = 'medium_range',
    units: str = 'metric',
    opts: dict = {},
) -> hv.Points: 
    
    color_variable_dict = get_metric_dict(metric=color_variable, scenario_name=scenario_name, units=units)
    
    clim = color_variable_dict['color_opts']['clim']
    cnorm = color_variable_dict['color_opts']['cnorm']
    cmap = color_variable_dict['color_opts']['cmap']
    csort = color_variable_dict['sort_ascending']    
    clabel = color_variable_dict['label']

    if clim == (None, None):
        clim = (df[color_variable].min(), df[color_variable].max())
    elif clim[0] is None:
        clim = (df[color_variable].min(), clim[1])
    elif clim[1] is None:
        clim = (clim[0], df[color_variable].max())
        
    # if categorical attribute, get colormap subset
    if color_variable in ['ecoregion_int', 'stream_order']:
        ncats = df[color_variable].nunique()
        cstep = int(np.floor(len(cmap) / ncats))
        cmap = [cmap[cstep * t] for t in range(0,ncats)]
    
    vdims=[c for c in df.columns if c not in ['easting','northing']] 
    
    points = hv.Points(df, kdims=['easting','northing'], vdims=vdims).opts(**opts,
        color=hv.dim(color_variable), cmap=cmap, cnorm=cnorm,  clim=clim, colorbar=True, clabel=None, #clabel,
        size=5, xaxis=None, yaxis=None, tools=['hover'])
                       
    return points


def post_event_dashboard_2(
    scenario_definitions: List[dict],
    scenario_selector: pn.widgets.Select,
    #huc2_selector: pn.widgets.Select,      
    value_time_slider: pn.Column,
    attribute_paths: dict[Path],
    include_metrics: List[str],    
    viz_units: str = 'metric',
    gage_basins_gdf: Union[gpd.GeoDataFrame, None] = None
) -> pn.Column:
    
    huc2_selector = get_huc2_selector()
    metric_selector = get_single_metric_selector(
        metrics = get_metric_selector_dict(metrics=include_metrics))
    scenarios = get_scenario(scenario_definitions, scenario_name=scenario_selector.value)
    scenario_text = get_scenario_text(scenario_selector.value)
    streamflow_scenario = [s for s in scenarios if s['variable']=='streamflow'][0]
    precip_scenario = [s for s in scenarios if s['variable']=='precipitation'][0]

    # reference time player (eventually replace with individual arrows)
    reference_time_player = get_reference_time_player_selected_dates(
        scenario=scenarios, 
        start=value_time_slider[1].value_start-dt.timedelta(hours=1), 
        end=value_time_slider[1].value_end
    )
    current_ref_time = pn.bind(get_reference_time_text, reference_time=reference_time_player.param.value)

    # Some universal plot settings
    map_opts = dict(show_grid=False, show_legend=False, xaxis = None, yaxis = None, width=600, height=500)
    ts_opts = dict(toolbar = None, tools=["hover"], show_title = False)

    # Build background map Elements
    tiles_background = gv.tile_sources.CartoLight #OSM
    timeseries_legend = get_separate_legend()

    # link widgets to query and build points element
    points_bind = pn.bind(
        build_points_from_query,
        scenario = streamflow_scenario,
        huc_id=huc2_selector.param.value,
        value_time_start=value_time_slider[1].param.value_start,
        value_time_end=value_time_slider[1].param.value_end,
        reference_time_single=reference_time_player.param.value,
        #value_min=0,   
        group_by=['primary_location_id','reference_time'],    
        include_metrics=include_metrics,#['primary_maximum','max_value_delta','max_value_timedelta'],    
        #metric_limits=dict(primary_maximum=(0.1, 10e6)),
        plot_metric=metric_selector.param.value,
        attribute_paths=attribute_paths,
        units=viz_units,
    )
    points_dmap = hv.DynamicMap(points_bind)

    # Define stream source as points selection from points_dmap
    point_selection = hv.streams.Selection1D(source=points_dmap)#, index=[0])

    gage_basin_bind = pn.bind(
        get_gage_basin_selected_point,
        index=point_selection.param.index,
        points_dmap = points_dmap,  
        gage_basins_gdf = gage_basins_gdf,
        )
    gage_basin_dmap = hv.DynamicMap(gage_basin_bind)
    #gage_basin_dmap.opts(line_color='k', line_width=2, fill_alpha=0)

    # link selected point to query and build timeseries element
    hyetograph_bind = pn.bind(
        build_hyetograph_from_query_selected_point,
        index=point_selection.param.index,
        points_dmap = points_dmap,          
        scenario = precip_scenario,
        reference_time_single=reference_time_player.param.value,
        value_min=0,     
        attribute_paths=attribute_paths,
        units=viz_units,
        opts = dict(ts_opts, xaxis = None, height=120, width=600),
    )
    hydrograph_bind = pn.bind(
        build_hydrograph_from_query_selected_point,
        index=point_selection.param.index,
        points_dmap = points_dmap,          
        scenario = streamflow_scenario,
        reference_time_single=reference_time_player.param.value,  #.value
        value_min=0,     
        attribute_paths=attribute_paths,
        units=viz_units,
        opts = dict(ts_opts, height=220, width=600),
    )
    
    ###### Apply styles 

    tiles_background.opts(**map_opts)
    points_dmap.opts(**map_opts, tools=['hover','tap'], colorbar=True, size=5, 
                     toolbar='above', selection_line_width=5, nonselection_line_width=0, nonselection_alpha=0.5)

    ###### Panel header

    header = pn.Row(
                pn.pane.PNG('https://ciroh.ua.edu/wp-content/uploads/2022/08/CIROHLogo_200x200.png', width=60),
                pn.pane.HTML(f"CIROH Tools for Exploratory Evaluation in Hydrology Research (TEEHR) \
                             ----Example 2: Explore by Reference Time<br> - {scenario_text}", 
                             sizing_mode="stretch_width", styles={'font-size': '18px', 'font-weight': 'bold'}),
    )
    # Build the Panel layout
    layout = \
        pn.Column(
            pn.Column(pn.Spacer(height=10), header, width=1100),
            pn.Row(
                pn.Column(pn.Spacer(height=20), metric_selector, width=200),
                pn.Column(pn.Spacer(height=20), huc2_selector, width=200),
                pn.Spacer(width=40),
                pn.Column(current_ref_time, reference_time_player),
                pn.Spacer(width=40), timeseries_legend,
            ),
            pn.Row(
                tiles_background*points_dmap*gage_basin_dmap, 
                pn.Column(hyetograph_bind, hydrograph_bind),
             )
    )
    
    return layout

    
def get_dashboard_header(subtitle: str) -> pn.Row:
    
    header = pn.Row(
            pn.pane.PNG('https://ciroh.ua.edu/wp-content/uploads/2022/08/CIROHLogo_200x200.png', width=60),
            pn.pane.HTML(f"CIROH Tools for Exploratory Evaluation in Hydrology Research (TEEHR) \
                         ----{subtitle}", 
                         sizing_mode="stretch_width", styles={'font-size': '18px', 'font-weight': 'bold'}),
    )
    return header
    

def read_root_dir(config_filepath: Path) -> Path:

    parsed_json = read_config_settings(config_filepath)
    
    return Path(parsed_json['PROTOCOL_ROOT'])
    
def read_config_settings(config_filepath: Path) -> dict:

    with open(config_filepath) as file:
        file_contents = file.read()
    parsed_json = json.loads(file_contents)
  
    return parsed_json
    
def read_event_definitions(event_def_path: Path) -> dict:

    # read prior event settings

    try:
        with open(event_def_path) as file:
            file_contents = file.read()
        parsed_json = json.loads(file_contents)
    except:
        parsed_json = {}
  
    return parsed_json
    
def write_event_definitions(event_def_path: Path, event_definitions: dict):

    with open(event_def_path, "w") as outfile:
        json.dump(event_definitions, outfile, indent=4)
  
    
def get_default_event() -> dict:

    default_event_specs = {
        'start_date' : dt.datetime.now().date(),
        'end_date' : dt.datetime.now().date(),
        'huc2_list' : [],
        'lat_limits' : (20, 55),
        'lon_limits' : (-130, -60),
        'name' : 'YYYYMM_name'
        }
        
    return default_event_specs
    
def get_existing_event(
    existing_events: dict, 
    select_event: pn.widgets.Select
    ) -> dict:

    event_specs = existing_events[select_event.value].copy()
    event_specs['start_date'] = dt.datetime.strptime(event_specs['start_date'], "%Y%m%d").date()
    event_specs['end_date'] = dt.datetime.strptime(event_specs['end_date'], "%Y%m%d").date()   
    event_specs['name'] = select_event.value
    event_specs['lat_limits'] = tuple(event_specs['lat_limits'])
    event_specs['lon_limits'] = tuple(event_specs['lon_limits'])
    
    return event_specs
    

def update_event_definitions(
    existing_events: dict,
    event_specs: dict,
    ) -> dict:
       
    event_name = event_specs['name']
    event_specs.pop("name")
    start_date = event_specs['start_date']
    end_date = event_specs['end_date']
    event_specs['start_date'] = dt.datetime.combine(start_date, dt.time(hour=0)).strftime("%Y%m%d")
    event_specs['end_date'] = dt.datetime.combine(end_date, dt.time(hour=0)).strftime("%Y%m%d")
    event_specs['lat_limits'] = list(event_specs['lat_limits'])
    event_specs['lon_limits'] = list(event_specs['lon_limits'])

    add_event = {event_name : event_specs}    
    
    return existing_events | add_event


    
############# region of interest selection utilities

def get_selected_huc2_list(
    huc2: gpd.GeoDataFrame,
    sel: list,
    ) -> list:
    
    huc2_list = huc2.iloc[sel].index.to_list()
    #if not huc2_list:
    #    print("No HUC2 is selected - filtering features based on Lat/Lon limits only")
    
    return huc2_list
    
    
def get_lat_lon_box_from_limits(
    lat_lim: pn.widgets.slider.IntRangeSlider,
    lon_lim: pn.widgets.slider.IntRangeSlider,
    ) -> Polygon:
    
    box_vertices = [[lon_lim[0], lat_lim[0]], 
                    [lon_lim[0], lat_lim[1]], 
                    [lon_lim[1], lat_lim[1]], 
                    [lon_lim[1], lat_lim[0]]]    
                    
    return Polygon(box_vertices)    
    

def get_usgs_id_list_as_str(
    huc2_list: list,
    latlon_box: Polygon,
    point_gdf: gpd.GeoDataFrame,    
    point_huc12_crosswalk: pd.DataFrame,
    ) -> list[str]:
    
    points_selected_gdf = get_point_features_selected(huc2_list, latlon_box, point_gdf, point_huc12_crosswalk)
    usgs_ids = [s.replace('usgs-','') for s in points_selected_gdf['id']] 
        
    return usgs_ids

def get_nwm_id_list_as_int(
    nwm_gdf: gpd.GeoDataFrame,    
    nwm_huc12_crosswalk: pd.DataFrame,
    ) -> list[int]:
    
    nwm_version_prefix = nwm_huc12_crosswalk['primary_location_id'][0].split('-')[0] + '-'  
    nwm_ids = list(map(int, [s.replace(nwm_version_prefix,'') for s in nwm_gdf['id'].to_list()]))     

    return nwm_ids
    
def get_nwm_subset_by_latlonbox(
    huc2_list: list,
    latlon_box: Polygon,
    nwm_gdf: gpd.GeoDataFrame,    
    nwm_huc12_crosswalk: pd.DataFrame,
    usgs_ids: list[str],
    usgs_nwm_crosswalk: pd.DataFrame,  
    reach_set: str,
    ) -> list[int]:
    
    nwm_version_prefix = nwm_huc12_crosswalk['primary_location_id'][0].split('-')[0] + '-'
    usgs_ids_with_prefix = ['-'.join(['usgs', s]) for s in usgs_ids]    
    
    if reach_set == 'all reaches':
        points_selected_gdf = get_point_features_selected(huc2_list, latlon_box, nwm_gdf, nwm_huc12_crosswalk)
    else:
        nwm_ids_with_prefix = get_crosswalked_id_list(usgs_ids_with_prefix, usgs_nwm_crosswalk, input_list_column = 'primary_location_id')      
        points_selected_gdf = nwm_gdf[nwm_gdf['id'].isin(nwm_ids_with_prefix)]
        
    print(f"{len(points_selected_gdf)} features selected")

    return points_selected_gdf

def get_nwm_subset_by_huc10s(
    huc10_list: list,
    nwm_gdf: gpd.GeoDataFrame,    
    nwm_huc12_crosswalk: pd.DataFrame,
    usgs_ids: list[str],
    usgs_nwm_crosswalk: pd.DataFrame,  
    reach_set: str,
    ) -> list[int]:
    
    nwm_version_prefix = nwm_huc12_crosswalk['primary_location_id'][0].split('-')[0] + '-'
    usgs_ids_with_prefix = ['-'.join(['usgs', s]) for s in usgs_ids]    
    
    if reach_set == 'all reaches':
        nwm_huc10_crosswalk = nwm_huc12_crosswalk.copy()
        nwm_huc10_crosswalk['secondary_location_id'] = nwm_huc12_crosswalk['secondary_location_id'].str.replace('huc12','huc10').str[:16]
        nwm_in_huc10_list = get_crosswalked_id_list(huc10_list, nwm_huc10_crosswalk, 'secondary_location_id')
        points_selected_gdf = nwm_gdf[nwm_gdf['id'].isin(nwm_in_huc10_list)]        
    else:
        nwm_ids_with_prefix = get_crosswalked_id_list(usgs_ids_with_prefix, usgs_nwm_crosswalk, input_list_column = 'primary_location_id')      
        points_selected_gdf = nwm_gdf[nwm_gdf['id'].isin(nwm_ids_with_prefix)]
        
    print(f"{len(points_selected_gdf)} features selected")

    return points_selected_gdf
    

def get_point_features_selected(
    huc2_list: list,
    latlon_box: Polygon,
    point_gdf: gpd.GeoDataFrame,    
    point_huc12_crosswalk: pd.DataFrame,
    ) -> gpd.GeoDataFrame:
    
    # subset crosswalk by huc2
    huc2_strings = ['-'.join(['huc12', a]) for a in huc2_list]
    point_huc12_crosswalk_subset1 = point_huc12_crosswalk[point_huc12_crosswalk['secondary_location_id'].str.contains('|'.join(huc2_strings))]
    
    # subset geometry by latlon box
    point_gdf_subset = point_gdf[latlon_box.contains(point_gdf['geometry'])]    
    
     # get the intersection of the two subsets
    point_gdf_subset2 = point_gdf_subset[point_gdf_subset['id'].isin(point_huc12_crosswalk_subset1['primary_location_id'])].copy()
    
    return point_gdf_subset2.to_crs(3857)
    

def get_hucs_selected(
    huc2_list: list,
    latlon_box: Polygon,
    huc_gdf: gpd.GeoDataFrame,    
    huc_level: int = 10,
    ) -> list:
    
    # subset hucX geometry by huc2
    huc_level_str = 'huc' + str(huc_level).zfill(2)
    huc2_strings = ['-'.join([huc_level_str, a]) for a in huc2_list]
    huc_gdf_subset = huc_gdf[huc_gdf['id'].str.contains('|'.join(huc2_strings))].copy()
    
    # further subset geometry by latlon box (where polygon centroid falls within box)
    huc_gdf_subset['centroid'] = huc_gdf_subset['geometry'].to_crs(3857).centroid.to_crs(4326)
    huc_gdf_subset = huc_gdf_subset[latlon_box.contains(huc_gdf_subset['centroid'])]  
    
    print(f"{len(huc_gdf_subset)} HUC{huc_level} polygons selected")
    
    return huc_gdf_subset[['id','name','geometry']].to_crs(3857)
    
    
def get_crosswalked_id_list(
    id_list: list,
    crosswalk: pd.DataFrame,
    input_list_column: str = 'primary_location_id'
    ) -> list:
      
    if id_list:
        if input_list_column == 'primary_location_id':
            lookup_column = 'secondary_location_id'
        else:
            lookup_column = 'primary_location_id'
            
        crosswalk_subset = crosswalk[crosswalk[input_list_column].isin(id_list)]
        lookup_list = crosswalk_subset[lookup_column].to_list()
        
    else:
        lookup_list = []
        
    return lookup_list 
    
    
def get_outer_bound(
    points: Union[gpd.GeoDataFrame, None] = None,
    polys: Union[gpd.GeoDataFrame, None] = None
    ) -> Polygon:
    
    point_coord_list = []
    poly_centroid_coord_list = []
    
    if points is not None:
        points = points.to_crs(3857)
        point_coord_list = [(x,y) for x,y in zip(points['geometry'].x , points['geometry'].y)]
    
    if polys is not None:   
        polys = polys.to_crs(3857)
        polys['centroid'] = polys['geometry'].centroid
        poly_centroid_list = [(x,y) for x,y in zip(polys['centroid'].x , polys['centroid'].y)]
        
    mp = MultiPoint(point_coord_list + poly_centroid_list)
    
    return mp.convex_hull
    
def adj_reftime_end(
    reftime_end: dt.date,
    configuration: str, 
    ) -> dt.date:
    
    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    adj_end = reftime_end
    
    if isinstance(reftime_end, dt.date):
        #assume last ref time on the date
        adj_end = get_last_ref_time(reftime_end, configuration)
    else:
        adj_end = reftime_end
        
    if adj_end > now:
        if configuration == 'medium_range_mem1':
            if now.hour < 6:
                adj_end = (now - dt.timedelta(days=1)).replace(hour=18)
            if now.hour > 6:
                adj_end = now.replace(hour=0)
            if now.hour > 12:
                adj_end = now.replace(hour=6)
            if now.hour > 18:
                adj_end = now.replace(hour=12)
        elif configuration == 'short_range':
            adj_end = now - dt.timedelta(hours=2)
    
    return adj_end
    
def adj_valtime_end(
    valtime_end: dt.date,
    configuration: str, 
    ) -> dt.date:
    
    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    adj_end = valtime_end
    
    if isinstance(valtime_end, dt.date):
        #assume last hour on the date
        adj_end = dt.datetime.combine(valtime_end, dt.time(hour=23))
    else:
        adj_end = valtime_end
        
    if adj_end > now:
        adj_end = now - dt.timedelta(hours=1)
        if 'extend' in configuration:
            if now.hour < 19:
                adj_end = (now - dt.timedelta(days=2))
            else:
                adj_end = (now - dt.timedelta(days=1))
    
    return adj_end
    
def add_valtime_hour(
    valtime_end: dt.date,
    ) -> dt.date:
    
    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    adj_end = dt.datetime.combine(valtime_end, dt.time(hour=now.hour-1))
    
    return adj_end

def list_nwm_dates_for_event_dates(
    event_start_date: dt.date,
    event_end_date: dt.date,
    ) -> dict[dt.datetime]:

    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        
    srf = get_nwm_dates_for_event_dates('short_range', event_start_date, event_end_date)
    srf_val_time_end = srf['reference_time_end'] + dt.timedelta(hours=18)
    mrf = get_nwm_dates_for_event_dates('medium_range_mem1', event_start_date, event_end_date)
    mrf_val_time_end = mrf['reference_time_end'] + dt.timedelta(days=10)

    if srf_val_time_end > now:
        srf_val_time_end = now
    if mrf_val_time_end > now:
        mrf_val_time_end = now

    date_strings = dict( 
        srf_ref = f"Short range forecasts references times that overlap with event dates (through current): {srf['reference_time_start']} through {srf['reference_time_end']}",
        srf_val = f"Short range forecasts valid times that overlap with event dates (through current): {srf['value_time_start']} through {srf_val_time_end}",
        mrf_ref = f"Medium range forecasts that overlap with event dates (through current): {mrf['reference_time_start']} through {mrf['reference_time_end']}",
        mrf_val = f"Medium range forecasts valid times that overlap with event dates (through current): {mrf['value_time_start']} through {mrf_val_time_end}")

    return date_strings


def get_nwm_dates_for_event_dates(
    configuration: str,
    event_start_date: dt.date,
    event_end_date: dt.date,
    ) -> dict[dt.datetime]:

    # get dates
    if configuration == 'medium_range_mem1':
        reference_time_start = dt.datetime.combine(event_start_date, dt.time(hour=0)) - dt.timedelta(days=10)
        reference_time_end = dt.datetime.combine(event_end_date, dt.time(hour=18))
    elif configuration == 'short_range':
        reference_time_start = dt.datetime.combine(event_start_date, dt.time(hour=0)) - dt.timedelta(hours=17)
        reference_time_start = reference_time_start.replace(hour=0)
        reference_time_end = dt.datetime.combine(event_end_date, dt.time(hour=23))
        reference_time_end = reference_time_end.replace(hour=23)
        
    adj_ref_end = adj_reftime_end(reference_time_end, configuration)
    
    value_time_start = reference_time_start
    value_time_end = get_last_value_time(adj_ref_end, configuration)      
    
    # store in dictionary
    nwm_dates = dict(
        reference_time_start = reference_time_start,
        reference_time_end = adj_ref_end,
        value_time_start = value_time_start,
        value_time_end = value_time_end,
    )    

    return nwm_dates

def get_load_dates_from_event_dates(
    configuration: str,
    start_date: dt.date,
    end_date: dt.date,
    ) -> dict[dt.datetime]:

    nwm_dates = get_nwm_dates_from_event_dates(configuration, start_date, end_date)
    
    print(f"Forecasts that overlap with event dates {event_start_date} through {event_end_date} will be loaded. \nThis includes {configuration} forecasts with reference times on \
    {nwm_dates['reference_time_start']} through {nwm_dates['reference_time_end']}")
          
    now = dt.datetime.now()
    if reference_time_end > now or value_time_end > now:
        print(f"\033[1m!Warning\033[0m - Some forecasts and/or observations are in the future. Only available data will be loaded (return to load more later)\n") 
    else:
        print("\n")
    
    return nwm_dates 
 
def get_n_ref_times(ndays: int, config: str) -> int:

    if config == 'medium_range':
        n_cycles_per_day = 4
    elif config == 'short_range':
        n_cycles_per_day = 24

    return n_cycles_per_day
    
def get_last_ref_time(date: dt.date, config: str) -> dt.datetime:
    
    if config == 'medium_range_mem1':
        last_ref_time = dt.datetime.combine(date, dt.time(hour=18))
    elif config == 'short_range':
        last_ref_time = dt.datetime.combine(date, dt.time(hour=23))  
    
    return last_ref_time       
    
    
def get_last_value_time(date: dt.date, config: str) -> dt.datetime:
    
    last_ref_time = get_last_ref_time(date, config)
    
    if config == 'medium_range_mem1':
        last_value_time = last_ref_time + dt.timedelta(days=10)
    elif config == 'short_range':
        last_value_time = last_ref_time + dt.timedelta(hours=18)
    
    return last_value_time
    

def validate_dates(
    start_date: Union[dt.date, dt.datetime], 
    end_date: Union[dt.date, dt.datetime],
    configuration: str,
    date_type: str,
    ):
    
    adj_end = end_date
    
    if start_date is None or end_date is None:
        raise ValueError(f"Dates must be selected: start date is {start_date}, end date is {end_date}")
    
    if start_date > end_date:
        raise ValueError(f"Invalid dates selected: start date {start_date} is greater than end date {end_date}")
    
    if start_date > dt.datetime.now().date():
        raise ValueError(f"Invalid dates selected: Start date {start_date} cannot be in the future")
 
    if end_date > dt.datetime.now().date():
    
        if date_type == 'reference':
            print(f'Warning! Requested reference end date {end_date} is in the future and has been adjusted to today')
            adj_end = adj_reftime_end(end_date, configuration)
        else:
            print(f'Warning! Requested valid end date {end_date} is in the future and has been adjusted to today')
            adj_end = adj_valtime_end(end_date, configuration)   
            
    elif end_date == dt.datetime.now().date() and 'extend' in configuration:
        adj_end = adj_valtime_end(end_date, configuration) 
            
    print(f"{date_type} start date: {start_date}, end date: {adj_end}")
            
    return adj_end
    
def get_n_obs_days_minus_future(
    start_date: Union[dt.date, dt.datetime], 
    n_days: int,
    ) -> int:
    
    if (start_date + dt.timedelta(n_days)) > dt.datetime.now():
        n_days = (dt.datetime.now().date() - start_date.date()).days + 1
 
    return n_days
 
 
def get_nwm_conus_forecast_configs() -> list:

    nwm_conus_forecast_configs = [
    'short_range', 
    'medium_range_mem1',
    ]
    return nwm_conus_forecast_configs

def select_event_widgets(huc2_gdf, states_gdf, existing_events, select_event_name):

    if select_event_name.value == 'define new event':
        event_specs = get_default_event()
        selected_huc2s = []
    else:
        event_specs = get_existing_event(existing_events, select_event_name)
        selected_huc2s = huc2_gdf.loc[event_specs['huc2_list']]
        
    # create selectable map and selection widgets
    
    huc2s = gv.Polygons(huc2_gdf, vdims=['huc2'],crs=ccrs.GOOGLE_MERCATOR)
    selected_huc2s = gv.Polygons(selected_huc2s, vdims=['huc2'],crs=ccrs.GOOGLE_MERCATOR)
    states = gv.Polygons(states_gdf, vdims=['STUSPS'], crs=ccrs.GOOGLE_MERCATOR)   
    selection = hv.streams.Selection1D(source=huc2s)
    
    widgets = {
        'event_name_input' : pn.widgets.TextInput(name='Event name (YYYYMM_name):', placeholder='YYYYMM_name', value=event_specs['name']),
        'start_picker' : pn.widgets.DatePicker(name='Event Start Date:', value=event_specs['start_date']),
        'end_picker' : pn.widgets.DatePicker(name='Event End Date:', value=event_specs['end_date']),
        'lat_slider' : pn.widgets.IntRangeSlider(name='Additional Latitude Limits [optional]  ', 
                                           start=20, end=55, step=1,
                                           value=tuple(event_specs['lat_limits']))
                                           ,
        'lon_slider' : pn.widgets.IntRangeSlider(name='Additional Longitude Limits [optional]  ', 
                                           start=-130, end=-60, step=1,
                                           value=tuple(event_specs['lon_limits']))        
         }

    event_panel = pn.Column(
        pn.pane.HTML("Event Region and Dates:", styles={'font-size': '15px', 'font-weight': 'bold'}),
        pn.Row(
            pn.Column(widgets['event_name_input'], pn.Spacer(height=10), widgets['start_picker'], widgets['end_picker'], pn.Spacer(height=10), widgets['lat_slider'], widgets['lon_slider']),
            states.opts(color_index=None, fill_color='lightgray', nonselection_alpha=1, line_color='white', tools=[''], 
                        title='Select one or more HUC2 regions (hold shift to select multiple)', fontsize=12) \
            * huc2s.opts(color_index=None, fill_color='none', width=700, height=450, tools=['hover', 'tap'], selection_line_width=4) \
            * selected_huc2s.opts(color_index=None, fill_color='none', line_color='red', line_width=3),
        )    
    )   
    
    return event_panel, widgets, selection
    

def select_data_widgets():

    widgets = {
        'select_forecast_config' : pn.widgets.MultiSelect(name='NWM Forecast Configuration', 
                                                        options=['none','short_range','medium_range_mem1'],
                                                        value=['short_range']
                                                       ),
        'select_ref_start' : pn.widgets.DatePicker(name='First Ref/Issue Date to Load:', value=dt.datetime.utcnow().date()),
        'select_ref_end' : pn.widgets.DatePicker(name='Last Ref/Issue Date to Load:', value=dt.datetime.utcnow().date()),
        'select_reach_set' : pn.widgets.Select(name='NWM Reach set:', options=['gaged reaches','all reaches']),
        'select_observed_source' : pn.widgets.MultiSelect(name='Analysis/Observed Source(s)', 
                                                        options=['none', 'USGS*','analysis_assim_extend', 'analysis_assim', 'analysis_assim_extend_no_da', 'analysis_assim_no_da'], 
                                                        value=['USGS*','analysis_assim_extend']),
        'select_value_start' : pn.widgets.DatePicker(name='First Value Date to Load:', value=dt.datetime.utcnow().date()),
        'select_value_end' : pn.widgets.DatePicker(name='Last Value Date to Load:', value=dt.datetime.utcnow().date())
    }

    
    data_panel = pn.Column(
            pn.pane.HTML("Define the data to load:", styles={'font-size': '15px', 'font-weight': 'bold'}),
            pn.Row(
                pn.Column(widgets['select_forecast_config'], widgets['select_ref_start'], widgets['select_ref_end']),
                pn.Column(widgets['select_observed_source'], widgets['select_value_start'], widgets['select_value_end'],
                         pn.pane.HTML("*USGS and no_da ignored for forcing", styles={'font-size': '12px'})),
                widgets['select_reach_set'],
            ),
            pn.Spacer(height=25)
    )
    
    return data_panel, widgets
        
