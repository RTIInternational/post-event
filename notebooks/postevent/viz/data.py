'''
TEEHR calls, and other evaluation data and metric utilities
'''

import pandas as pd
import geopandas as gpd
import numpy as np

from typing import List, Union
from pathlib import Path

import teehr.queries.duckdb as tqd
import teehr.queries.utils as tqu

from ..utils import convert
from .. import config

def build_teehr_filters(
    joined_query: bool = True,
    location_id: Union[str, List[str], None] = None,    
    location_column_prefix: Union[str, None] = "primary",
    huc_id: Union[str, List[str], None] = None,
    primary_huc_crosswalk_filepath: Union[Path,None] = None,
    order_limit: Union[int, None] = None,
    stream_order_filepath: Union[Path,None] = None,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None,
    attribute_paths: Union[dict, None] = None,
) -> List[dict]:
    
    '''
    Utility to build the TEEHR filter dictionary based on
    some COMMON user specifications
    
    Parameters
    ----------
    joined_query: bool = True
        True adds a prefix (defined by location_column_prefix) to 
        location_id column for location filters and adds both prefixes 
        (primary and secondary) to value columns for value_min and 
        value_max filters
    location_id: Union[str, List[str], None] = None
        String or list of strings corresponding to location identifiers 
        for the primary dataset (observed, analysis, gages, etc.)
    location_column_prefix: Union[str, None] = "primary"
        If joined query, prefix of the location_id column that should 
        be filtered - e.g., "primary" and "secondary".  Ignored if 
        joined_query = False  
    huc_id: Union[str, List[str], None] = None
        String or list of strings corresponding to HUC polygon 
        identifiers that are used to extract the subset of primary 
        location ids located within the defined set of HUC polygons. 
        HUC level determined by string lengths.
    primary_huc_crosswalk_filepath: Union[Path,None] = None
        Path to crosswalk file between primary data 
        (points or polygons) and hucs, used only if huc_id is included
    order_limit: Union[int, None] = None
        Maximum stream order of locations (primary) to include 
        *attribute_paths must include "usgs_stream_order"
        **currently only works if primary dataset is USGS
    stream_order_filepath: Union[Path,None] = None
        Path to stream order attribute file, used only if order_limit 
        is included
    value_time_start: Union[pd.Timestamp, None] = None
        First value time of data to include 
        (default None applies no lower limit on value time)
    value_time_end: Union[pd.Timestamp, None] = None
        Last value time of data to include
        (default None applies no upper limit on value time)
    reference_time_single: Union[pd.Timestamp, None] = None
        Single reference time of data to include
        *Overrides reference_time_start and ..._end
    reference_time_start: Union[pd.Timestamp, None] = None
        First reference time of data to include 
        (default None applies no lower limit on reference time)    
    reference_time_end: Union[pd.Timestamp, None] = None
        Last reference time of data to include
        (default None applies no upper limit on reference time)
    value_min: Union[float, None] = None
        Minimum data value to include
        *applies to BOTH primary and secondary datasets 
        (e.g., value_min = 0 will return data where both primary and
        secondary are >=0)
    value_max: Union[float, None] = None
        Maximum data value to include
        *applies to BOTH primary and secondary datasets 
        (e.g., value_max = 100 will return data where both primary and
        secondary are <= 100)   
    attribute_paths: Union[dict, None] = None
        dictionary of paths for attribute data that are required for
        any of the above filter builders (huc_id, order_limit)

        
    Returns
    -------
    results : List(dict)
        List of dictionaries describing the "where" clause to limit 
        data that is included in metric or timeseries queries
        
    '''

    filters=[]    
    
    if joined_query:
        location_column_prefix = location_column_prefix + "_"
    else:
        location_column_prefix = ''
    
    if huc_id is not None:        
        if type(huc_id) is not list:
            huc_id = [huc_id]
        
        if 'all' not in huc_id:
            if primary_huc_crosswalk_filepath is None:
                raise ValueError("crosswalk not provided, crosswalk " \
                                 "required for huc-based location filter")
            else:   
                crosswalk = pd.read_parquet(primary_huc_crosswalk_filepath)
                prefix = crosswalk['primary_location_id'][0].split("-")[0]
                
                # if the primary data are hucs, use a like operator
                if prefix[:3] == 'huc':
                    for huc_id_x in huc_id:    
                        filters.append(
                            {
                                "column": location_column_prefix + "location_id",
                                "operator": "like",
                                #"value": f"huc10-{huc_id_x}%"
                                "value": f"{huc_id_x}%"
                            }
                        )
                # otherwise use the crosswalk to get a location list
                else:
                    location_list = get_locations_within_huc(crosswalk, huc_id)
                    filters.append(
                        {
                            "column": location_column_prefix + "location_id",
                            "operator": "in",
                            "value": location_list
                        }
                    )
    if location_id is not None:    
        if type(location_id) is str:
            filters.append(
                {
                    "column": location_column_prefix + "location_id",
                    "operator": "like",
                    "value": f"{location_id}%"
                }
            )
        elif type(location_id) is list:
            filters.append(
                {
                    "column": location_column_prefix + "location_id",
                    "operator": "in",
                    "value": location_id
                }
            )       
            
    # stream order
    if order_limit is not None:
        if type(order_limit) is int:
            if stream_order_filepath is None:
                raise ValueError("stream order attributes not provided " \
                                 "but required for stream order filter")
            else:
                stream_order = pd.read_parquet(stream_order_filepath)
                location_list = get_locations_below_order_limit(
                    stream_order, 
                    order_limit
                )
                filters.append(
                    {
                        "column": location_column_prefix + "location_id",
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
        if joined_query:
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
        else:
            filters.append(
                {
                    "column": "value",
                    "operator": ">=",
                    "value": value_min
                }
            )
            
    if value_max is not None:
        if type(value_min) is str:
            value_min = float(value_min.split(' ')[0])  
        if joined_query:
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
        else:
            filters.append(
                {
                    "column": "value",
                    "operator": "<=",
                    "value": value_max
                }
            )
        
    return filters

def get_locations_within_huc(
    crosswalk: pd.DataFrame,
    huc_id: Union[list[str], str] = '01', 
) -> List[str]:

    '''
    Get list of [defined source] locations within a HUC or list of HUCs
    
    Parameters
    ----------
    crosswalk: Union[Path,None] = None
        Path to crosswalk between the point locations and hucs 
        (e.g., "usgs_huc_crosswalk", or "nwmXX_huc_crosswalk")    
    huc_id: Union[list[str], str] = '01'
        String or list of strings corresponding to HUC identifiers 
        of any level, HUC level is determined by length of the strings,
        *all HUCs in a list must be the same HUC level
    
    Returns
    -------
    returns: List[str]
        List of IDs as strings with source prefix (e.g., "usgs-")
    
    '''
    
    if type(huc_id) == list:
        huc_level = len(huc_id[0])
    else:
        huc_level = len(huc_id)

    crosswalk['huc'] = crosswalk['secondary_location_id'].str[6:6+huc_level]
    prefix = crosswalk['primary_location_id'][0].split("-")[0]

    if 'all' not in huc_id:
        location_list = crosswalk[crosswalk['huc'].isin(huc_id)][
            'primary_location_id'
        ].to_list()
    else:
        location_list = crosswalk['primary_location_id'].to_list()

    if not location_list:
       print(f"Warning, no locations found in {huc_id}")     
        
    return location_list

def get_locations_below_order_limit(
    stream_order: pd.DataFrame,
    order_limit: int = 10,
) -> List[str]:
    
    '''
    Get list of locations with stream order equal to or less than a limit
    
    Parameters
    ----------
    stream_order: Union[Path,None] = None
        Relevant stream order attribute dataframe
    order_limit: int = 10
        Maximum stream order of locations to include

    Returns
    -------
    returns: List[str]
        List of IDs as strings with source prefix (e.g., "usgs-")
    
    '''
    location_list = stream_order[
        stream_order['attribute_value'] <= order_limit
    ]['location_id'].to_list()
    if not location_list:
       print(f"Warning, no locations found with stream order <= {order_limit}")
        
    return location_list

def get_ids_in_parquet_for_date_range(
    filepath: Path, 
    start_date: pd.Timestamp, 
    end_date: pd.Timestamp
) -> List[str]:

    filters = [{
        "column": "value_time",
        "operator": ">=",
        "value": f"{start_date}"
        },
        {
        "column": "value_time",
        "operator": "<=",
        "value": f"{end_date}"
        },
        {
        "column": "value",
        "operator": ">=",
        "value": 0
        }]

    df = tqd.get_timeseries(
        timeseries_filepath = filepath,
        filters=filters,
        order_by=['location_id','value_time'],
        return_query=False,   
    )     
    ids = list(df['location_id'].unique())
    
    return ids


########## teehr queries wrappers and utilities specifically for postevent - 
########## some of this functionality may eventually be added to teehr

def teehr_get_precip_metrics(
    paths: config.Paths, 
    event: config.Event, 
    dates: config.Dates, 
    polygons='huc10', 
) -> gpd.GeoDataFrame:
    
    if polygons == 'huc10':
        forcing_filepaths = paths.forcing_filepaths
        location_id_list = event.huc10_list
    elif polygons == 'usgs_basins':
        forcing_filepaths = paths.alt_forcing_filepaths
        location_id_list = ['usgs-' + s for s in event.usgs_id_list]
    else:
        raise ValueError(f"invalid MAP polygon layer {map_polygons}")
        
    filters = build_teehr_filters(   
        location_id=location_id_list,
        primary_huc_crosswalk_filepath=forcing_filepaths['crosswalk_filepath'],
        reference_time_start=dates.ref_time_start,
        reference_time_end=dates.ref_time_end,
        value_time_start=dates.analysis_time_start,
        value_time_end=dates.analysis_time_end,
        value_min=0)
            
    query_gdf = tqd.get_metrics(
        forcing_filepaths['primary_filepath'],
        forcing_filepaths['secondary_filepath'],
        forcing_filepaths['crosswalk_filepath'],        
        group_by=['primary_location_id','reference_time', 'measurement_unit'],
        order_by=['primary_location_id','reference_time'], 
        filters=filters,
        return_query=False,
        geometry_filepath=forcing_filepaths['geometry_filepath'],       
        include_geometry=True,
        include_metrics=[
            'primary_sum',
            'secondary_sum', 
            'primary_maximum', 
            'secondary_maximum',
            'primary_count',
            'secondary_count', 
        ],
    )    
    if query_gdf.empty:
        raise ValueError("TEEHR precipitation metrics query returned empty - "\
                         "confirm requested event data exists in parquet files")
    
    gdf = convert.convert_query_units(query_gdf, paths.units, 'precipitation')
    gdf['sum_diff'] = gdf['secondary_sum'] - gdf['primary_sum']
    
    return gdf

def teehr_get_obs_precip_total(
    paths: config.Paths, 
    event: config.Event, 
    dates: config.Dates, 
    polygons='huc10', 
) -> gpd.GeoDataFrame:

    if polygons == 'huc10':
        forcing_filepaths = paths.forcing_filepaths
        location_id_list = event.huc10_list
    elif polygons == 'usgs_basins':
        forcing_filepaths = paths.alt_forcing_filepaths
        location_id_list = ['usgs-' + s for s in event.usgs_id_list]
    else:
        raise ValueError(f"invalid MAP polygon layer {map_polygons}")
        
    filters = build_teehr_filters(
        joined_query=False,
        location_id=location_id_list,
        primary_huc_crosswalk_filepath=forcing_filepaths['crosswalk_filepath'],
        value_time_start=dates.analysis_time_start,
        value_time_end=dates.analysis_time_end,
        value_min=0)
    
    query_df = tqd.get_timeseries_chars(
        forcing_filepaths['primary_filepath'],  
        group_by=['location_id','measurement_unit'],
        order_by=['location_id','measurement_unit'],
        filters=filters,
        return_query=False,
    )        
    if query_df.empty:
        raise ValueError("TEEHR observed precipitation "\
                         "timeseries query returned "\
                         "empty - confirm requested event data "\
                         "exists in parquet files")
        
    df = convert.convert_query_units(
        query_df, 
        paths.unit_selector.value, 
        'precipitation'
    )

    # add geometry
    geom = gpd.read_parquet(forcing_filepaths['geometry_filepath'])
    gdf = df[['location_id','sum','count']].merge(
        geom[['id','geometry']], 
        how='left', 
        left_on='location_id', 
        right_on='id'
    ).drop('id', axis=1)
    gdf = gpd.GeoDataFrame(gdf)
    
    return gdf

def teehr_get_obs_precip_timeseries(
    ts_poly_id, 
    paths: config.Paths, 
    dates: config.Dates, 
    polygons='huc10'
) -> pd.DataFrame():

    if polygons == 'huc10':
        forcing_filepaths = paths.forcing_filepaths
    elif polygons == 'usgs_basins':
        forcing_filepaths = paths.alt_forcing_filepaths
    else:
        raise ValueError(f"invalid MAP polygon layer {map_polygons}")
    
    filters = build_teehr_filters(  
        joined_query = False,
        location_id=ts_poly_id,
        value_time_start=dates.data_value_time_start,
        value_time_end=dates.data_value_time_end,
        value_min=0)
            
    query_df = tqd.get_timeseries(
        forcing_filepaths['primary_filepath'],    
        order_by=['location_id','value_time'], 
        filters=filters,
        return_query=False,
    )    
    if query_df.empty:
        raise ValueError("TEEHR observed precipitation timeseries "\
                         "query returned empty - confirm requested "\
                         "event data exists in parquet files")
        
    df = convert.convert_query_units(
        query_df, 
        paths.unit_selector.value, 
        'precipitation'
    )
    
    return df  

def teehr_get_fcst_precip_timeseries(
    ts_poly_id, 
    paths: config.Paths, 
    dates: config.Dates, 
    polygons='huc10'
) -> pd.DataFrame():

    if polygons == 'huc10':
        forcing_filepaths = paths.forcing_filepaths
    elif polygons == 'usgs_basins':
        forcing_filepaths = paths.alt_forcing_filepaths
    else:
        raise ValueError(f"invalid MAP polygon layer {map_polygons}")
        
    filters = build_teehr_filters(  
        joined_query = False,
        location_id=ts_poly_id,
        reference_time_start=dates.ref_time_start,
        reference_time_end=dates.ref_time_end,
        value_min=0)
            
    query_df = tqd.get_timeseries(
        forcing_filepaths['secondary_filepath'],    
        order_by=['location_id','reference_time','value_time'], 
        filters=filters,
        return_query=False,
    )    
    if query_df.empty:
        raise ValueError("TEEHR forecast precipitation timeseries query "\
                         "returned empty - confirm requested event data "\
                         "exists in parquet files")
        
    df = convert.convert_query_units(query_df, paths.units, 'precipitation')
    
    return df

def teehr_get_flow_metrics(
    paths: config.Paths, 
    event: config.Event, 
    dates: config.Dates, 
) -> gpd.GeoDataFrame:

    filters = build_teehr_filters(   
        location_id=['-'.join(['usgs',id]) for id in event.usgs_id_list],
        primary_huc_crosswalk_filepath=paths.streamflow_filepaths[
            'crosswalk_filepath'
        ],
        reference_time_start=dates.ref_time_start,
        reference_time_end=dates.ref_time_end,
        value_time_start=dates.analysis_time_start,
        value_time_end=dates.analysis_time_end,
        value_min=0)
     
    metric_list = [
        "primary_maximum",              
        "secondary_maximum", 
        "max_value_delta",
        "primary_max_value_time",
        "secondary_max_value_time",      
        "max_value_timedelta",
        "primary_count",
        "secondary_count",
        "primary_average",
        "secondary_average",
        "primary_minimum",
        "secondary_minimum",
        "mean_error",
        "primary_sum",
        "secondary_sum",
        ]
            
    query_gdf = tqd.get_metrics(
        paths.streamflow_filepaths['primary_filepath'],
        paths.streamflow_filepaths['secondary_filepath'],
        paths.streamflow_filepaths['crosswalk_filepath'],        
        group_by=['primary_location_id','reference_time', 'measurement_unit'],
        order_by=['primary_location_id','reference_time'], 
        filters=filters,
        return_query=False,
        geometry_filepath=paths.streamflow_filepaths['geometry_filepath'],       
        include_geometry=True,
        include_metrics=metric_list,
    )    
    if query_gdf.empty:
        raise ValueError("TEEHR streamflow metrics query returned empty "\
                         "- confirm requested event data exists in "\
                         "parquet files")
        
    gdf = convert.convert_query_units(
        query_gdf, 
        paths.unit_selector.value, 
        'streamflow'
    )

    return gdf

def teehr_get_obs_flow_chars(
    paths: config.Paths, 
    event: config.Event, 
    dates: config.Dates, 
    polygons='huc10', 
) -> gpd.GeoDataFrame:
    
    filters = build_teehr_filters(   
        joined_query=False,        
        location_id=['-'.join(['usgs',id]) for id in event.usgs_id_list],
        value_time_start=dates.analysis_time_start,
        value_time_end=dates.analysis_time_end,
        value_min=0)
    
    query_df = tqd.get_timeseries_chars(
        paths.streamflow_filepaths['primary_filepath'],
        group_by=['location_id','measurement_unit'],
        order_by=['location_id','measurement_unit'],
        filters=filters,
        return_query=False,
    )        
    if query_df.empty:
        raise ValueError("TEEHR observed streamflow timeseries query "\
                         "returned empty - confirm requested event data "\
                         "exists in parquet files")
        
    df = convert.convert_query_units(
        query_df, 
        paths.unit_selector.value, 
        'streamflow'
    )

    #add geometry
    geom = gpd.read_parquet(paths.streamflow_filepaths['geometry_filepath'])
    gdf = df.merge(
        geom[['id','geometry']], 
        how='left', 
        left_on='location_id', 
        right_on='id'
    ).drop('id', axis=1)
    gdf = gpd.GeoDataFrame(gdf)
    
    return gdf

def add_flow_exceedence(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    # add threshold exceedence and matrix
    gdf['obs_exceed'] = gdf['primary_maximum'] > gdf['hw_threshold']
    gdf['fcst_exceed'] = gdf['secondary_maximum'] > gdf['hw_threshold']
    
    #TN, true neg
    gdf['contingency_matrix'] = 0  
    #TP, hit
    gdf.loc[
        (gdf['obs_exceed'] == gdf['fcst_exceed']) & gdf['obs_exceed'],
        'contingency_matrix'
    ] = 1  
    #FN, miss
    gdf.loc[
        (gdf['obs_exceed'] != gdf['fcst_exceed']) & gdf['obs_exceed'],
        'contingency_matrix'
    ] = 2  
    #FP, false alarm
    gdf.loc[
        (gdf['obs_exceed'] != gdf['fcst_exceed']) & ~gdf['obs_exceed'],
        'contingency_matrix'
    ] = 3 
    return gdf

def add_prior_signal_time(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    # get the 'warning hours', i.e the number of hours from
    # forecast issue to the max value (peak) 
    gdf['secondary_peak_timestep'] = (
        (gdf['secondary_max_value_time'] - gdf['reference_time']) \
        / pd.Timedelta(hours=1)
    ).astype('int')
    gdf['primary_peak_timestep'] = (
        (gdf['primary_max_value_time'] - gdf['reference_time']) \
        / pd.Timedelta(hours=1)
    ).astype('int')

    # prior_hw_signal is True if it is a 'useful' hit, i.e.
    # forecast was issued prior to a threshold-exceeding observed peak 
    # that contained a threshold-exceeding forecast flow 
    # IGNORES TIMING ERROR OF THE FORECAST EXCEEDANCE
    gdf['prior_hw_signal'] = np.where(
        ((gdf['primary_peak_timestep'] > 1) & (gdf['contingency_matrix'] == 1)), 
        True, 
        False
    )

    # get the warning time provided by 'useful hits' (zero if not a useful hit)
    # the amount of time prior to an observed threshold-exceeding peak
    # that a forecast was issued containing a threshold-exceeding forecast flow
    # IGNORES TIMING ERROR OF THE FORECAST EXCEEDANCE
    gdf['prior_hw_signal_time'] = np.where(
        gdf['prior_hw_signal'], 
        gdf['primary_peak_timestep'], 
        0
    )
    return gdf

    # design/add other utility metrics that factor in timing error and latency

def add_percent_difference(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    gdf['peak_percent_diff'] = (gdf['max_value_delta']) \
                           * 100 / gdf['primary_maximum']
    gdf['vol_percent_diff'] = (gdf['secondary_sum'] - gdf['primary_sum']) \
                           * 100 / gdf['primary_sum']
    gdf['peak_time_diff_hours'] = (
        gdf['max_value_timedelta'] / np.timedelta64(1, 'h')
    ).astype(int)
    
    return gdf

def add_normalized_peakflow(
    gdf: gpd.GeoDataFrame, 
    paths: config.Paths
) -> gpd.GeoDataFrame:
    
    if 'primary_maximum' in gdf.columns:
        gdf = calc_normalized_flow(
            gdf, 'primary_maximum', 
            paths.units, 
            gdf['drainage_area']
        )
        gdf = calc_normalized_flow(
            gdf, 
            'secondary_maximum', 
            paths.units, 
            gdf['drainage_area']
        )
        gdf['max_norm_diff'] = gdf['secondary_maximum_norm'] \
                               - gdf['primary_maximum_norm']
    if 'max' in gdf.columns:
        gdf = calc_normalized_flow(
            gdf, 
            'max', 
            paths.unit_selector.value, 
            gdf['drainage_area']
        )
        
    return gdf

def add_normalized_volume(
    gdf: gpd.GeoDataFrame, 
    paths: config.Paths
) -> gpd.GeoDataFrame:
    
    if 'primary_sum' in gdf.columns:
        gdf = calc_normalized_flow(
            gdf, 
            'primary_sum', 
            paths.units, 
            gdf['drainage_area']
        )
        gdf = calc_normalized_flow(
            gdf, 
            'secondary_sum', 
            paths.units, 
            gdf['drainage_area']
        )
        gdf['vol_norm_diff'] = gdf['secondary_sum_norm'] - gdf['primary_sum_norm']
        
    if 'sum' in gdf.columns:
        gdf = calc_normalized_flow(
            gdf, 
            'sum', 
            paths.units, 
            gdf['drainage_area']
        )
    return gdf

def add_normalized_timeseries(
    gdf: gpd.GeoDataFrame, 
    paths: config.Paths,
    drainage_area: float
) -> gpd.GeoDataFrame:
    
    if 'primary_value' in gdf.columns:
        gdf = calc_normalized_flow(
            gdf, 
            'primary_value', 
            paths.units, 
            drainage_area
        )
        gdf = calc_normalized_flow(
            gdf, 
            'secondary_value', 
            paths.units, 
            drainage_area
        )
    if 'value' in gdf.columns:
        gdf = calc_normalized_flow(
            gdf, 
            'value', 
            paths.units, 
            drainage_area
        ) 
    return gdf

def calc_normalized_flow(
    df: pd.DataFrame(), 
    column: str, 
    units: str,
    drainage_area: float
) -> pd.DataFrame:
    
    if units == 'english':
        # flow units are cfs, area is mi2, convert to in/hr
        df[column + '_norm'] = df[column] \
                               / convert.convert_area_to_ft2(
                                   'mi2', drainage_area
                               ) * 12 * 3600
    else:
        # flow units are cms, area is km2, convert to mm/hr
        df[column + '_norm'] = df[column] \
                               / convert.convert_area_to_m2(
                                   'km2', drainage_area
                               ) * 1000 * 3600
    
    return df
    
def teehr_get_obs_flow_timeseries(
    location_id: str, 
    paths: config.Paths, 
    dates: config.Dates
) -> pd.DataFrame:
    
    filters = build_teehr_filters(  
        joined_query = False,
        location_id=location_id,
        value_time_start=dates.data_value_time_start,
        value_time_end=dates.data_value_time_end,
        value_min=0)
            
    query_df = tqd.get_timeseries(
        paths.streamflow_filepaths['primary_filepath'],    
        order_by=['location_id','value_time'], 
        filters=filters,
        return_query=False,
    )    
    if query_df.empty:
        raise ValueError("TEEHR observed streamflow timeseries query "\
                         "returned empty - confirm requested event data "\
                         "exists in parquet files")
        
    df = convert.convert_query_units(
        query_df, 
        paths.units, 
        'streamflow'
    )
    
    return df  

def teehr_get_noda_flow_timeseries(
    location_id: str, 
    paths: config.Paths, 
    dates: config.Dates
) -> pd.DataFrame:
    
    filters = build_teehr_filters(  
        joined_query = False,
        location_id=location_id,
        value_time_start=dates.data_value_time_start,
        value_time_end=dates.data_value_time_end,
        value_min=0)
            
    query_df = tqd.get_timeseries(
        paths.streamflow_filepaths['noda_filepath'],    
        order_by=['location_id','value_time'], 
        filters=filters,
        return_query=False,
    )    
    if query_df.empty:
        raise ValueError("TEEHR analysis streamflow timeseries query "\
                         "returned empty - confirm requested event data "\
                         "exists in parquet files")
        
    df = convert.convert_query_units(query_df, paths.units, 'streamflow')
    
    return df  

def teehr_get_fcst_flow_timeseries(
    location_id: str, 
    paths: config.Paths, 
    dates: config.Dates
) -> pd.DataFrame:
    
    filters = build_teehr_filters(  
        joined_query = False,
        location_id=location_id,
        reference_time_start=dates.ref_time_start,
        reference_time_end=dates.ref_time_end,
        value_min=0)
            
    query_df = tqd.get_timeseries(
        paths.streamflow_filepaths['secondary_filepath'],    
        order_by=['location_id','reference_time','value_time','measurement_unit'], 
        filters=filters,
        return_query=False,
    )    
    if query_df.empty:
        raise ValueError("TEEHR forecast streamflow timeseries query "\
                         "returned empty - confirm requested event data "\
                         "exists in parquet files")
        
    df = convert.convert_query_units(query_df, paths.units, 'streamflow')
    
    return df





