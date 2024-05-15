'''
location-related utilities
'''
import pandas as pd
import geopandas as gpd

from typing import List
from shapely import Polygon
from pathlib import Path

import teehr.queries.duckdb as tqd

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

def get_usgs_id_list_as_str(
    huc10_list: List[str],
    usgs_points: gpd.GeoDataFrame,
    cross_usgs_huc: pd.DataFrame,
    cross_usgs_nwm: pd.DataFrame,
    ) -> list[str]:    
    '''
    Get a list of USGS IDs as strings and without prefix 
    for input to the USGS data loading function
    '''    
    
    points_subset = get_point_features_subset_by_huc10s(
        huc10_list, 
        usgs_points, 
        cross_usgs_huc
    )
    nwm_version_list = cross_usgs_nwm['primary_location_id'].to_list()
    points_subset2 = points_subset[points_subset['id'].isin(nwm_version_list)]
    usgs_ids = [s.replace('usgs-','') for s in points_subset2['id']] 
        
    return usgs_ids

def get_nwm_id_list( 
    cross_usgs_nwm: pd.DataFrame,
    cross_nwm_huc: pd.DataFrame,
    nwm_version: str = 'nwm22',
    usgs_id_list: List[str] = [],
    huc10_list: List[str] = [],
) -> List[str]:
    '''
    Get a list of NWM feature IDs as integers and without prefix for 
    input to the NWM loading function
    List is based on
    1) crosswalk to usgs ids (if provided) or
    2) within hucs defined by 'huc2_list' attribute of event
    '''
    
    if usgs_id_list:
        usgs_ids_with_prefix = ['-'.join(['usgs', s]) for s in usgs_id_list]  
        nwm_ids_with_prefix = get_crosswalked_id_list(
            usgs_ids_with_prefix, 
            cross_usgs_nwm, 
            input_list_column = 'primary_location_id'
        )   
    else:
        print('Getting list of all nwm reaches in the selected HUCs...')            
        nwm_huc10_crosswalk = cross_nwm_huc.copy()
        nwm_huc10_crosswalk['secondary_location_id'] = \
            cross_nwm_huc['secondary_location_id'].str.replace(
                'huc12','huc10'
            ).str[:16]
        nwm_ids_with_prefix = get_crosswalked_id_list(
            huc10_list, 
            nwm_huc10_crosswalk, 
            'secondary_location_id'
        )       
        
    return nwm_ids_with_prefix

def get_nwm_id_list_as_int(
    cross_usgs_nwm: pd.DataFrame,
    cross_nwm_huc: pd.DataFrame,
    nwm_version: str = 'nwm22',
    usgs_id_list: List[str] = [],
    huc10_list: List[str] = [],
) -> List[int]:
    '''
    Get a list of NWM feature IDs as integers and without prefix 
    for input to the NWM loading function
    List is based on
    1) crosswalk to usgs ids (if provided) or
    2) within hucs defined by 'huc2_list' attribute of event
    '''
    nwm_ids_with_prefix = get_nwm_id_list(
        cross_usgs_nwm, 
        cross_nwm_huc, 
        nwm_version, 
        usgs_id_list, 
        huc10_list
    )
    nwm_version_prefix = nwm_version + '-'
    nwm_ids = list(map(
        int, 
        [s.replace(nwm_version_prefix,'') for s in nwm_ids_with_prefix]
    ))  
        
    return nwm_ids
    
def get_crosswalked_id_list(
    id_list: List[str],
    crosswalk: pd.DataFrame,
    input_list_column: str = 'primary_location_id'
) -> list:
    '''
    Get a list of IDs from one column in a crosswalk based on the other
    '''       
    if id_list:
        if input_list_column == 'primary_location_id':
            lookup_column = 'secondary_location_id'
        else:
            lookup_column = 'primary_location_id'
            
        crosswalk_subset = crosswalk[
            crosswalk[input_list_column].isin(id_list)
        ]
        lookup_list = crosswalk_subset[lookup_column].to_list()
        
    else:
        lookup_list = []
        
    return lookup_list 

def get_point_features_subset_by_huc10s(
    huc10_list: List[str],
    point_gdf: gpd.GeoDataFrame,    
    point_huc12_crosswalk: pd.DataFrame,
) -> gpd.GeoDataFrame:    
    '''
    get a subset of points within both the list of 
    HUC2s and lat/lon polygon
    '''
    
    # subset crosswalk by huc10s
    huc12_strings = [
        a.replace('huc10','huc12') for a in huc10_list
    ]
    point_huc12_crosswalk_subset = point_huc12_crosswalk[
        point_huc12_crosswalk['secondary_location_id'
    ].str.contains('|'.join(huc12_strings))]

    # get the corresponding point subset
    point_gdf_subset = point_gdf[
        point_gdf['id'].isin(
            point_huc12_crosswalk_subset['primary_location_id']
        )
    ].copy()
    
    return point_gdf_subset #to_crs(3857)
    
def get_point_features_subset(
    huc2_list: List[str],
    region_polygon: Polygon,
    point_gdf: gpd.GeoDataFrame,    
    point_huc12_crosswalk: pd.DataFrame,
) -> gpd.GeoDataFrame:    
    '''
    get a subset of points within both the list of HUC2s and 
    lat/lon polygon
    '''
    
    # subset crosswalk by huc2
    huc2_strings = ['-'.join(['huc12', a]) for a in huc2_list]
    point_huc12_crosswalk_subset = point_huc12_crosswalk[
        point_huc12_crosswalk['secondary_location_id'].str.contains(
            '|'.join(huc2_strings)
        )
    ]

    # subset geometry by latlon polygon, if any
    if region_polygon:
        point_gdf_poly_subset = point_gdf[
            region_polygon.contains(point_gdf['geometry'])
        ]    
    else:
        point_gdf_poly_subset = point_gdf
        
    # get the intersection of the two subsets
    point_gdf_subset = point_gdf_poly_subset[
        point_gdf_poly_subset['id'].isin(
            point_huc12_crosswalk_subset['primary_location_id']
        )
    ].copy()
    
    if len(point_huc12_crosswalk_subset) > 0 and \
       len(point_gdf_poly_subset) > 0 and \
       len(point_gdf_subset) == 0:
        print('Warning - no gages found in intersecting area of HUC2s '\
              'and lat-lon polygon (or the two do not overlap). Check '\
              'region selections.')
    
    return point_gdf_subset #to_crs(3857)

def get_hucx_subset(
    huc_gdf: gpd.GeoDataFrame(),
    huc2_list: List = [],
    poly_list: List[Polygon] = [Polygon()],
    box: Polygon = Polygon(),
    huc_level: int = 10,
) -> list:
    '''
    get a subset of HUCs by HUC2 list and a polygon
    '''    
        
    # subset hucX geometry by huc2
    huc_level_str = 'huc' + str(huc_level).zfill(2)
    huc2_strings = ['-'.join([huc_level_str, a]) for a in huc2_list]
    huc_gdf_subset1 = huc_gdf[huc_gdf['id'].str.contains(
        '|'.join(huc2_strings)
    )].copy()
    
    # further subset geometry by latlon polygon (where polygon 
    # centroid falls within box) get centroids of remaining HUCs
    huc_gdf_subset1['centroid'] = huc_gdf_subset1[
        'geometry'
    ].to_crs(3857).centroid.to_crs(4326)

    huc_gdf_subset_prior = huc_gdf_subset1
    
    for poly in poly_list:
        if poly:
            huc_gdf_subset_curr = huc_gdf_subset_prior[
                poly.contains(huc_gdf_subset_prior['centroid'])
            ].copy()
        else:
            huc_gdf_subset_curr = huc_gdf_subset_prior.copy()
            
        huc_gdf_subset_curr['centroid'] = \
            huc_gdf_subset_curr['geometry'].to_crs(3857).centroid.to_crs(4326)
        huc_gdf_subset_prior = huc_gdf_subset_curr.copy()
    
    return huc_gdf_subset_curr[['id','name','geometry']]

