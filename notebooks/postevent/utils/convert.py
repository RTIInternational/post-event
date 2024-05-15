'''
unit conversion related utilities 
(temporary - unit handling will eventually be added to teehr)
'''

import pandas as pd
import geopandas as gpd

from typing import List, Union


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


def convert_rate_to_depth(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    # assume hourly timesteps for now
    if units in ['mm s^-1','mm/s','in s^-1', 'in/s']:
        converted_values = values * 3600
    else: #assume already depth
        converted_values = values
        
    return converted_values

def get_depth_units(units: str) -> str:
        
    if units in ['mm s^-1','mm/s']:
        new_units = 'mm'
    elif units in ['in s^-1', 'in/s']:
        new_units = 'in'
    else:  #already depth
        new_units = units
  
    return new_units

        
def convert_query_units(
    gdf_orig: Union[pd.DataFrame, gpd.GeoDataFrame], 
    to_units: 'str',
    variable: Union['str', None] = None,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    
    # need a metric library to look up units and ranges
    
    convert_columns = [
        "value",
        "bias",
        "secondary_value",
        "primary_value",
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
        "min",
        "max",
        "average",
        "sum"]
    
    gdf = gdf_orig.copy()
    measurement_unit = gdf['measurement_unit'][0]
    
    if variable in ['streamflow','flow','discharge']:
        if to_units == 'english':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_flow_to_cfs(
                        measurement_unit, 
                        gdf[col]
                    )
            gdf['measurement_unit'] = 'ft3/s'
        elif to_units == 'metric':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_flow_to_cms(
                        measurement_unit, 
                        gdf[col]
                    )  
            gdf['measurement_unit'] = 'm3/s'
            
    elif variable in ['precip','precipitation','precipitation_rate','RAINRATE']:
        
        if to_units == 'english':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_rate_to_depth(
                        measurement_unit, 
                        gdf[col]
                    )
                    depth_measurement_unit = get_depth_units(
                        measurement_unit
                    )
                    gdf[col] = convert_depth_to_in(
                        depth_measurement_unit, 
                        gdf[col])
                    
            gdf['measurement_unit'] = 'in/hr'
        elif to_units == 'metric':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_rate_to_depth(
                        measurement_unit, 
                        gdf[col]
                    )
                    depth_measurement_unit = get_depth_units(
                        measurement_unit
                    )
                    gdf[col] = convert_depth_to_mm(
                        depth_measurement_unit, 
                        gdf[col]
                    )                   
            gdf['measurement_unit'] = 'mm/hr'        

    return gdf
    
def convert_attr_units(
    df: pd.DataFrame, 
    to_units: 'str',
) -> pd.DataFrame:
    
    attr_units = df['attribute_unit'][0]
    attr_name = df['attribute_name'][0]
    
    if to_units == 'english':
        if attr_name.find('area')>=0:
            df['attribute_value'] = convert_area_to_mi2(
                attr_units, 
                df['attribute_value']
            )
            df['attribute_unit'] = 'mi2'
        elif attr_name.find('flow')>=0:
            df['attribute_value'] = convert_flow_to_cfs(
                attr_units, 
                df['attribute_value']
            )
            df['attribute_unit'] = 'cfs'   
            
    elif to_units == 'metric':
        if attr_name.find('area')>=0:
            df['attribute_value'] = convert_area_to_km2(
                attr_units, 
                df['attribute_value']
            )
            df['attribute_unit'] = 'km2'
        elif attr_name.find('flow')>=0:
            df['attribute_value'] = convert_flow_to_cms(
                attr_units, 
                df['attribute_value']
            )
            df['attribute_unit'] = 'cms'   
        
    return df