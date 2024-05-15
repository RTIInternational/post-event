'''
nwm-related utilities
'''
import datetime as dt
import pandas as pd

from typing import List

def get_nwm_version(
    start_date: dt.datetime,
    end_date: dt.datetime,
) -> float:
    '''
    Get NWM version based on event dates
    '''    
    version_start = nwm_version(
        dt.datetime.combine(start_date, dt.time(hour=0))
    )
    version_end = nwm_version(
        dt.datetime.combine(end_date, dt.time(hour=23))
    )
    
    if version_start != version_end:
        raise ValueError("Dates span two different NWM versions - "\
                         "currently not supported")
        
    return version_start

def nwm_version(
    date: dt.datetime,
) -> float:
    '''
    Get NWM version (2.0 or 2.1) corresponding to the reference time     
        - v2.0 assumed for all datetimes prior to 4-20-2021 13z
        - v2.1 begins 4-20-2021 at 14z 
        - v2.2 begins 7-09-2022 at 00z 
        - v3.0 begins 9-19-2023 at 12z
        - *code update would be needed for versions prior to 2.0
    '''    
    
    v21_date = dt.datetime(2021, 4, 20, 14, 0, 0)
    v22_date = dt.datetime(2022, 7, 9, 0, 0, 0)
    v30_date = dt.datetime(2023, 9, 19, 12, 0, 0)

    if date >= v30_date:
        version = 'nwm30'
    elif date >= v22_date:
        version = 'nwm22'
    elif date >= v21_date:
        version = 'nwm21'
    else:
        version = 'nwm20'
        
    return version
    

def get_value_times_for_ref_time_range(
    forecast_config: str, 
    ref_time_start: pd.Timestamp, 
    ref_time_end: pd.Timestamp,
) -> List[pd.Timestamp]:
    '''
    Get start and end values times that correspond to 
    all time steps in a given set of forecast reference times
    '''        
    now = dt.datetime.utcnow().replace(
        second=0, 
        microsecond=0, 
        minute=0, 
        hour=0
    )
    
    if forecast_config in ['medium_range','medium_range_mem1']:
        value_time_start = ref_time_start
        value_time_end = ref_time_end \
            + dt.timedelta(days=10)
    elif forecast_config in ['short_range']:
        value_time_start = ref_time_start
        value_time_end = ref_time_end \
            + dt.timedelta(hours=18)     

    return [value_time_start, value_time_end] 

