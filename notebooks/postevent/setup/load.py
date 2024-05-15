'''
building and executing TEEHR data loading functions
'''

import os
import time
import datetime as dt
import pandas as pd

from dask.distributed import Client
from pathlib import Path
from typing import List, Union

import teehr.loading.nwm.nwm_points as tlp
import teehr.loading.nwm.nwm_grids as tlg
import teehr.loading.usgs.usgs as tlu

from .. import config
from . import class_data

def get_client():
    '''
    Start dask distributed cluster with appropriate # workers
    based on size of EC2 instance on TEEHR Hub, or all available 
    processers if running locally
    '''
    if 'jovyan' in list(Path().absolute().parts): 
        n_workers = os.cpu_count()
        client = Client(n_workers=n_workers)
    else:
        client = Client()
        
    return client     

def launch_teehr_streamflow_loading(
    paths: config.Paths,
    event: config.Event,
    data_selector: class_data.DataSelector_NWMOperational,
):
    '''
    Launch TEEHR loading functions for streamflow data sources 
    based on data selections
    '''

    if data_selector.overwrite_flag:
        print('Heads Up - you are overwriting existing output.  '\
              'Kill the process if you did not intend to overwrite')
        overwrite_output = True
    else:
        overwrite_output = False
    
    # only load streamflow data if it is a selected variable
    if 'streamflow' in data_selector.variable:
    
        # set a subdirectory name (under forecast/obs timeseries dir) 
        # to keep the smaller dataset (gages only) separate from the larger 
        # so that exploring the smaller dataset is fast
        if data_selector.reach_set == 'all reaches':
            parquet_subdir = 'all'
        else:
            parquet_subdir = 'gages'
            
        # load selected forecast data if any
        if data_selector.forecast_config != 'none':
            t_start = time.time()
            
            # get sub-directories and # days of forecasts to load
            n_days = (data_selector.dates.ref_time_end \
                      - data_selector.dates.ref_time_start).days + 1
            ts_dir = Path(
                paths.parquet_dir, 
                data_selector.forecast_config, 
                parquet_subdir
            )    
            zarr_dir = Path(
                paths.zarr_dir, 
                data_selector.forecast_config
            )
            
            output_label = 'channel_rt'
            if data_selector.forecast_config == 'medium_range_mem1':
                output_label = 'channel_rt_1'
            
            print(f"Loading {data_selector.forecast_config} "\
                  f"streamflow for {len(event.nwm_id_list)} "\
                  f"NWM reaches from {data_selector.dates.ref_time_start} "\
                  f"to {data_selector.dates.ref_time_end}")
                  
            tlp.nwm_to_parquet(
                data_selector.forecast_config,
                output_label,
                'streamflow',
                data_selector.dates.ref_time_start,
                n_days,
                event.nwm_id_list,
                zarr_dir,
                ts_dir,
                event.nwm_version,
                ignore_missing_file = True,
                overwrite_output = data_selector.overwrite_flag
            )
            print(f"...{data_selector.forecast_config} "\
                  f"streamflow loading complete in "\
                  f"{round((time.time() - t_start)/60,5)} minutes\n")   
    
        # load USGS data if selected
        if 'USGS*' in data_selector.verify_config:
            t_start = time.time()
            
            # get sub-directory
            ts_dir = Path(paths.parquet_dir, 'usgs')
            print(f"Loading USGS streamflow for {len(event.usgs_id_list)} "\
                  f"gages from {data_selector.dates.data_value_time_start} "\
                  f"to {data_selector.dates.data_value_time_end}") 
            tlu.usgs_to_parquet(
                event.usgs_id_list,
                data_selector.dates.data_value_time_start,
                data_selector.dates.data_value_time_end,
                ts_dir,
                chunk_by = 'day',
                overwrite_output = overwrite_output
            )
            print(f"...USGS loading complete in "\
                  f"{round((time.time() - t_start)/60,5)} minutes\n") 
 
        # load NWM analysis data if selected
        # currently only CONUS!!!
    
        ana_list = [
            'analysis_assim_extend', 
            'analysis_assim', 
            'analysis_assim_extend_no_da*', 
            'analysis_assim_no_da*'
        ]
        n_days = (data_selector.dates.data_value_time_end \
                  - data_selector.dates.data_value_time_start).days + 1
        for ana in ana_list:
            if ana in data_selector.verify_config:
                if ana[-1] == '*':
                    ana = ana[:-1]
                
                t_start = time.time()
                ts_dir = Path(paths.parquet_dir, ana, parquet_subdir)
                zarr_dir = Path(paths.zarr_dir, ana)
                if 'extend' in ana:
                    tm_range = [t for t in range(0,28)]
                else:
                    tm_range = [t for t in range(0,2)]  

                print(f"Loading {ana} streamflow for {len(event.nwm_id_list)} "\
                      f"NWM reaches from {data_selector.dates.data_value_time_start} "\
                      f"to {data_selector.dates.data_value_time_end}")
                tlp.nwm_to_parquet(
                    ana,
                    'channel_rt',
                    'streamflow',
                    data_selector.dates.data_value_time_start,
                    n_days,        
                    event.nwm_id_list,
                    zarr_dir,
                    ts_dir,
                    event.nwm_version,
                    t_minus_hours = tm_range,
                    ignore_missing_file = True,
                    overwrite_output = data_selector.overwrite_flag
                )
                print(f"...{ana} streamflow loading complete in "\
                      f"{round((time.time() - t_start)/60,5)} minutes\n")
                
def launch_teehr_precipitation_loading(
    paths: config.Paths,
    event: config.Event,
    geo: config.Geo,
    data_selector: class_data.DataSelector_NWMOperational,
):
    '''
    Launch TEEHR loading functions for precipitation data sources 
    based on data selections
    '''
    # only load precipitation data if it is a selected variable
    if 'mean areal precipitation' in data_selector.variable:
        for map_polygons in data_selector.map_polygons:
            
            # set a subdirectory name (under forecast/obs timeseries dir) 
            # to keep the MAPs for different polygon sets separate 
            # (HUC10s or USGS basins for now)
            if map_polygons == 'HUC10':
                parquet_subdir = 'huc10'
                n_polys = len(event.huc10_list)
                polygon_set = 'HUC10s'
            elif map_polygons == 'usgs_basins':
                parquet_subdir = 'usgs_basins'
                n_polys = len(event.usgs_id_list)
                polygon_set = 'USGS basins'
                
            # valid observed configurations for precipitation
            ana_list = ['analysis_assim_extend', 'analysis_assim']
            
            # write subset of weights to temporary file (necessary to 
            # avoid memory issues when passing in memory for distributed 
            # computing)
            write_grid_weights_subset(
                paths.config_file_contents, 
                paths.grid_wts_dir, 
                map_polygons,
                event.huc10_list, 
                event.usgs_id_list,                
            )       
            
            # load selected forecast data if any
            if data_selector.forecast_config != 'none':
                t_start = time.time()
                
                # add the prefix for forcing config
                forcing_forecast_configuration = 'forcing_' \
                                                  + data_selector.forecast_config
                if forcing_forecast_configuration == 'forcing_medium_range_mem1':
                    forcing_forecast_configuration = 'forcing_medium_range'                
                
                # get sub-directories and # days of forecasts to load
                n_days = (data_selector.dates.ref_time_end \
                          - data_selector.dates.ref_time_start).days + 1
                ts_dir = Path(
                    paths.parquet_dir, 
                    forcing_forecast_configuration, 
                    parquet_subdir
                )    
                zarr_dir = Path(
                    paths.zarr_dir, 
                    forcing_forecast_configuration
                )
    
                print(f"Loading {forcing_forecast_configuration} "\
                      f"mean areal precipitation for {n_polys} {polygon_set} "\
                      f"from {data_selector.dates.ref_time_start} to "\
                      f"{data_selector.dates.ref_time_end}")
                tlg.nwm_grids_to_parquet(
                    forcing_forecast_configuration,
                    'forcing',
                    'RAINRATE',
                    data_selector.dates.ref_time_start,
                    n_days,
                    str(paths.geo_dir) + '/temp_grid_weights_subset.parquet',
                    #Path(paths.geo_dir, 'temp_grid_weights_subset.parquet'),
                    zarr_dir,
                    ts_dir,
                    event.nwm_version,
                    ignore_missing_file = True,
                    kerchunk_method = "local",
                    overwrite_output = data_selector.overwrite_flag
                )
                print(f"...{forcing_forecast_configuration} "\
                      f"mean areal precipitation loading complete in "\
                      f"{round((time.time() - t_start)/60,5)} minutes\n")
            else:
                if not any(s in data_selector.verify_config for s in ana_list):
                    print('No data loaded - no valid datasets selected')

            n_days = (data_selector.dates.data_value_time_end \
                       - data_selector.dates.data_value_time_start).days + 1
            
            for ana in ana_list:          
                if ana in data_selector.verify_config:
                    t_start = time.time()
                    
                    forcing_ana_config = 'forcing_' + ana
                    ts_dir = Path(
                        paths.parquet_dir, 
                        forcing_ana_config, 
                        parquet_subdir
                    )
                    zarr_dir = Path(paths.zarr_dir, forcing_ana_config)
                    if 'extend' in ana:
                        tm_range = [t for t in range(4,28)]
                    else:
                        tm_range = [2]   
    
                    print(f"Loading {ana} mean areal precipitation for "\
                          f"{n_polys} {polygon_set} from "\
                          f"{data_selector.dates.data_value_time_start} "\
                          f"to {data_selector.dates.data_value_time_end}")
                    
                    tlg.nwm_grids_to_parquet(
                        forcing_ana_config,
                        'forcing',
                        'RAINRATE',
                        data_selector.dates.data_value_time_start,
                        n_days,        
                        str(paths.geo_dir) + '/temp_grid_weights_subset.parquet',
                        #Path(paths.geo_dir, 'temp_grid_weights_subset.parquet'),
                        zarr_dir,
                        ts_dir,
                        event.nwm_version,
                        t_minus_hours = tm_range,
                        ignore_missing_file = True,
                        overwrite_output = data_selector.overwrite_flag
                    )
                    print(f"...{forcing_ana_config} mean areal precipitation "\
                          f"loading complete in {round((time.time() - t_start)/60,5)} "\
                          f"minutes\n")

            remove_grid_weights_subset()
            

def write_grid_weights_subset(
    user_config: dict,
    grid_wts_dir: str,
    polygon_set: str,
    huc10_list: List[str],
    usgs_id_list: List[str],
    ):
    '''
    Create a subset of grid weights to speed up read and processing 
    during MAP calculations. TEEHR preciptation loading and 
    MAP calculations read a weights file from disk to prevent
    memory issues that would occur is passing in memory for 
    distributed computing). 
    '''        
    if any(s in polygon_set for s in ['huc10','HUC10']):
        grid_weights = pd.read_parquet(
            Path(
                grid_wts_dir, 
                user_config["GRID_WEIGHTS_FILES_CONUS"]["HUC10_NWM"]
            )
        )
        id_list_with_prefix = huc10_list
        
    elif any(s in polygon_set for s in ['usgs','USGS','usgs_basins']):
        grid_weights = pd.read_parquet(
            Path(
                grid_wts_dir, 
                user_config["GRID_WEIGHTS_FILES_CONUS"]["USGS_NWM"]
            )
        )  
        id_list_with_prefix = ['-'.join(['usgs', s]) for s in usgs_id_list]

    grid_weights_subset = grid_weights[
        grid_weights['location_id'].isin(id_list_with_prefix)
    ]

    grid_weights_subset.to_parquet(
        Path(
            grid_wts_dir, 
            'temp_grid_weights_subset.parquet'
        )
    )

def remove_grid_weights_subset(grid_wts_dir: str):

    file = Path(
        grid_wts_dir, 
        'grid_weights', 
        'temp_grid_weights_subset.parquet'
    )
    file.unlink()
    

