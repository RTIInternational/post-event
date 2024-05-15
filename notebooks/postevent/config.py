'''
Classes to hold event and data specifications
for post-event dashbaords
'''
import json
import pandas as pd
import geopandas as gpd
import datetime as dt
import panel as pn

from typing import List, Union
from pathlib import Path
from shapely import Polygon

from . import utils

# date constants as type datetime.date
NOW = dt.datetime.now()
TODAY = NOW.date()
YESTERDAY = TODAY - dt.timedelta(days=1)
DATE_BOUNDS = (dt.date(2018, 9, 17), TODAY)

class Paths:
    '''
    Main class to collect/organize/store filepaths and 
    associated selectors
    '''
    
    def __init__(
        self, 
        config_file: str
    ):
        '''
        read post_event_config file and setup
        event selectors
        '''
        if config_file is not None:
            self.config_file_contents = read_json_definitions(
                config_file
            )
            self.set_config_paths()
            self.get_selectors()
        else:
            self.config_file_contents = {}            
    
    def set_config_paths(self):
        '''
        set parent path variables (independent of event name)
        '''        
        user_config = self.config_file_contents    

        self.events_dir = Path(
            user_config["EVENTS_DIR"]
        )
        self.geo_dir = Path(
            user_config["GEO_DIR"]
        )
        self.cross_dir = Path(
            user_config["CROSSWALK_DIR"]
        )
        self.attribute_dir = Path(
            user_config["ATTRIBUTE_DIR"]
        )
        self.grid_wts_dir = Path(
            user_config["WEIGHTS_DIR"]
        )
        self.zarr_dir = Path(
            user_config["ZARR_DIR"]
        )
        self.event_defs_file = Path(
            user_config["EVENT_DEFINITIONS_FILE"]
        )  

        try:
            self.existing_events = read_json_definitions(
                self.event_defs_file
            )
        except:
            self.existing_events = {}    

    def set_data_paths(
        self, 
        event_name: str = None, 
        name_selector_with_new = True
    ):
        '''
        Set event name and main output paths
        '''        
        if event_name is not None:
            self.event_name = event_name
            
        elif self.event_name_selector_without_new is not None and \
            not name_selector_with_new:
            self.event_name = self.event_name_selector_without_new.value

        elif self.event_name_selector_with_new is not None and \
            name_selector_with_new:
            self.event_name = self.event_name_selector_with_new.value
            
        self.parquet_dir = Path(
            self.events_dir, 
            self.event_name, 
            'parquet'
        )
        self.viz_dir = Path(
            self.events_dir,
            self.event_name, 
            'viz'
        )    
                      
    def set_eval_paths(
        self, 
        forecast_config:str = None
    ):
        '''
        Set the paths for TEEHR queries and check that the necessary
        data has been loaded
        '''
        self.set_data_paths(name_selector_with_new=False)
        
        if self.forecast_config_selector is not None:
            self.forecast_config = self.forecast_config_selector.value
        elif forecast_config is not None:
            self.forecast_config=forecast_config
        
        # filepaths consistent for all post-event studies
        self.set_forcing_paths()
        self.set_forcing_paths(
            polygons='usgs_basins',
            alt=True
        )
        self.set_streamflow_paths()

        # check that necessary directories and 
        # files exist (not checking specific 
        # files here, just that any files exist 
        # in the dir as a first check)
        self.check_paths()
            
    def check_paths(self):
        '''
        Check that paths and data exist for the evaluation
        '''            
        for path in self.streamflow_filepaths:
            check_dir_exists = self.streamflow_filepaths[path].parent.exists()
            check_not_empty = False
            if check_dir_exists:
                check_not_empty = any(
                    self.streamflow_filepaths[path].parent.iterdir()
                )
            path_good = check_dir_exists and check_not_empty
            if not path_good:
                raise FileNotFoundError(                                  
                    f" Data needed for this evaluation are missing. \
                    Directory {self.streamflow_filepaths[path].parent} \
                    does not exist or is empty"
                )
    
        for path in self.forcing_filepaths:
            check_dir_exists = self.forcing_filepaths[path].parent.exists()
            check_not_empty = False
            if check_dir_exists:
                check_not_empty = any(
                    self.forcing_filepaths[path].parent.iterdir()
                )
            path_good = check_dir_exists and check_not_empty
            if not path_good:
                raise FileNotFoundError(
                    f" Data needed for this evaluation are missing.    \
                    Directory {self.forcing_filepaths[path].parent}    \
                    does not exist or is empty"
                )

    def set_forcing_paths(
        self, 
        polygons='huc10', 
        domain='conus',
        alt = False
    ):
        '''
        Define an alternate set of forcing paths - for cases where
        dashboards are showing summaries by one set of polygons in maps (e.g., HUC10)
        and another set of polygons for time series plots (e.g., USGS basins)
        '''
        # 'observed' source filepaths (same in all cases)
        primary_filepath=Path(
            self.parquet_dir, 
            "forcing_analysis_assim_extend", 
            polygons, 
            "*.parquet"
        )
        # crosswalk is identical IDs (polygon IDs) for MAP time series
        crosswalk_filepath=Path(
            self.cross_dir, 
            polygons + "_" + polygons + "_crosswalk." + domain + ".parquet"
        )

        # read file names
        user_config = self.config_file_contents

        # filepaths dependent on the polygon set
        if polygons == 'usgs_basins':
            geometry_filepath=Path(
                self.geo_dir, 
                user_config["GEO_FILES_CONUS"]["USGS_BASINS"]
            )    
        elif polygons in ['huc10','huc2']:
            geometry_filepath=Path(
                self.geo_dir, 
                user_config["GEO_FILES_CONUS"][polygons.upper()]
            )

        # filepaths dependent on the forecast config and polygon set
        if self.forecast_config == 'short_range':
            secondary_filepath = Path(
                self.parquet_dir, 
                "forcing_short_range", 
                polygons, 
                "*.parquet"
            )
        elif self.forecast_config == 'medium_range_mem1':
            secondary_filepath = Path(
                self.parquet_dir, 
                "forcing_medium_range", 
                polygons, 
                "*.parquet"
            )
        
        if alt:
            self.alt_forcing_filepaths = dict(  
                primary_filepath=primary_filepath,
                secondary_filepath=secondary_filepath,
                crosswalk_filepath=crosswalk_filepath,
                geometry_filepath=geometry_filepath,
            )
        else:
            self.forcing_filepaths = dict(  
                primary_filepath=primary_filepath,
                secondary_filepath=secondary_filepath,
                crosswalk_filepath=crosswalk_filepath,
                geometry_filepath=geometry_filepath,
            )            
            
    def set_streamflow_paths(
        self, 
        nwm_version='nwm22', 
        domain='conus', 
        reach_set='gages'
    ):
        '''
        Define main set of streamflow paths for TEEHR queries  
        '''

        user_config = self.config_file_contents
        
        self.streamflow_filepaths = dict(  
            primary_filepath=Path(
                self.parquet_dir, 
                "usgs", 
                "*.parquet"
            ),
            secondary_filepath=Path(
                self.parquet_dir, 
                self.forecast_config, 
                reach_set, 
                "*.parquet"
            ),
            crosswalk_filepath=Path(
                self.cross_dir, 
                user_config["CROSSWALK_FILES_CONUS"]["USGS_" + nwm_version.upper()]
            ),
            geometry_filepath=Path(
                self.geo_dir, 
                user_config["GEO_FILES_CONUS"]["USGS_POINTS"]
            ),
            noda_filepath=Path(
                self.parquet_dir, 
                "analysis_assim_extend_no_da", 
                reach_set, 
                "*.parquet"
            )        
        ) 

    def update_streamflow_path_nwm_version(
        self, 
        nwm_version
    ):
        '''
        Update the crosswalk filepath based on selected NWM version
        '''
        path = self.streamflow_filepaths['crosswalk_filepath']
        parent = path.parents[0]
        fileparts = path.name.split('_')
        fileparts[1] = nwm_version
        self.streamflow_filepaths['crosswalk_filepath'] = Path(
            parent, 
            ("_").join(fileparts)
        )

    def update_streamflow_paths(self, event, reach_set='gages'):

        
        self.set_streamflow_paths(
            nwm_version = event.nwm_version, 
            domain=event.domain, 
            reach_set=reach_set
        )

    def get_selectors(self):

        # for loading workflow - name selector with option 
        # to define new event
        self.event_name_selector_with_new = pn.widgets.Select(
            name='Select new or previously defined event:', 
            options=['define new event'] + list(self.existing_events.keys())
        )  

        # for evaluation workflow - name selector with defined events only
        self.event_name_selector_without_new = pn.widgets.Select(
            name='Select a defined event:', 
            options=list(self.existing_events.keys())
        ) 

        # other evaluation options (currently conus is only 
        # supported domain - others coming)
        self.forecast_config_selector = pn.widgets.Select(
            name='Forecast configuration:', 
            options=['short_range','medium_range_mem1']
        )
        self.unit_selector = pn.widgets.Select(
            name='Plot units:', 
            options=['english','metric']
        )
        self.domain_selector = pn.widgets.Select(
            name='Geographic domain (currently conus only):', 
            options=['conus'] #'alaska','hawaii','puertorico']
        )    

        # widget selector layout for eval notebooks
        self.eval_selectors = pn.Row(
            self.event_name_selector_without_new, 
            self.forecast_config_selector, 
            self.domain_selector, 
            self.unit_selector
        )           

        # widget selector layout for eval notebooks
        self.loading_selectors = pn.Row(
            self.event_name_selector_with_new, 
            self.domain_selector
        )   
    
    def update_loading_options(self):
        self.set_data_paths()
        self.domain = self.domain_selector.value

    def update_eval_options(self):
        self.set_eval_paths()
        self.forecast_config = self.forecast_config_selector.value
        self.units = self.unit_selector.value
        self.domain = self.domain_selector.value


class Event:
    '''
    Event specs read from stored event definitions or 
    defined (or redefined) via the EventSelector dashboard
    '''
    def __init__(self, paths):      
        
        # initialize as empty - needed for ClassSelector param definition
        if paths is None:
            self.dir_name = None
            self.domain = None
            self.event_start_date = None
            self.event_end_date = None
            self.region_polygon = None
            self.huc2_list = None
            self.nwm_version = None
        else:
            if paths.event_name in paths.existing_events.keys():        
                event_specs_from_file = paths.existing_events[
                    paths.event_name
                ].copy()
                self.dir_name = paths.event_name
                self.domain = paths.domain            
                self.event_start_date = dt.datetime.strptime(
                    event_specs_from_file['event_start_date'], 
                    "%Y%m%d"
                ).date()
                self.event_end_date = dt.datetime.strptime(
                    event_specs_from_file['event_end_date'], 
                    "%Y%m%d"
                ).date()
                self.region_polygon = utils.geom.get_polygon_from_coords(
                    event_specs_from_file['region_boundary_coords']
                )
                self.huc2_list = event_specs_from_file['huc2_list']
                self.nwm_version = utils.nwm.get_nwm_version(
                    self.event_start_date, 
                    self.event_end_date
                )    

                # update some filepaths based on event name 
                # and dates (version)
                paths.set_data_paths(self.dir_name)
                
                if hasattr(paths, "streamflow_filepaths"):
                    paths.update_streamflow_path_nwm_version(
                        self.nwm_version
                    )
                    
            elif paths.event_name == 'define new event':
                self.get_default_event()        
            
            else:
                raise ValueError("Event not found")
        
            
    def get_default_event(self):
        '''
        default starting specs for a new event
        '''
        self.dir_name = 'YYYYMM_name'
        self.event_start_date = YESTERDAY
        self.event_end_date = TODAY
        self.region_polygon = utils.geom.get_polygon_from_coords([]) 
        self.huc2_list = []
        self.nwm_version = utils.nwm.get_nwm_version(
            self.event_start_date, 
            self.event_end_date
        )
        self.domain = 'conus'
        
    def get_location_lists(self, paths, geo):
        
        # get the list of usgs ids in the region intersection 
        # (huc2s and lat/lon polygon)
        self.usgs_id_list = utils.locations.get_usgs_id_list_as_str(
            self.huc10_list, 
            geo.usgs_points, 
            geo.cross_usgs_huc, 
            geo.cross_usgs_nwm
        )
        self.usgs_id_list_with_prefix = [
            'usgs-' + s for s in self.usgs_id_list
        ]
        
        # get the initial list of nwm ids corresponding to
        # usgs locations (huc10s)
        self.nwm_id_list = utils.locations.get_nwm_id_list_as_int(
            geo.cross_usgs_nwm, 
            geo.cross_nwm_huc, 
            self.nwm_version, 
            self.usgs_id_list, 
            self.huc10_list
        )   
        
class Geo:
    '''
    Geo directory and geometry data needed for 
    event selection and data loading
    '''
    def __init__(self, paths, event):
        
        # initialize as empty geodataframes - needed for 
        # ClassSelector param definition
        if paths is None:
            self.states = gpd.GeoDataFrame()
            self.huc2 = gpd.GeoDataFrame()
            self.huc10 = gpd.GeoDataFrame()
            self.usgs_points = gpd.GeoDataFrame()
            self.cross_usgs_huc = pd.DataFrame()
            self.cross_usgs_nwm = pd.DataFrame()
            self.huc2_subset = gpd.GeoDataFrame()
            self.huc10_subset = gpd.GeoDataFrame()
            self.region_polygon = Polygon()
            self.plot_box = Polygon()
        else:
            user_config = paths.config_file_contents
            self.states = gpd.read_parquet(
                Path(
                    paths.geo_dir, 
                    user_config["GEO_FILES_CONUS"]["STATES"]
                )
            )
            self.huc2 = gpd.read_parquet(
                Path(
                    paths.geo_dir, 
                    user_config["GEO_FILES_CONUS"]["HUC2"]
                )
            )
            self.huc10 = gpd.read_parquet(
                Path(
                    paths.geo_dir, 
                    user_config["GEO_FILES_CONUS"]["HUC10"]
                )
            )
            self.usgs_points = gpd.read_parquet(
                Path(
                    paths.geo_dir, 
                    user_config["GEO_FILES_CONUS"]["USGS_POINTS"]
                )
            )
            self.usgs_basins = gpd.read_parquet(
                Path(
                    paths.geo_dir, 
                    user_config["GEO_FILES_CONUS"]["USGS_BASINS"]
                )
            )
            self.cross_usgs_huc = pd.read_parquet(
                Path(
                    paths.cross_dir, 
                    user_config["CROSSWALK_FILES_CONUS"]["USGS_HUC12"]
                )
            )
            self.region_polygon = event.region_polygon
            self.domain_limits = utils.geom.get_domain_limits(paths.domain)
            self.get_huc_subsets(event)
            self.get_map_limits()            
            self.read_usgs_nwm_crosswalk_version(
                paths, 
                event.nwm_version
            )
            self.read_nwm_huc_crosswalk_version(
                paths, 
                event.nwm_version
            )

    def read_usgs_nwm_crosswalk_version(
        self, paths, 
        nwm_version
    ):
        user_config = paths.config_file_contents
        config_file_key = dict(
            nwm20 = "USGS_NWM20",
            nwm21 = "USGS_NWM21",
            nwm22 = "USGS_NWM22",
            nwm30 = "USGS_NWM30",
            nwm31 = "USGS_NWM31",
        )
        self.cross_usgs_nwm = pd.read_parquet(
            Path(
                paths.cross_dir, 
                user_config["CROSSWALK_FILES_CONUS"][
                    config_file_key[nwm_version]
                ]
            )
        ) 
    
    def read_nwm_huc_crosswalk_version(
        self, 
        paths, 
        nwm_version
    ):
        user_config = paths.config_file_contents
        config_file_key = dict(
            nwm20 = "NWM20_HUC12",
            nwm21 = "NWM21_HUC12",
            nwm22 = "NWM22_HUC12",
            nwm30 = "NWM30_HUC12",
            nwm31 = "NWM31_HUC12",
        )
        self.cross_nwm_huc = pd.read_parquet(
            Path(
                paths.cross_dir, 
                user_config["CROSSWALK_FILES_CONUS"][
                    config_file_key[nwm_version]
                ]
            )
        )    
                
    def get_usgs_basins(self, paths): 
        user_config = self.config_file_contents
        self.usgs_basins = gpd.read_parquet(
            Path(
                paths.geo_dir, 
                user_config["GEO_FILES_CONUS"]["USGS_BASINS"]
            )
        )#.to_crs('EPSG:4326')
        
    def get_usgs_attributes(self, paths):
        user_config = paths.config_file_contents
        self.attribute_list = [
            'drainage_area', 
            'hw_threshold', 
            'ecoregion', 
            'stream_order'
        ]

        usgs_drainage_area = pd.read_parquet(
            Path(
                paths.attribute_dir, 
                user_config["USGS_ATTRIBUTES_CONUS"]["DRAINAGE_AREA"]
            )
        )
        usgs_hw_threshold = pd.read_parquet(
            Path(
                paths.attribute_dir, 
                user_config["USGS_ATTRIBUTES_CONUS"]["HW_THRESHOLD"]
            )
        )    
        self.usgs_drainage_area = utils.convert.convert_attr_units(
            usgs_drainage_area, 
            paths.unit_selector.value
        )
        self.usgs_hw_threshold = utils.convert.convert_attr_units(
            usgs_hw_threshold, 
            paths.unit_selector.value
        )        
        self.usgs_ecoregions = pd.read_parquet(
            Path(paths.attribute_dir, 
                 user_config["USGS_ATTRIBUTES_CONUS"]["ECOREGIONS"]
                )
        )
        self.usgs_stream_order = pd.read_parquet(
            Path(
                paths.attribute_dir, 
                user_config["USGS_ATTRIBUTES_CONUS"]["STREAM_ORDER"]
            )
        )
        self.attribute_df_list = [
            self.usgs_drainage_area, 
            self.usgs_hw_threshold, 
            self.usgs_ecoregions, 
            self.usgs_stream_order
        ]        
    
    # TEMPORARY PENDING TEEHR UPDATES
    def merge_attributes_to_points(self):

        left_gdf = self.usgs_points_subset
        col_list = ['id','name','geometry']
        for i, attribute in enumerate(self.attribute_list):
            right_gdf = self.attribute_df_list[i]
            merged_gdf = left_gdf.merge(
                right_gdf, 
                how='left', 
                left_on='id', 
                right_on='location_id'
            )
            # fill NAN thresholds with zero
            if attribute in ['hw_threshold']:
                merged_gdf['attribute_value'] \
                    = merged_gdf['attribute_value'].fillna(0)

            # fill 0 thresholds with 0.01 to avoid divide by zero
            if attribute in ['drainage_area']:
                merged_gdf['attribute_value'] \
                    = merged_gdf['attribute_value'].fillna(0.01)
                merged_gdf['attribute_value'] \
                    = merged_gdf['attribute_value'].replace(0, 0.01)

            
            merged_gdf = merged_gdf[col_list + ['attribute_value']]
            merged_gdf = merged_gdf.rename(
                columns={'attribute_value': attribute}
            )
            col_list = col_list + [attribute]
            left_gdf = merged_gdf
            
        self.usgs_points_subset = merged_gdf
        

    def update_geometry(
        self, 
        paths, 
        event, 
        dates, 
        poly_stream = None
    ):
        
        self.get_huc_subsets(event, poly_stream)
        self.get_map_limits()
        self.get_usgs_date_subset(paths, dates, check_data = True)
        
        
    def get_huc_subsets(
        self, 
        event, 
        poly_stream = None
    ):
        
        # get additional zoom-area polygon if defined
        if poly_stream is not None:
            poly = utils.geom.get_polygon_from_poly_stream(poly_stream) 
        else:
            poly = Polygon()
        polygons = [event.region_polygon, poly]
        
        # huc2 subset 
        if event.huc2_list:
            self.huc2_subset = self.huc2[self.huc2['id'].isin(event.huc2_list)]
        else:
            self.huc2_subset = gpd.GeoDataFrame() #self.huc2

        # huc 10 subset within huc2 and/or polygons
        self.huc10_subset = utils.locations.get_hucx_subset(
            self.huc10, 
            event.huc2_list, 
            polygons, 
            huc_level = 10
        )
        event.huc10_list = self.huc10_subset['id'].to_list()

    
    def get_map_limits(self):

        # get lat-lon bounds around HUC10s
        huc10_bounds = self.huc10_subset['geometry'] \
            .to_crs('4326').total_bounds
        lat_limits = (huc10_bounds[1], huc10_bounds[3])
        lon_limits = (huc10_bounds[0], huc10_bounds[2])
        xlims = (lon_limits[0]*1.004, lon_limits[1]*0.996)
        ylims = (lat_limits[0]*0.993, lat_limits[1]*1.003)

        xlims_mercator, ylims_mercator = utils.geom.project_limits_to_mercator(
            xlims, 
            ylims
        )
        self.map_limits = dict(
            xlims_lon = xlims,
            ylims_lat = ylims,
            xlims_mercator = xlims_mercator,
            ylims_mercator = ylims_mercator
        )
        self.map_limits = utils.geom.adjust_square_map_limits(self.map_limits)  
        
    def get_usgs_date_subset(
        self, 
        paths, 
        dates, 
        check_data = True
    ):
        # usgs points subset
        usgs_filepath = paths.streamflow_filepaths['primary_filepath']
        start_date = dates.data_value_time_start
        end_date = dates.data_value_time_end
        usgs_ids_with_data = utils.locations.get_ids_in_parquet_for_date_range(
            usgs_filepath, 
            start_date, 
            end_date
        )   
        if check_data:
            self.usgs_points_subset = self.usgs_points[
            self.usgs_points['id'].isin(usgs_ids_with_data)
            ]        
        else:
            self.usgs_points_subset = self.usgs_points
        self.merge_attributes_to_points()
        
        
class Dates:
    '''
    Event dates, forecast reference times, 
    and values times associated with the event
    '''
    def __init__(self, paths, event):
        
        self.initialize_dates(paths, event)
    
    def initialize_dates(self, paths, event, forecast_config=None):
        
        now = dt.datetime.utcnow().replace(
            second=0, 
            microsecond=0, 
            minute=0, 
            hour=0
        )
        
        if forecast_config is not None:
            self.forecast_config = forecast_config
        else:
            if paths is None:
                self.forecast_config = 'short_range'
            else:
                # assume forecast config is SRF then 
                # check selector value if there is one
                if paths.forecast_config_selector:
                    self.forecast_config = paths.forecast_config_selector.value
                else:
                    self.forecast_config = 'short_range'  
                    
        if self.forecast_config == 'short_range':
            self.forecast_duration = dt.timedelta(hours=18)
        elif self.forecast_config in ['medium_range','medium_range_mem1']:
            self.forecast_duration = dt.timedelta(days=10)

        # get date constants as datetimes
        today_dt = NOW.replace(
            second=0, 
            microsecond=0, 
            minute=0, 
            hour=0
        )
        yesterday_dt = today_dt - dt.timedelta(days=1)
        date_bounds_dt = (dt.datetime(2018, 9, 17, 0), today_dt)
        
        if event is None:
            self.event_value_time_start = yesterday_dt
            self.event_value_time_end = today_dt
            self.ref_time_start = yesterday_dt
            self.ref_time_end = today_dt
        else:
            # get the value times associated with the start 
            # and end of the defined event period
            self.event_value_time_start = dt.datetime.combine(
                event.event_start_date, 
                dt.time(hour=0)
            )
            self.event_value_time_end = dt.datetime.combine(
                event.event_end_date, 
                dt.time(hour=23)
            )
            self.get_ref_times(event)           
        
        self.get_data_value_times()
        
    def get_ref_times(self, event):
        '''
        Get start and end reference times that correspond to 
        a given forecast configuration and event dates
        ''' 
        if self.forecast_config in ['medium_range','medium_range_mem1']:
            self.ref_time_start = dt.datetime.combine(
                event.event_start_date, 
                dt.time(hour=0)
            ) - dt.timedelta(days=10)
            self.ref_time_end = dt.datetime.combine(
                event.event_end_date, 
                dt.time(hour=18)
            )
        elif self.forecast_config in ['short_range']:
            self.ref_time_start = dt.datetime.combine(
                event.event_start_date, 
                dt.time(hour=0)
            ) - dt.timedelta(hours=18)
            self.ref_time_end = dt.datetime.combine(
                event.event_end_date, 
                dt.time(hour=23)
            )        

    def get_data_value_times(self):
        '''
        Get start and end values times that correspond to 
        all time steps in a given set of forecasts
        '''        
        now = dt.datetime.utcnow().replace(
            second=0, 
            microsecond=0, 
            minute=0, 
            hour=0
        )
        
        if self.forecast_config in ['medium_range','medium_range_mem1']:
            self.data_value_time_start = self.ref_time_start
            self.data_value_time_end = self.ref_time_end \
                + dt.timedelta(days=10)
        elif self.forecast_config in ['short_range']:
            self.data_value_time_start = self.ref_time_start
            self.data_value_time_end = self.ref_time_end \
                + dt.timedelta(hours=18)    
        elif self.forecast_config == 'none':
            self.data_value_time_start = self.event_value_time_start
            self.data_value_time_end = self.event_value_time_end
            
        if self.data_value_time_end > now:
            self.data_value_time_end = now    

    def get_analysis_value_times(self, restrict_to_event_period):
        '''
        Get start and end values times of the analysis
        '''        

        # restrict the analysis to include only time steps overlapping the 
        # specified dates (e.g. event dates),
        # rather than full value time range of the forecasts (empty list 
        # if not defined)
        if restrict_to_event_period:
            # retrict the analysis period to the event dates, unless the 
            # values times of the forecasts are a smaller range
            if self.data_value_time_start > self.event_value_time_start:
                self.analysis_time_start = self.data_value_time_start
            else:
                self.analysis_time_start = self.event_value_time_start
                
            if self.data_value_time_end < self.event_value_time_end:
                self.analysis_time_end = self.data_value_time_end
            else:
                self.analysis_time_end = self.event_value_time_end
        
        else:
            # otherwise use the forecast value time range
            self.analysis_time_start = self.data_value_time_start
            self.analysis_time_end = self.data_value_time_end

def read_json_definitions(
    definitions_filepath: Path
) -> List[Path]:
    '''
    read and parse contents from a json file
    '''
    try:
        with open(definitions_filepath) as file:
            file_contents = file.read()
        parsed_json = json.loads(file_contents)
    except:
        parsed_json = {}
    
    return parsed_json