'''
post-event specific classes and utilities
'''
from dask.distributed import Client
import os
import datetime as dt
from pathlib import Path
import json
import time
from typing import List, Union
import panel as pn
import pandas as pd
import geopandas as gpd
import param
import holoviews as hv
import geoviews as gv
import cartopy.crs as ccrs
from shapely.geometry import Polygon
from pyproj import CRS
import teehr.loading.nwm22.nwm_point_data as tlp
import teehr.loading.usgs.usgs as tlu
import teehr.loading.nwm22.nwm_grid_data as tlg

TODAY = dt.datetime.now().date()
YESTERDAY = TODAY - dt.timedelta(days=1)
DATE_BOUNDS = (dt.date(2018, 9, 17), TODAY)

#### Classes and subclasses to organize event and data specs
#### subclasses become parameters of parameterized classes
#### in order to pass all the needed data into the dashbaords

class Config:
    '''
    Main class with subclasses to collect/organize/store 
    data specs, geometry and event specs
    '''
    def __init__(self, config_file):
        
        if config_file is not None:
        
            self.json = read_json_definitions(config_file)
            self.get_data_specs()

            # initialize widget to select an event
            self.select_event = pn.widgets.Select(name='Select new or previously defined event:', 
                                                  options=['define new event'] + list(self.data.existing_events.keys()))    
        else:
            self.json = {}
        
    def get_data_specs(self):
        self.data = DataSpecs(self)        
        
    def get_geometry(self):
        self.geo = Geo(self)     
        
    def get_event_specs(self):
        self.event = EventSpecs(self) 
        
    def update_event_specs(self, event_selector):
        '''
        update event specs after making selections in the event_selector dashboard
        '''
        selection_index = event_selector.region.stream.index
        prior_selected_huc2_list = event_selector.region.geo.selected_huc2.index.to_list()
        selected_huc2_list = get_updated_huc2_list(event_selector.region.geo.huc2, selection_index, prior_selected_huc2_list)
        
        self.event.huc2_list = selected_huc2_list
        self.event.lat_limits = event_selector.region.lat_limits
        self.event.lon_limits = event_selector.region.lon_limits
        self.event.event_start_date = event_selector.event_start_date
        self.event.event_end_date = event_selector.event_end_date
        self.event.event_name = event_selector.event_name  
        self.event.nwm_version = get_nwm_version(self.event)
        
        # set the timeseries dir based on event name
        self.data.ts_dir = Path(self.data.protocol_dir, 'events', self.event.event_name, 'timeseries')
        
        # get the list of usgs ids in the region
        self.event.usgs_id_list = get_usgs_id_list_as_str(self)
        
        # get the initial list of nwm ids corresponding to usgs locations
        self.event.nwm_id_list = get_nwm_id_list_as_int(self, self.event.usgs_id_list)
        
        # get subset of HUC10 polygons in the region
        huc_gdf = get_hucs_subset(self, huc_level = 10)
        self.event.huc10_list = huc_gdf['id'].to_list() 
        
        
class DataSpecs:
    '''
    TEEHR directories and stored event definitions info, single widget to select new or stored event
    prior to launching dashboards (eventually could refactor to include this widget in the Event Selector dashbaord 
    (and automatically update all the other widgets) but will make that dashboard code significantly more complex)
    '''
    def __init__(self, config):
        
        json = config.json
    
        if 'jovyan' in list(Path().absolute().parts):
            self.protocol_dir = Path(json["TEEHR_ROOT_TEEHRHUB"], json["PROTOCOL_NAME"])
        else:
            self.protocol_dir = Path(json["TEEHR_ROOT_LOCAL"], json["PROTOCOL_NAME"])
        self.geo_dir = Path(self.protocol_dir, json["TEEHR_GEO_DIR"])
        self.json_dir = Path(self.protocol_dir, json["TEEHR_ZARR_DIR"])
        self.studies_dir = Path(self.protocol_dir, json["TEEHR_STUDIES_DIR"])
        self.event_defs_path = Path(self.studies_dir, json["EVENT_DEFINITIONS_FILE"])    

        try:
            self.existing_events = read_json_definitions(self.event_defs_path)
        except:
            self.existing_events = {}
        
class Geo:
    '''
    Geo directory and geometry data needed for event selection and data loading
    '''
    def __init__(self, config):
        
        # initialize as empty geodataframes - needed for ClassSelector param definition
        if config is None:
            self.states = gpd.GeoDataFrame()
            self.huc2 = gpd.GeoDataFrame()
            self.selected_huc2 = gpd.GeoDataFrame()
            self.huc10 = gpd.GeoDataFrame()
            self.usgs_pts = gpd.GeoDataFrame()
            self.cross_usgs_huc = pd.DataFrame()
        else:
            self.states = gpd.read_parquet(Path(config.data.geo_dir, config.json["GEO_FILES_CONUS"]["STATES"]))
            self.huc2 = gpd.read_parquet(Path(config.data.geo_dir, config.json["GEO_FILES_CONUS"]["HUC2"]))
            self.huc10 = gpd.read_parquet(Path(config.data.geo_dir, config.json["GEO_FILES_CONUS"]["HUC10"]))
            self.usgs_points = gpd.read_parquet(Path(config.data.geo_dir, config.json["GEO_FILES_CONUS"]["USGS_POINTS"]))
            self.cross_usgs_huc = pd.read_parquet(Path(config.data.geo_dir, config.json["CROSSWALK_FILES_CONUS"]["USGS_HUC12"]))
            self.cross_usgs_nwm = pd.read_parquet(Path(config.data.geo_dir, config.json["CROSSWALK_FILES_CONUS"]["USGS_NWM22"]))
            
            try:
                self.selected_huc2=self.huc2[self.huc2.index.isin(config.event.huc2_list)]
            except:
                self.selected_huc2=gpd.GeoDataFrame()      
                
    def write_grid_weights_subset(self, config, polygon_set):
        '''
        Create a subset of grid weights to speed up read and processing during MAP calculations.
        TEEHR preciptation loading and MAP calculations read a weights file from disk to prevent
        memory issues that would occur is passing in memory for distributed computing). 
        '''
        if any(s in polygon_set for s in ['huc10','HUC10']):
            grid_weights = pd.read_parquet(Path(config.data.geo_dir, config.json["GRID_WEIGHTS_FILES_CONUS"]["HUC10_NWM"]))
            id_list_with_prefix = config.event.huc10_list
            
        elif any(s in polygon_set for s in ['usgs','USGS']):
            grid_weights = pd.read_parquet(Path(config.data.geo_dir, config.json["GRID_WEIGHTS_FILES_CONUS"]["USGS_NWM"]))  
            id_list_with_prefix = ['-'.join(['usgs', s]) for s in config.event.usgs_id_list]
    
        grid_weights_subset = grid_weights[grid_weights['location_id'].isin(id_list_with_prefix)]
        grid_weights_subset.to_parquet(Path(config.data.geo_dir, 'temp_grid_weights_subset.parquet'))  
     
                
class EventSpecs:
    '''
    Event specs read from stored event definitions or defined (or redefined) via the EventSelector dashboard
    '''
    def __init__(self, config):      
        event_name = config.select_event.value
        if event_name in config.data.existing_events.keys():        
            event_specs_from_file = config.data.existing_events[event_name].copy()
            self.event_name = event_name
            self.event_start_date = dt.datetime.strptime(event_specs_from_file['event_start_date'], "%Y%m%d").date()
            self.event_end_date = dt.datetime.strptime(event_specs_from_file['event_end_date'], "%Y%m%d").date()
            self.lat_limits = tuple(event_specs_from_file['lat_limits'])
            self.lon_limits = tuple(event_specs_from_file['lon_limits']) 
            self.huc2_list = event_specs_from_file['huc2_list']
            self.nwm_version = get_nwm_version(self)       
        else:
            self.get_default_event()        
            
    def get_default_event(self):
        '''
        default starting specs for a new event
        '''
        self.event_name = 'YYYYMM_name'
        self.event_start_date = YESTERDAY
        self.event_end_date = TODAY
        self.lat_limits = (23, 54)
        self.lon_limits = (-126, -65)
        self.huc2_list = []
        self.nwm_version = get_nwm_version(self)
        
#### Parameterized classes to enable Panel interactive/reactive dashboards
        
class RegionSelector(param.Parameterized):
    '''
    Map portion and reactive portion of the event selector dashboard
    '''
    lat_limits = param.Tuple(default=(23,54))
    lon_limits = param.Tuple(default=(-126,-65))
    huc2_list = param.List(default=[])
    geo = param.ClassSelector(class_=Geo, default=Geo(None))
    
    @param.depends("lat_limits","lon_limits")
    def map_overlay(self):
        '''
        Create and update the map overlay of huc2s and lat/lon bounds
        
        Note about known issues - as currently structured, the map is rerendered whenever lat or lon bounds change
        via the slider widget.  This is slow and causes the stream (click-selected huc2s) to be reset to none.  
        Alternative to avoid this is use a dropdown selector to select HUC2, but less useful/appealing option since 
        users then need to know/remember the HUC2 by number.  Have not yet figured out how to get it to work with two 
        separate reactive functions for each layer (huc2s and lat/lon box) and overlay when building the dashboard 
        (i.e., cannot overlay a pn.Param(..) with another pn.Param(..)
        Suggestions (via GitHub issue tracker) welcome.
        '''
        if not self.geo.states.empty:
            crs = self.geo.states.crs
        else:
            crs = CRS.from_string('EPSG:4326')
        
        box_vertices = [[self.lon_limits[0], self.lat_limits[0]],
                        [self.lon_limits[1], self.lat_limits[0]],
                        [self.lon_limits[1], self.lat_limits[1]],
                        [self.lon_limits[0], self.lat_limits[1]]]
        
        box_poly = Polygon(box_vertices)
        latlon_box_gdf = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[box_poly]).to_crs(crs)
        latlon_box_gv = gv.Polygons(latlon_box_gdf, crs=ccrs.GOOGLE_MERCATOR, label='bounding box')
        latlon_box_gv.opts(color_index=None, fill_color='none', line_color='darkorange', line_dash='dashed', line_width=3)   
        
        states_gv = gv.Polygons(self.geo.states, vdims=['STUSPS'], crs=ccrs.GOOGLE_MERCATOR)
        states_gv.opts(color_index=None, fill_color='lightgray', nonselection_alpha=1, line_color='white', tools=[''], 
                        title='Select HUC2 polygons (hold shift to select multiple)', fontsize=12, width=700, height=450)
        
        huc2_gv = gv.Polygons(self.geo.huc2, vdims=['huc2'], crs=ccrs.GOOGLE_MERCATOR)
        huc2_gv.opts(color_index=None, fill_color='none', tools=['hover', 'tap'], selection_line_width=4)
        
        prior_selected_huc2_gv = gv.Polygons(self.geo.selected_huc2, vdims=['huc2'], crs=ccrs.GOOGLE_MERCATOR, label='prior selected HUC2s')
        prior_selected_huc2_gv.opts(color_index=None, fill_color='none', line_color='red', line_width=2, tools=[], nonselection_alpha=1)
        
        self.stream = hv.streams.Selection1D(source=huc2_gv)
                        
        return states_gv * huc2_gv * prior_selected_huc2_gv * latlon_box_gv 
    
class EventSelector(param.Parameterized):
    '''
    Main class for event selector dashboard, within which 'region' is the reactive parameterized subclass
    and 'button' launches a function to write the new/updated event specs to the event_definitions.json
    '''
    event_name = param.String(default="YYYYMM_name")
    event_start_date = param.Date(default=YESTERDAY, bounds=DATE_BOUNDS)
    event_end_date = param.Date(default=TODAY, bounds=DATE_BOUNDS)
    config = param.ClassSelector(class_=Config, default=Config(None))    
    region = param.ClassSelector(class_=RegionSelector, default=RegionSelector())  
    button = param.Action(lambda x: x.param.trigger('button'), label='Update/Store Event Specs')    

    @param.depends('button', watch=True)
    def update_event_definitions_file(self):
        '''
        Executed when the "update" button is clicked in the event_selector dashboard.
        Writes new or altered specs to the event_definitions.json file stored on disk 
        to prevent the need to redefine specs for the same event when loading more data
        or in the visualization notebooks
        '''
        self.update_event_specs()
        event_specs_for_json = {
                'huc2_list': self.config.event.huc2_list,
                'event_start_date': dt.datetime.combine(self.event_start_date, dt.time(hour=0)).strftime("%Y%m%d"),
                'event_end_date': dt.datetime.combine(self.event_end_date, dt.time(hour=0)).strftime("%Y%m%d"),
                'lat_limits': list(self.region.lat_limits),
                'lon_limits': list(self.region.lon_limits)
            }
        
        if self.event_name in self.config.data.existing_events.keys():
            updated_event_definitions = self.config.data.existing_events.copy()
            updated_event_definitions[self.event_name] = event_specs_for_json
        else:
            updated_event_definitions = {self.event_name : event_specs_for_json} | self.config.data.existing_events
        
        with open(self.config.data.event_defs_path, "w") as outfile:
            json.dump(updated_event_definitions, outfile, indent=4)
 
        print(f"{self.config.data.event_defs_path} updated")
    
    def update_event_specs(self):
        '''
        Update event specs based on dashboard selections
        '''
        
        selection_index = self.region.stream.index
        prior_selected_huc2_list = self.region.geo.selected_huc2.index.to_list()
        selected_huc2_list = get_updated_huc2_list(self.region.geo.huc2, selection_index, prior_selected_huc2_list)
        
        self.config.event.huc2_list = selected_huc2_list
        self.config.event.lat_limits = self.region.lat_limits
        self.config.event.lon_limits = self.region.lon_limits
        self.config.event.event_start_date = self.event_start_date
        self.config.event.event_end_date = self.event_end_date
        self.config.event.event_name = self.event_name   
        
        
class DataSelector(param.Parameterized):
    '''
    Main class for data_selector dashboard
    '''
    variable = param.ListSelector(default = ['streamflow'],
                                  objects=['streamflow','mean areal precipitation'])
    forecast_config = param.Selector(objects=['short_range','medium_range_mem1','none'])
    verify_config = param.ListSelector(default = ['USGS*','analysis_assim_extend', 'analysis_assim_extend_no_da*'],
                                       objects=['USGS*','analysis_assim_extend', 'analysis_assim_extend_no_da*', 'analysis_assim', 'analysis_assim_no_da*', 'none'])
    reach_set = param.Selector(objects=['gaged reaches','all reaches'])
    map_polygons = param.Selector(objects=['HUC10','usgs_basins'])
    config = param.ClassSelector(class_=Config, default=Config(None)) 
    
    now = dt.datetime.utcnow().replace(second=0, microsecond=0, minute=0, hour=0)
    bounds = (dt.datetime(2018, 9, 17, 0), now + dt.timedelta(days=30))
    
    event_value_time_start = param.Date(bounds=bounds)
    event_value_time_end = param.Date(bounds=bounds)
    ref_time_start = param.Date(bounds=bounds)
    ref_time_end = param.Date(bounds=bounds)      
    data_value_time_start = param.Date(bounds=bounds)
    data_value_time_end = param.Date(bounds=bounds)
    
    def intialize_dates(self):
        '''
        Sets default dates to populate date widgets in the data selector dashboard
        '''
        now = dt.datetime.utcnow().replace(second=0, microsecond=0, minute=0, hour=0)

        self.event_value_time_start = dt.datetime.combine(self.config.event.event_start_date, dt.time(hour=0))
        self.event_value_time_end= dt.datetime.combine(self.config.event.event_end_date, dt.time(hour=23))

        if self.forecast_config in ['medium_range','medium_range_mem1']:
            self.ref_time_start = dt.datetime.combine(self.config.event.event_start_date, dt.time(hour=0)) - dt.timedelta(days=10)
            self.ref_time_end = dt.datetime.combine(self.config.event.event_end_date, dt.time(hour=18))
        elif self.forecast_config in ['short_range']:
            self.ref_time_start = dt.datetime.combine(self.config.event.event_start_date, dt.time(hour=0)) - dt.timedelta(hours=18)
            self.ref_time_end = dt.datetime.combine(self.config.event.event_end_date, dt.time(hour=23))

        if self.forecast_config in ['medium_range','medium_range_mem1']:
            self.data_value_time_start = self.ref_time_start - dt.timedelta(hours=1)
            self.data_value_time_end = self.ref_time_end + dt.timedelta(days=10)
        elif self.forecast_config in ['short_range']:
            self.data_value_time_start = self.ref_time_start - dt.timedelta(hours=1)
            self.data_value_time_end = self.ref_time_end + dt.timedelta(hours=18)      
        elif self.forecast_config == 'none':
            self.data_value_time_start = dt.datetime.combine(self.config.event.event_start_date, dt.time(hour=0))
            self.data_value_time_end = dt.datetime.combine(self.config.event.event_end_date, dt.time(hour=23))
            
        if self.data_value_time_end > now:
            self.data_value_time_end = now
    
    def get_value_times(self):
        '''
        Get start and end values times that correspond to all time steps in a given set of 
        forecasts
        '''
        now = dt.datetime.utcnow().replace(second=0, microsecond=0, minute=0, hour=0)
        
        if self.forecast_config in ['medium_range','medium_range_mem1']:
            self.data_value_time_start = self.ref_time_start - dt.timedelta(hours=1)
            self.data_value_time_end = self.ref_time_end + dt.timedelta(days=10)
        elif self.forecast_config in ['short_range']:
            self.data_value_time_start = self.ref_time_start - dt.timedelta(hours=1)
            self.data_value_time_end = self.ref_time_end + dt.timedelta(hours=18)    
            
        if self.data_value_time_end > now:
            self.data_value_time_end = now            
        
#### dashboard functions

def get_event_selector(
    config: Config, 
    #geo: Geo,
) -> EventSelector:
    '''
    Wrapper to define an EventSelector class
    '''
    event_selector = EventSelector(
        event_name = config.event.event_name, 
        event_start_date = config.event.event_start_date, 
        event_end_date = config.event.event_end_date,
        config=config,
        region=RegionSelector(
            lat_limits=config.event.lat_limits, 
            lon_limits=config.event.lon_limits, 
            huc2_list=config.event.huc2_list,
            #geo=geo)
            geo=config.geo)
    )
    return event_selector
    
def get_event_selector_legend() -> hv.Overlay:
    '''
    separate legend explaining layers in the region map (for more control than possible
    in holoviews automatic legends)
    '''
    
    huc2=hv.Curve([(0,2),(.5,2)]).opts(fontscale=0.5, xlim=(-0.2,3),ylim=(-1,3), 
                               toolbar=None, height=100, width=250, color='black', line_width=1, xaxis=None, yaxis=None)
    prior_huc2=hv.Curve([(0,1),(.5,1)]).opts(color='red', line_width=2)
    latlon_box=hv.Curve([(0,0),(.5,0)]).opts(color='orange', line_color='darkorange', line_dash='dashed', line_width=3)
    text_huc2=hv.Text(0.7,2,'All HUC2s').opts(color='black', text_align='left', text_font_size='10pt')
    text_sel_huc2=hv.Text(0.7,1,'Prior-selected HUC2s').opts(color='black', text_align='left', text_font_size='10pt')
    text_latlon=hv.Text(0.7,0,'Lat/Lon limits').opts(color='black', text_align='left', text_font_size='10pt')
    
    return huc2*prior_huc2*latlon_box*text_huc2*text_sel_huc2*text_latlon
    
def build_event_selector_dashboard(event_selector):
    '''
    build and combine panel components for event selector dashboard
    '''
    dates_view = pn.Param(
        event_selector,
        widgets={
            "event_name" : {"type": pn.widgets.TextInput, "name" : "Event Name (YYYYMM_name):", "value": event_selector.event_name},
            "event_start_date": {"type": pn.widgets.DatePicker, "name" : "Event Start Date:", "value" : event_selector.event_start_date},
            "event_end_date": {"type": pn.widgets.DatePicker, "name" : "Event End Date:", "value" : event_selector.event_end_date},
        },
        parameters=["event_name", "event_start_date", "event_end_date"],
        show_name=False,
        default_layout=pn.Row,
        width=600
    )
    button_view = pn.Param(
        event_selector,
        widgets={
            "button" : {"type": pn.widgets.Button, "name" : "Update/Store Event Specs", "button_type":"primary"},
        },
        parameters=["button"],
        show_name=False,
        width=200
    )
    latlon_limits_view = pn.Column(
        pn.Param(
            event_selector.region,
            show_name=False,
            widgets = {
                "lon_limits": {"type": pn.widgets.IntRangeSlider, "name": "Lon limits", 
                           "start": -127, "end":-64, "step":1, "throttled": True}, 
                "lat_limits": {"type": pn.widgets.IntRangeSlider, "name": "Lat limits", 
                           "start": 22, "end": 55, "step":1, "throttled": True},  
            },
            parameters=["lon_limits", "lat_limits"]
        ),
    )
    region_view = pn.Row(
        pn.Column(
            latlon_limits_view,
            get_event_selector_legend(),
            pn.Row(pn.Spacer(width=10),
                   pn.pane.Markdown("### *Analysis region will be the intersection of selected HUC2s (if any) and lat/lon limits", width=250)),        
            pn.Spacer(height=30),
            button_view,
        ),
        event_selector.region.map_overlay,
        sizing_mode='stretch_width'
    )
    return pn.Column(
                pn.pane.Markdown(f"## Make changes if desired to event name, dates, or region (HUC2s and/or lat-lon box), then click the Update button."),
                pn.pane.Markdown("### Event Name and Dates:"), 
                dates_view, 
                pn.pane.Markdown("### Event Region*:"), 
                region_view,
                sizing_mode='stretch_width',
            )
    
def build_data_selector_dashboard(data_selector):
    '''
    build and combine panel components for data selector dashboard
    '''
    data_selector.intialize_dates()
    
    source = pn.Param(
            data_selector,
            widgets={
                "forecast_config": {"name" : "NWM Forecast Configuration (select one)"},
                "verify_config": {"name" : "Analysis/Obs Source (Ctrl to select multiple)"},
            },                
            parameters=["forecast_config", "verify_config"],
            show_name=False,
            default_layout=pn.Column,
        )
    fcst_widget = source.widget('forecast_config')
    obs_widget = source.widget('verify_config')

    date_start = pn.widgets.DatePicker(
        name='First Reference/Issue Date to Load:', 
        value=data_selector.ref_time_start.date(),
    )
    date_end = pn.widgets.DatePicker(
        name='Last Reference/Issue Date to Load:', 
        value=data_selector.ref_time_end.date(),
    )
    var = pn.Param(
            data_selector,
            widgets={
                "variable": {"name" : "Variable (Ctrl to select both)"},
            },
            parameters=["variable"],
            show_name=False,
            default_layout=pn.Column,
        )
    unit = pn.Param(
            data_selector,
            widgets={
                "reach_set": {"name" : "NWM Reach Set (for streamflow):"},
                "map_polygons": {"name" : "MAP Polygons (for precipitation)"},
            },
            parameters=["reach_set", "map_polygons"],
            show_name=False,
            default_layout=pn.Column,
        )
    reach_widget = unit.widget('reach_set')
    map_widget = unit.widget('map_polygons')
    
    # Explanatory footnotes at the bottom of the dashboard change based on source selections
    # Setting initial footnote values here
    forecast_selected_footnote1 = ' - Default dates are the first and last reference/issue dates of forecasts that overlap the event dates.'
    forecast_selected_footnote2 = ' - All timesteps of the selected forecasts will be loaded.'
    forecast_selected_footnote3 = ' - Observed/analysis data for corresponding valid dates will be loaded.'
    footnote4 = ''
    no_forecast_selected_footnote1 = '- Default dates are the analysis/obs valid dates that overlap the event dates (no forecasts are selected).'
    footnote_styles = {'font-size':'10pt', 'font-weight':'bold', 'color':'black'}
    warning_styles = {'font-size':'12pt', 'font-weight':'bold', 'color':'red'}
    
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
    # Get initial # of reaches and location footnote text
    if reach_widget.value == 'all reaches':
        location_footnote_text = f" ({len(data_selector.config.event.nwm_id_list)} NWM reaches in selected region)"
    else:
        # if no usgs gages in the region, reset to 'all reaches'
        if len(data_selector.config.event.usgs_id_list) == 0:
            reach_widget.value = 'all reaches'
            location_footnote_text = f" ({len(data_selector.config.event.nwm_id_list)} NWM reaches in selected region)"
        else:
            location_footnote_text = f" ({len(data_selector.config.event.nwm_id_list)} NWM gaged reaches in selected region)"
    
    location_footnote = pn.widgets.StaticText(
        value=location_footnote_text,
        styles={'font-size':'10pt'},
        margin=(0,0,20,20),
    )
    # Get initial # of polygons and polygon footnote text
    if map_widget.value == 'HUC10':
        map_footnote_text = f" ({len(data_selector.config.event.huc10_list)} HUC10 polygons in selected region)"
    else:
        map_footnote_text = f" ({len(data_selector.config.event.usgs_id_list)} USGS basins in selected region)"    
    
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
        pn.pane.Markdown(f"## Select data to load for event ```{data_selector.config.event.event_name}``` and event dates: ```{data_selector.event_value_time_start.date()}``` to ```{data_selector.event_value_time_end.date()}```."),
        pn.pane.Markdown(f"### Event Dates: {data_selector.event_value_time_start.date()} to {data_selector.event_value_time_end.date()}"), 
        pn.pane.Markdown("### Source Selections:"), 
        pn.Row(pn.Column(fcst_widget, obs_widget, obs_footnote), var, pn.Column(reach_widget, location_footnote, map_widget, map_footnote)),
        pn.pane.Markdown(f"### Dates of Data to Load:"),
        pn.Row(date_start, date_end),
        pn.Spacer(height=20),
        footnote1,footnote2,footnote3,footnote4,
        pn.Spacer(height=100),
        pn.pane.Markdown("#### ------ (blank space added so date selectors are visible without scrolling the cell) ------")
    )   
    # update date footnotes if source selections change
    @pn.depends(fcst_widget.param.value, obs_widget.param.value, watch=True)
    def update_footnote_text(fcst_widget, obs_widget):
        data_selector.forecast_config = fcst_widget
        data_selector.intialize_dates()

        if fcst_widget != 'none':
            date_start.value = data_selector.ref_time_start.date()
            date_start.name = 'First Reference/Issue Date to Load:'
            date_end.value = data_selector.ref_time_end.date()
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
            date_start.value = data_selector.event_value_time_start.date()
            date_start.name = 'First Valid Date to Load:'
            date_end.value = data_selector.event_value_time_end.date()
            date_end.name = 'Last Valid Date to Load:'
            footnote1.value = no_forecast_selected_footnote1
            footnote2.value = ''
            footnote3.value = ''
            footnote1.styles = footnote_styles
        else:
            footnote1.value = '!!Forecast and observed selections cannot both be \'none\', choose at least one configuration to load'
            footnote2.value = ''
            footnote3.value = ''
            footnote1.styles = warning_styles
  
    # update num of reaches and footnote text if reach set changes
    @pn.depends(reach_widget.param.value, watch=True)
    def update_reach_count(reach_widget):
        if reach_widget == 'all reaches':
            data_selector.config.event.nwm_id_list = get_nwm_id_list_as_int(data_selector.config)
            location_footnote.value = f" ({len(data_selector.config.event.nwm_id_list)} NWM reaches in selected region)"
        else:
            data_selector.config.event.nwm_id_list = get_nwm_id_list_as_int(data_selector.config, data_selector.config.event.usgs_id_list)
            location_footnote.value = f" ({len(data_selector.config.event.nwm_id_list)} NWM gaged reaches in selected region)"
     
    # update num of reaches and footnote text if polygon set changes
    @pn.depends(map_widget.param.value, watch=True)
    def update_map_count(map_widget):
        if map_widget == 'HUC10':
            map_footnote.value = f" ({len(data_selector.config.event.huc10_list)} HUC10 polygons in selected region)"
        else:
            map_footnote.value = f" ({len(data_selector.config.event.usgs_id_list)} USGS basins in selected region)"

    # validate and update footnotes (if warning needed) if dates change
    @pn.depends(date_start.param.value, date_end.param.value, watch=True)
    def update_dates(date_start, date_end):
    
        now = dt.datetime.utcnow().replace(second=0, microsecond=0, minute=0, hour=0)
        
        if date_start > date_end:
            footnote1.value = '!! WARNING - Invalid dates.  Start date must be before end date.  Choose different dates.'
            footnote2.value = ''
            footnote3.value = ''
            footnote1.styles = warning_styles
        elif date_start > now.date():
            footnote1.value = '!! WARNING - Invalid dates.  Start date cannot be in the future.  Choose different dates.'
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
                data_selector.ref_time_start = dt.datetime.combine(date_start, dt.time(hour=0))
                data_selector.ref_time_end = dt.datetime.combine(date_end, dt.time(hour=23))
                data_selector.get_value_times()

            elif fcst_widget.value == 'none' and obs_widget.value != ['none']:
                footnote1.value = no_forecast_selected_footnote1
                footnote2.value = ''
                footnote3.value = ''
                footnote1.styles = footnote_styles 
                data_selector.data_value_time_start = dt.datetime.combine(date_start, dt.time(hour=0))
                data_selector.data_value_time_end = dt.datetime.combine(date_end, dt.time(hour=23))
                
            if date_end > now.date():
                footnote4.value = '!! NOTE - End Date is in the future.  Any missing or future datetimes will be ignored and will not crash the loading process.'
                footnote4.styles = warning_styles
            else:
                footnote4.value = ''

        if data_selector.data_value_time_end > now:
            data_selector.data_value_time_end = now 
            
    return layout
    
    
#### data processing and geometry functions

def get_nwm_version(
    event: EventSpecs,
) -> float:
    '''
    Get NWM version based on event dates
    '''
    version_start = nwm_version(dt.datetime.combine(event.event_start_date, dt.time(hour=0)))
    version_end = nwm_version(dt.datetime.combine(event.event_end_date, dt.time(hour=23)))
    
    if version_start != version_end:
        raise ValueError("Dates span two different NWM versions - currently not supported")
        
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
        #version = 3.0
        version = 2.2  # temporary until geo files are created for 3.0
    elif date >= v22_date:
        version = 2.2
    elif date >= v21_date:
        version = 2.1
    else:
        version = 2.0
        
    return version

def read_json_definitions(
    definitions_filepath: Path
) -> List[Path]:
    '''
    read protocol or study definitions from a json file
    '''
    try:
        with open(definitions_filepath) as file:
            file_contents = file.read()
        parsed_json = json.loads(file_contents)
    except:
        if definitions_filepath == 'post_event_config.json':
            raise OSError(f"{definitions_filepath} not found")
        else:
            print(f"{definitions_filepath} not found, no prior events available")
        parsed_json = {}
    
    return parsed_json

def get_updated_huc2_list(
    huc2_gdf: gpd.GeoDataFrame, 
    selection_index: List[int], 
    prior_huc2_list: List[str],
) -> List[str]:
    '''
    Update selected HUC2s 
        If new HUC2s are selected - use only those
        Otherwise use the prior selected HUC2s, if any
    '''
    selected_huc2_list = huc2_gdf.iloc[selection_index].index.to_list()
    
    # if no new selection, updated list is the prior list
    if not selected_huc2_list:
        
        # if no prior list, updated list is empty
        if not prior_huc2_list:
            updated_huc2_list = []
        else:
            updated_huc2_list = prior_huc2_list
            
    # if there is a new selection, updated list is the new selections
    else:
        updated_huc2_list = selected_huc2_list

    return updated_huc2_list

def get_usgs_id_list_as_str(
    config: Config,
    ) -> list[str]:    
    '''
    Get a list of USGS IDs as strings and without prefix for input to the USGS data loading function
    '''
    points_selected_gdf = get_point_features_subset(config, config.geo.usgs_points, config.geo.cross_usgs_huc).to_crs(3857)
    usgs_ids = [s.replace('usgs-','') for s in points_selected_gdf['id']] 
        
    return usgs_ids

def get_nwm_id_list_as_int(
    config: Config,
    usgs_ids: list[str] = [],
    ) -> list[str]:
    '''
    Get a list of NWM feature IDs as integers and without prefix for input to the NWM loading function
    List is based on
    1) crosswalk to usgs ids (if provided) or
    2) within hucs defined by 'huc2_list' attribute of config.event
    '''
    if usgs_ids:
        usgs_ids_with_prefix = ['-'.join(['usgs', s]) for s in usgs_ids]  
        nwm_ids_with_prefix = get_crosswalked_id_list(usgs_ids_with_prefix, config.geo.cross_usgs_nwm, input_list_column = 'primary_location_id')   
    else:
        print('getting list of all nwm reaches in the selected HUC2(s)...')
        huc_gdf = get_hucs_subset(config, huc_level = 10)
        huc10_list = huc_gdf['id'].to_list() 
        
        # UPDATE once we have 3.0 geometry created (if different than 2.2)!!!
        
        if config.event.nwm_version in [2.1, 2.2, 3.0]:
            nwm_huc12_crosswalk = pd.read_parquet(Path(config.data.geo_dir, config.json["CROSSWALK_FILES_CONUS"]["NWM22_HUC12"]))
        elif config.event.nwm_version in [1.2]:
            nwm_huc12_crosswalk = pd.read_parquet(Path(config.data.geo_dir, config.json["CROSSWALK_FILES_CONUS"]["NWM12_HUC12"]))
            
        nwm_huc10_crosswalk = nwm_huc12_crosswalk.copy()
        nwm_huc10_crosswalk['secondary_location_id'] = nwm_huc12_crosswalk['secondary_location_id'].str.replace('huc12','huc10').str[:16]
        nwm_ids_with_prefix = get_crosswalked_id_list(huc10_list, nwm_huc10_crosswalk, 'secondary_location_id')       
        
    # remove the prefix
    nwm_version_prefix = 'nwm' + str(config.event.nwm_version).replace('.','') + '-'
    nwm_ids = list(map(int, [s.replace(nwm_version_prefix,'') for s in nwm_ids_with_prefix]))   
        
    return nwm_ids
    
def get_crosswalked_id_list(
    id_list: list,
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
            
        crosswalk_subset = crosswalk[crosswalk[input_list_column].isin(id_list)]
        lookup_list = crosswalk_subset[lookup_column].to_list()
        
    else:
        lookup_list = []
        
    return lookup_list 
    
def get_latlon_box(
    config: Config,
    ) -> Polygon:
    '''
    Get a rectangular polygon object based on lat/lon limits
    '''
    box =  Polygon(
        [[config.event.lon_limits[0], config.event.lat_limits[0]], 
         [config.event.lon_limits[0], config.event.lat_limits[1]], 
         [config.event.lon_limits[1], config.event.lat_limits[1]], 
         [config.event.lon_limits[1], config.event.lat_limits[0]]]
    )  

    return box
    
def get_point_features_subset(
    config: Config,
    point_gdf: gpd.GeoDataFrame,    
    point_huc12_crosswalk: pd.DataFrame,
    ) -> gpd.GeoDataFrame:  
    '''
    get a subset of points within both the list of HUC2s and lat/lon box 
    '''
    
    # subset crosswalk by huc2
    huc2_strings = ['-'.join(['huc12', a]) for a in config.event.huc2_list]
    point_huc12_crosswalk_subset1 = point_huc12_crosswalk[point_huc12_crosswalk['secondary_location_id'].str.contains('|'.join(huc2_strings))]
    
    # subset geometry by latlon box
    latlon_box = get_latlon_box(config)
    point_gdf_subset = point_gdf[latlon_box.contains(point_gdf['geometry'])]    
    
    # get the intersection of the two subsets
    point_gdf_subset2 = point_gdf_subset[point_gdf_subset['id'].isin(point_huc12_crosswalk_subset1['primary_location_id'])].copy()
    
    # print warning if there is no intersection, returning empty geodataframe
    if len(point_huc12_crosswalk_subset1) > 0 and len(point_gdf_subset) > 0 and len(point_gdf_subset2) == 0:
        print('Warning - no gages found in intersecting area of HUC2s and lat-lon box (or the two do not overlap). Check region selections.')
    
    return point_gdf_subset2

def get_hucs_subset(
    config: Config,
    huc_level: int = 10,
    ) -> list:
    '''
    get a subset of HUCs (currently HUC10 or 12 available)
    '''
    huc_attribute = 'huc' + str(huc_level)
    try:
        huc_gdf = getattr(config.geo, huc_attribute)
    except:
        raise ValueError(f"HUC {huc_level} geometry not found")
        
    # subset hucX geometry by huc2
    huc_level_str = 'huc' + str(huc_level).zfill(2)
    huc2_strings = ['-'.join([huc_level_str, a]) for a in config.event.huc2_list]
    huc_gdf_subset1 = huc_gdf[huc_gdf['id'].str.contains('|'.join(huc2_strings))].copy()
    
    # further subset geometry by latlon box (where polygon centroid falls within box)
    latlon_box = get_latlon_box(config)
    huc_gdf_subset1['centroid'] = huc_gdf_subset1['geometry'].to_crs(3857).centroid.to_crs(4326)
    huc_gdf_subset2 = huc_gdf_subset1[latlon_box.contains(huc_gdf_subset1['centroid'])]  
    
    return huc_gdf_subset2[['id','name','geometry']]


#### data loading functions

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
    config: Config,
    data_selector: DataSelector,
):
    '''
    Launch TEEHR loading functions for streamflow data sources based on data selections
    '''
    # only load streamflow data if it is a selected variable
    if 'streamflow' in data_selector.variable:
    
        # set a subdirectory name (under forecast/obs config dir) to keep the smaller dataset 
        # (gages only) separate from the larger so that exploring the smaller dataset is fast
        if data_selector.reach_set == 'all reaches':
            timeseries_subdir = 'all'
        else:
            timeseries_subdir = 'gages'
            
        # load selected forecast data if any
        if data_selector.forecast_config != 'none':
            t_start = time.time()
            
            # get sub-directories and # days of forecasts to load
            n_days = (data_selector.ref_time_end - data_selector.ref_time_start).days + 1
            ts_dir_config = Path(config.data.ts_dir, data_selector.forecast_config, timeseries_subdir)    
            json_dir_config = Path(config.data.json_dir, data_selector.forecast_config)
            
            # set and alter the output group if medium range
            output_group = 'channel_rt'
            if data_selector.forecast_config == 'medium_range_mem1':
                output_group = 'channel_rt_1'
            
            print(f"Loading {data_selector.forecast_config} streamflow for {len(config.event.nwm_id_list)} NWM reaches from {data_selector.ref_time_start} to {data_selector.ref_time_end}")
            tlp.nwm_to_parquet(
                data_selector.forecast_config,
                output_group,
                'streamflow',
                data_selector.ref_time_start,
                n_days,
                config.event.nwm_id_list,
                json_dir_config,
                ts_dir_config,
                ignore_missing_file = True
            )
            print(f"...{data_selector.forecast_config} streamflow loading complete in {round((time.time() - t_start)/60,5)} minutes\n")   
    
        # load USGS data if selected
        if 'USGS*' in data_selector.verify_config:
            t_start = time.time()
            
            # get sub-directory then launch loading function
            ts_dir_config = Path(config.data.ts_dir, 'usgs')
            print(f"Loading USGS streamflow for {len(config.event.usgs_id_list)} gages from {data_selector.data_value_time_start} to {data_selector.data_value_time_end}") 
            tlu.usgs_to_parquet(
                config.event.usgs_id_list,
                data_selector.data_value_time_start,
                data_selector.data_value_time_end,
                ts_dir_config,
                chunk_by = 'day'
            )
            print(f"...USGS loading complete in {round((time.time() - t_start)/60,5)} minutes\n") 
 
        # load NWM analysis data if selected
        # currently only CONUS!!!
        
        # List of valid analysis options for streamflow
        ana_list = ['analysis_assim_extend', 'analysis_assim', 'analysis_assim_extend_no_da*', 'analysis_assim_no_da*']
        n_days = (data_selector.data_value_time_end - data_selector.data_value_time_start).days + 1
        
        # loop through analysis types, load any selected
        for ana in ana_list:
            if ana in data_selector.verify_config:
            
                # remove * at end of streamflow-only types
                if ana[-1] == '*':
                    ana = ana[:-1]
                
                # set timer, directories, and timestep range to include
                t_start = time.time()
                ts_dir_config = Path(config.data.ts_dir, ana, timeseries_subdir)
                json_dir_config = Path(config.data.json_dir, ana)
                if 'extend' in ana:
                    tm_range = range(0,28)
                else:
                    tm_range = range(0,2)   

                # launch loading function
                print(f"Loading {ana} streamflow for {len(config.event.nwm_id_list)} NWM reaches from {data_selector.data_value_time_start} to {data_selector.data_value_time_end}")
                tlp.nwm_to_parquet(
                    ana,
                    'channel_rt',
                    'streamflow',
                    data_selector.data_value_time_start,
                    n_days,        
                    config.event.nwm_id_list,
                    json_dir_config,
                    ts_dir_config,
                    tm_range,
                    ignore_missing_file = True
                )
                print(f"...{ana} streamflow loading complete in {round((time.time() - t_start)/60,5)} minutes\n")
                
                
def launch_teehr_precipitation_loading(
    config: Config,
    data_selector: DataSelector,
):
    '''
    Launch TEEHR loading functions for precipitation data sources based on data selections
    '''
    # only load precipitation data if it is a selected variable
    if 'mean areal precipitation' in data_selector.variable:
        
        # set a subdirectory name (under forecast/obs config dir) to keep the parquet files for 
        # different polygon sets separate (HUC10s or USGS basins for now)
        if any(s in data_selector.map_polygons for s in ['huc10','HUC10']):
            timeseries_subdir = 'huc10'
            n_polys = len(config.event.huc10_list)
            polygon_set = 'HUC10s'
        elif any(s in data_selector.map_polygons for s in ['usgs','USGS']):
            timeseries_subdir = 'usgs_basins'
            n_polys = len(config.event.usgs_id_list)
            polygon_set = 'USGS basins'
            
        # valid analysis configurations for precipitation
        ana_list = ['analysis_assim_extend', 'analysis_assim']
        
        # write subset of weights to temporary file (necessary to avoid memory issues when passing in memory for distributed computing)
        config.geo.write_grid_weights_subset(config, data_selector.map_polygons)       
        
        # load selected forecast data if any
        if data_selector.forecast_config != 'none':
            t_start = time.time()
            
            # add the prefix for forcing config
            forcing_forecast_configuration = 'forcing_' + data_selector.forecast_config
            if forcing_forecast_configuration == 'forcing_medium_range_mem1':
                forcing_forecast_configuration = 'forcing_medium_range'                
            
            # get sub-directories and # days of forecasts to load
            n_days = (data_selector.ref_time_end - data_selector.ref_time_start).days + 1
            ts_dir_config = Path(config.data.ts_dir, forcing_forecast_configuration, timeseries_subdir)    
            json_dir_config = Path(config.data.json_dir, forcing_forecast_configuration)

            # launch the loading function
            print(f"Loading {forcing_forecast_configuration} mean areal precipitation for {n_polys} {polygon_set} from {data_selector.ref_time_start} to {data_selector.ref_time_end}")
            tlg.nwm_grids_to_parquet(
                forcing_forecast_configuration,
                'forcing',
                'RAINRATE',
                data_selector.ref_time_start,
                n_days,
                Path(config.data.geo_dir, 'temp_grid_weights_subset.parquet'),
                json_dir_config,
                ts_dir_config,
                ignore_missing_file = True
            )
            print(f"...{forcing_forecast_configuration} mean areal precipitation loading complete in {round((time.time() - t_start)/60,5)} minutes\n")
        else:
            # if no forecasts were selected nor any valid analysis type, no data to load - print message
            if not any(s in data_selector.verify_config for s in ana_list):
                print('No data loaded - no valid datasets selected')
                
        # loop through selected analysis types
        n_days = (data_selector.data_value_time_end - data_selector.data_value_time_start).days + 1
        for ana in ana_list:          
            if ana in data_selector.verify_config:
                t_start = time.time()
                
                # adjust config name, set directories and timesteps to include
                forcing_ana_config = 'forcing_' + ana
                ts_dir_config = Path(config.data.ts_dir, forcing_ana_config, timeseries_subdir)
                json_dir_config = Path(config.data.json_dir, forcing_ana_config)
                if 'extend' in ana:
                    tm_range = range(0,28)
                else:
                    tm_range = range(0,2)   
                
                # launch loading function
                print(f"Loading {ana} mean areal precipitation for {n_polys} {polygon_set} from {data_selector.data_value_time_start} to {data_selector.data_value_time_end}")
                tlg.nwm_grids_to_parquet(
                    forcing_ana_config,
                    'forcing',
                    'RAINRATE',
                    data_selector.data_value_time_start,
                    n_days,        
                    Path(config.data.geo_dir, 'temp_grid_weights_subset.parquet'),
                    json_dir_config,
                    ts_dir_config,
                    tm_range,
                    ignore_missing_file = True
                )
                print(f"...{forcing_ana_config} mean areal precipitation loading complete in {round((time.time() - t_start)/60,5)} minutes\n")