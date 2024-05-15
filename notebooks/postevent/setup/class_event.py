'''
parameterized class for event selector dashboard
'''
import json
import param
import datetime as dt
import geoviews as gv
import holoviews as hv
import geopandas as gpd
import numpy as np
import cartopy.crs as ccrs
hv.extension('bokeh', logo=False)
gv.extension('bokeh', logo=False)

from .. import config
from .. import utils

# date constants as type datetime.date
TODAY = dt.datetime.now().date()
YESTERDAY = TODAY - dt.timedelta(days=1)
DATE_BOUNDS = (dt.date(2018, 9, 17), TODAY)
        
class RegionSelector(param.Parameterized):
    '''
    Map portion and reactive portion of the event selector dashboard
    '''
    geo = param.ClassSelector(
        class_=config.Geo, 
        default=config.Geo(None, None)
    )

    def map_overlay(self):
        '''
        Create and update the map overlay of huc2s and lat/lon polygon
        '''

        basemap = hv.element.tiles.CartoLight().opts(
                    width=700, 
                    height=450,   
                    xlim=self.geo.domain_limits['xlims_mercator'], 
                    ylim=self.geo.domain_limits['ylims_mercator'], 
                )
        
        states_gv = gv.Polygons(
            self.geo.states.to_crs(3857), 
            vdims=['STUSPS'], 
            crs=ccrs.GOOGLE_MERCATOR
        )
        states_gv.opts(
            color_index=None, 
            fill_color='none', 
            nonselection_alpha=1, 
            line_color='lightgray',    
            tools=[''], 
            width=700, 
            height=450, 
            show_title=False, 
            #fontsize=12,
            xlim=self.geo.domain_limits['xlims_mercator'], 
            ylim=self.geo.domain_limits['ylims_mercator']
        )
        
        # all hucs2
        huc2_gv = gv.Polygons(
            self.geo.huc2.to_crs(3857), 
            vdims=['id'], 
            crs=ccrs.GOOGLE_MERCATOR
        )
        huc2_gv.opts(
            color_index=None, 
            fill_color='none', 
            tools=['hover', 'tap'], 
            selection_line_width=4
        )
        self.huc_stream = hv.streams.Selection1D(source=huc2_gv)

        # event-selected huc2(s) (if any)
        if not self.geo.huc2_subset.empty:
            prior_huc2_subset_gv = gv.Polygons(
                self.geo.huc2_subset.to_crs(3857), 
                vdims=['id'], 
                crs=ccrs.GOOGLE_MERCATOR, 
                label='selected HUC2s'
            )
            prior_huc2_subset_gv.opts(
                color_index=None, 
                fill_color='none', 
                line_color='red', 
                line_width=2, 
                tools=[], 
                nonselection_alpha=1
            )
        else:
            prior_huc2_subset_gv = gv.Polygons([])
        
        # event-selected polygon boundary (if any)
        if self.geo.region_polygon:
            poly_gdf = gpd.GeoDataFrame(
                index=[0], 
                geometry=[self.geo.region_polygon]
            )
            poly = gv.Polygons(
                poly_gdf, 
                label='bounding polygon'
            )
        else:
            poly = gv.Polygons([])
        poly.opts(
            fill_color='none', 
            line_color='darkorange', 
            line_dash='dashed', 
            line_width=3
        ) 
        self.poly_stream = hv.streams.PolyDraw(
            source=poly, 
            vertex_style={'color': 'darkorange'}, 
            num_objects=1
        )  

        return basemap * states_gv * huc2_gv * prior_huc2_subset_gv * poly

class EventSelector(param.Parameterized):
    '''
    Main class for event selector dashboard, within which 
    'region' is the reactive parameterized subclass and 'button' 
    launches a function to write the new/updated event specs to 
    the event_definitions.json
    '''
    dir_name = param.String(
        default="YYYYMM_name"
    )
    event_start_date = param.Date(
        default=YESTERDAY, 
        bounds=DATE_BOUNDS
    )
    event_end_date = param.Date(
        default=TODAY, 
        bounds=DATE_BOUNDS
    )
    event = param.ClassSelector(
        class_=config.Event, 
        default=config.Event(None)
    )    
    paths = param.ClassSelector(
        class_=config.Paths, 
        default=config.Paths(None)
    )    
    region = param.ClassSelector(
        class_=RegionSelector, 
        default=RegionSelector()
    )  
    button = param.Action(
        lambda x: x.param.trigger('button'), 
        label='Update/Store Event Specs'
    )    

    @param.depends('button', watch=True)
    def update_event_definitions_file(self):
        '''
        Executed when the "update" button is clicked 
        in the event_selector dashboard. Writes new or 
        altered specs to the event_definitions.json file 
        stored on disk to prevent the need to redefine specs 
        for the same event when loading more data
        or in the visualization notebooks
        '''        
        self.update_event_specs()       
        event_specs_for_json = {
                'huc2_list': self.event.huc2_list,
                'event_start_date': dt.datetime.combine(
                    self.event_start_date, 
                    dt.time(hour=0)
                ).strftime("%Y%m%d"),
                'event_end_date': dt.datetime.combine(
                    self.event_end_date, 
                    dt.time(hour=0)
                ).strftime("%Y%m%d"),
                'region_boundary_coords': (
                    np.around(self.event.region_polygon.exterior.xy,1)
                ).tolist(), 
            }
        if self.dir_name in self.paths.existing_events.keys():
            updated_event_definitions = self.paths.existing_events.copy()
            updated_event_definitions[self.dir_name] = event_specs_for_json
        else:
            updated_event_definitions = {
                self.dir_name : event_specs_for_json
            } | self.paths.existing_events
        
        with open(self.paths.event_defs_file, "w") as outfile:
            json.dump(updated_event_definitions, outfile, indent=4)
 
        print(f"{self.paths.event_defs_file} updated")
    
    def update_event_specs(self):
        '''
        Update event specs based on dashboard selections
        '''        
        # read selected huc2(s) from stream
        huc2_selection_index = self.region.huc_stream.index
        huc2_selection_list =  self.region.geo.huc2.loc[
            huc2_selection_index
        ]['id'].to_list()

        # read prior-selected huc2(s) from initialized subset
        if not self.region.geo.huc2_subset.empty:
            prior_huc2_subset_list = self.region.geo.huc2_subset[
            'id'
            ].to_list()
        else:
            prior_huc2_subset_list = []

        # if new selection, merged list is the new list
        if huc2_selection_list:
            merged_huc2_list = huc2_selection_list
        # if no new selection, but prior selection, new list is the prior list
        elif not huc2_selection_list and prior_huc2_subset_list:
            merged_huc2_list = prior_huc2_subset_list
        # if neither exist, empty list
        elif not huc2_selection_list and not prior_huc2_subset_list:
            merged_huc2_list = []
        
        self.event.huc2_list = merged_huc2_list
        self.event.region_polygon = utils.geom.get_polygon_from_poly_stream(
            self.region.poly_stream
        )
        self.event.event_start_date = self.event_start_date
        self.event.event_end_date = self.event_end_date
        self.event.dir_name = self.dir_name    

        # update the timeseries dir based on new event name
        self.paths.set_data_paths(event_name = self.event.dir_name)

        # update the NWM crosswalk filename for TEEHR queries based on nwm version
        if hasattr(self.paths, "streamflow_filepaths"):
            self.paths.update_streamflow_path_nwm_version(
                self.event.nwm_version
            )

        # read the usgs-nwm and huc-nwm crosswalks based on NWM version
        self.region.geo.read_usgs_nwm_crosswalk_version(
            self.paths, 
            self.event.nwm_version
        )
        self.region.geo.read_nwm_huc_crosswalk_version(
            self.paths, 
            self.event.nwm_version
        )  

        # get the huc subsets
        self.region.geo.get_huc_subsets(self.event)
        
        # update usgs and nwm lists
        self.event.get_location_lists(
            self.paths, 
            self.region.geo
        )
        
