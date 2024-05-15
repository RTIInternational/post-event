''' 
Main parameterized class for post-event visualization dashboards
'''
import param
import datetime as dt
import pandas as pd
import geopandas as gpd
import numpy as np
import datashader as ds
import colorcet as cc
import holoviews as hv
import geoviews as gv
hv.extension('bokeh', logo=False)
gv.extension('bokeh', logo=False)

from holoviews.operation.datashader import rasterize
from bokeh.models import HoverTool
from bokeh.models import DatetimeTickFormatter
from shapely import Point

from .. import utils
from .. import config
from . import data

import importlib
importlib.reload(utils)
importlib.reload(data)

class ForecastExplorer(param.Parameterized):
    '''
    Parameterized class containing parameters, 
    methods to create holoviews elements for dashboards
    and associated utilities
    '''

    # parameters
    paths = param.ClassSelector(
        class_=config.Paths, 
        default=config.Paths(None)
    ) 
    event = param.ClassSelector(
        class_=config.Event, 
        default=config.Event(None)
    ) 
    geo = param.ClassSelector(
        class_=config.Geo, 
        default=config.Geo(None, None)
    )
    dates = param.ClassSelector(
        class_=config.Dates, 
        default=config.Dates(None, None)
    )    
    explore_precip = param.Boolean(False)
    explore_streamflow = param.Boolean(False)    
    ref_time_str = param.String(default=None)
    reach_set = param.String(default='gages')    
    map_polygons = param.String(default='huc10')  
    ts_polygons = param.String(default='huc10')  

    # holoviews streams
    coord_stream = hv.streams.Tap(x=np.nan, y=np.nan)
    point_stream = hv.streams.Selection1D(index=[np.nan])

    
    def initialize(
        self, 
        restrict_to_event_period = False,
    ):
        '''
        initialize settings and data
        '''
    
        # flag to restrict the analysis to include only time steps 
        # overlapping the event dates, rather than all timesteps in the
        # forecasts (empty list if not defined)
        self.dates.get_analysis_value_times(restrict_to_event_period)

        # get data
        if self.explore_precip:
            self.get_precip()
        if self.explore_streamflow:
            self.paths.set_streamflow_paths(
                nwm_version = self.event.nwm_version, 
                domain=self.event.domain, 
                reach_set=self.reach_set
            )
            self.get_flow()

        # Initialize other attributes

        self.get_unit_labels() 
        
        self.static_ts_opts = dict(
            default_tools=[], 
            toolbar='above',
            show_title = True,                
            xrotation=90, 
            xlabel='DateTime', 
            shared_axes=False,
            height=300,            
            border=5
        )

        # for dashboards only including precip
        if self.explore_precip:
            self.get_date_lists(self.precip_metrics_gdf)
            self.get_precip_colorbar_lims()        
            self.get_precip_colormaps()                               
            self.get_precip_timeseries_plot_opts()
            self.get_cumulative = True
            self.get_location_forecasts = True
            self.count_precip_queries = 0

        # for dashboards only including streamflow
        if self.explore_streamflow:
            self.get_date_lists(self.flow_metrics_gdf)
            self.get_flow_colorbar_lims()
            self.get_flow_colormaps()
            self.point_id = self.flow_points_max['primary_location_id'].iloc[0]
            self.point_name = self.flow_points_max['name'].iloc[0]
            self.get_flow_timeseries_plot_opts()
            self.get_location_forecasts = True
            self.get_cumulative = True
            self.count_flow_queries = 0

        # for dashboards including both
        if self.explore_precip and self.explore_streamflow:

            # get point closest to map polygon max 
            max_polygon = self.precip_poly_max['geometry'].iloc[0]

            gdf = self.flow_metrics_gdf[['primary_location_id','geometry','name']]
            if self.ts_polygons == 'usgs_basins': 
                # not all usgs basins have boundaries defined (yet)
                points_with_basins_gdf = gdf[
                    gdf['primary_location_id'].isin(self.ts_polys_gdf['id'])
                ]
            else:
                # all points have a basin if huc10s
                points_with_basins_gdf = gdf
                
            # get the point closest to max_polygon 
            min_point = min(
                points_with_basins_gdf['geometry'], 
                key=max_polygon.distance
            )
            selected_point = points_with_basins_gdf[
                points_with_basins_gdf['geometry'] == min_point]
            self.point_id = selected_point['primary_location_id'].iloc[0]
            self.point_name = selected_point['name'].iloc[0]

            # set initial MAP polygon
            if self.ts_polygons == 'usgs_basins':
                self.ts_poly_id = self.point_id
                if not self.ts_poly_id in self.ts_polys_gdf['id'].to_list():
                    self.ts_poly_id = None
            else:
                point = gdf[
                    gdf['primary_location_id']==self.point_id
                    ]['geometry'].iloc[0]
                nearest_ts_poly = self.ts_polys_gdf[
                    self.ts_polys_gdf['geometry'].contains(point)
                    ]
                self.ts_poly_id = nearest_ts_poly['id'].iloc[0]

        self.ts_cmap = []


    
    ######################## precip methods

    def get_precip(self):
        self.get_precip_obs_summary()
        self.get_precip_metrics()
        self.get_precip_ts_poly()

    def get_precip_obs_summary(self):
        '''
    
        '''
        
        # get obs precip for the full analysis period 
        # (1st timestep of 1st forecast to last timestep of last forecast)
        self.precip_obs_gdf = data.teehr_get_obs_precip_total(
            self.paths, 
            self.event, 
            self.dates, 
            polygons = self.map_polygons, 
        )      
        self.precip_obs_max = self.precip_obs_gdf['sum'].max()

    def get_precip_metrics(self):
        '''
    
        '''

        # get precip totals and difference
        self.precip_metrics_gdf = data.teehr_get_precip_metrics(
            self.paths, 
            self.event, 
            self.dates, 
            polygons = self.map_polygons, 
        )

        # subset of polygons included in the precip metrics query
        self.map_polys_gdf = self.precip_metrics_gdf[
            ['primary_location_id','geometry']
        ].groupby('primary_location_id').first().reset_index()

        # assign crs to subset
        self.map_polys_gdf.crs = self.precip_metrics_gdf.crs
        
        # get the precip metrics row for the huc10 with max obs sum 
        # (will use multiple columns from this)       
        self.precip_poly_max = self.precip_metrics_gdf.loc[
            self.precip_metrics_gdf['primary_sum'] == \
            self.precip_metrics_gdf['primary_sum'].max()
        ]
        self.precip_hourly_max = self.precip_metrics_gdf[
            ['primary_maximum','secondary_maximum']
        ].max().max()
        

    def get_precip_ts_poly(self):
    
        # assign the polygons and initial selection for MAP 
        # time series plots

        # if the polygons to use in the map and time series plots
        # are the same:
        if self.map_polygons == self.ts_polygons: 
            self.ts_polys_gdf = self.map_polys_gdf
            self.ts_poly_id = self.precip_poly_max[
                'primary_location_id'
            ].iloc[0]
            self.ts_polys_gdf = self.ts_polys_gdf.rename(
                columns = {'primary_location_id':'id'}
            )
        # otherwise the time series plots use usgs_basins
        elif self.ts_polygons == 'usgs_basins':
            self.ts_polys_gdf = self.geo.usgs_basins[
                self.geo.usgs_basins['id'].isin(
                    self.event.usgs_id_list_with_prefix
                )
            ]
            self.ts_poly_id = self.ts_polys_gdf['id'].iloc[0]      

    def get_usgs_basin_precip(self):

        # if map_polygons are not usgs basins, get usgs_basin precip
        # to enable ROC calcs
        if self.map_polygons != 'usgs_basins':
            self.precip_usgs_gdf = data.teehr_get_obs_precip_total(
                self.paths, 
                self.event, 
                self.dates, 
                polygons='usgs_basins', 
            )     
        elif get_runoff_coeff and self.map_polygons == 'usgs_basins':
            self.precip_usgs_gdf = self.precip_obs_gdf    
    
    def get_precip_colorbar_lims(self):
        '''
    
        '''
        self.precip_clims = {}
        value_max = max(
            self.precip_metrics_gdf['primary_sum'].max(), 
            self.precip_metrics_gdf['secondary_sum'].max()
        )
        if value_max > 5:
            self.precip_clims['value']=(0, value_max)        
        else:
            self.precip_clims['value']=(0, 5)   
        
        diff_max = max(-1*self.precip_metrics_gdf['sum_diff'].min(), 
                       self.precip_metrics_gdf['sum_diff'].max())

        self.precip_clims['sum_diff']=(-diff_max, diff_max)
           
    def get_precip_colormaps(self):
        ''' 
        build custom precip colormaps
        '''
        #precip values somewhat following WPC standard
        
        cmap1 = cc.CET_L6[85:]
        cmap2 = [cmap1[i] for i in range(0, len(cmap1), 3)]
        ext = [cmap2[-1] + aa for aa in ['00','10','30','60','99']]
        self.precip_cmap = ext + cmap2[::-1] + cc.CET_R1 
        
        self.get_precip_difference_colormap()

    def get_precip_difference_colormap(self):
        '''
    
        '''
        #precip difference, white at 0
        cmap_base = cc.CET_D1A[::-1]
        l = len(cmap_base)
        half = int(l/2)
        cmap1 = [cmap_base[i] for i in range(0,half-2)]
        cmap2 = [cmap_base[i] for i in range(half+2,l)]
        white = [
             '#efebf5',
             '#f9f7fb',
             '#ffffff',
             '#fcf7f6',
             '#f8eae8']
     
        self.precip_diff_cmap = cmap1 + white + cmap2        
    
    def get_precip_timeseries_by_poly(self):
        '''
    
        '''
        # query primary (observed) precip timeseries, 
        precip_obs_ts = data.teehr_get_obs_precip_timeseries(
            self.ts_poly_id, 
            self.paths, 
            self.dates, 
            polygons = self.ts_polygons
        )

        ## error if max usgs gage location does not have a basin defined

        # remove duplicates due to overlapping timesteps in the AnA, 
        # keep most recent
        precip_obs_ts = precip_obs_ts.sort_values(
            ['value_time','reference_time']
        )
        precip_obs_ts = precip_obs_ts.groupby('value_time').last().reset_index()

        # fill missing timesteps with zero for cumulative calcs 
        # fill with -0.1 to indicate missing and prevent time gaps on x-axis
        # REVISIT THIS DECISION
        self.precip_obs_ts = self.data_value_time_list_df.merge(
            precip_obs_ts[['value_time','value','location_id']], 
            how='left', 
            left_on='value_time', 
            right_on='value_time'
        )
        self.precip_obs_ts['value_fill_missing'] = \
            self.precip_obs_ts['value'].fillna(-0.1)
        self.precip_obs_ts['value_fill_zero'] = \
            self.precip_obs_ts['value'].fillna(0)
        if self.get_cumulative:
            self.precip_obs_ts['cumulative'] = \
                self.precip_obs_ts['value_fill_zero'].cumsum()
            self.precip_cumulative_max = max(
                self.precip_clims['value'][1], 
                self.precip_obs_ts['cumulative'].max()
            )*1.05

        # query forecasts 
        # !! currently not yet dealing with missing forecast data !!
        if self.get_location_forecasts:
            self.precip_all_fcst_ts = data.teehr_get_fcst_precip_timeseries(
                self.ts_poly_id, 
                self.paths, 
                self.dates, 
                polygons = self.ts_polygons
            )
            if self.get_cumulative:
                self.precip_all_fcst_ts['cumulative'] = 0.0
                for ref_time in self.ref_time_list:                   
                    fcst_df = self.precip_all_fcst_ts[
                        self.precip_all_fcst_ts['reference_time'] == ref_time
                    ].copy()                    
                    fcst_df['cumulative'] = fcst_df['value'].cumsum()                
                    obs_t0 = self.precip_obs_ts.loc[
                        self.precip_obs_ts['value_time']==ref_time, 'cumulative'
                    ].iloc[0]            
                    fcst_df['cumulative_from_t0'] = fcst_df['cumulative'] + obs_t0          
                    self.precip_all_fcst_ts.loc[
                        fcst_df.index, 
                        'cumulative'
                    ] = fcst_df['cumulative']        
                    self.precip_all_fcst_ts.loc[
                        fcst_df.index, 
                        'cumulative_from_t0'
                    ] = fcst_df['cumulative_from_t0']
                    
                # add t0 values so forecast plots connect to obs
                t0_rows = self.precip_obs_ts[
                    ['value_time','cumulative']
                    ][self.precip_obs_ts['value_time'].isin(
                        self.ref_time_list
                    )].copy()
                t0_rows['reference_time'] = t0_rows['value_time']
                t0_rows = t0_rows.rename(
                    columns = {'cumulative' : 'cumulative_from_t0'}
                )      
                self.precip_all_fcst_with_t0 = pd.concat(
                    [self.precip_all_fcst_ts, t0_rows], 
                    axis = 0
                ).sort_values(['reference_time','value_time'])
                
                precip_cumulative_max = max(
                    self.precip_obs_ts['cumulative'].max(), 
                    self.precip_all_fcst_with_t0['cumulative_from_t0'].max()
                )
                self.precip_cumulative_max = max(
                    self.precip_clims['value'][1], 
                    precip_cumulative_max
                )*1.05
    
    def update_precip_timeseries_for_selected_location(self):
        '''
        Get obs and forecast time series for polygon nearest to selected 
        x, y point OR upstream basin for selected Point (gage)
        '''
        # first find the ts_poly_id from stream selection
        
        # if first pass, no selections, ts_poly_id already 
        # set in initialization
        if self.coord_stream.x is np.nan and \
           self.point_stream.index == [np.nan]:
            pass

        # if coord_stream source is defined and xy point is selected, 
        # find the nearest map polygon
        if self.coord_stream.x is not np.nan and \
           self.point_stream.index == [np.nan]:
            point = Point(self.coord_stream.x, self.coord_stream.y)
            nearest_poly = self.map_polys_gdf[
                self.map_polys_gdf['geometry'].contains(point)
            ]    
            # if empty, xy point is outside the region
            if nearest_poly.empty:
                self.ts_poly_id = None
            else:
                self.ts_poly_id = nearest_poly['primary_location_id'].iloc[0]

        # if point_stream exists, but is empty, selection is outside region
        elif self.coord_stream.x is np.nan and self.point_stream.index == []:
            self.ts_poly_id = None

        # if point_stream not empty, nor nan, get the point
        elif self.coord_stream.x is np.nan and \
            self.point_stream.index != [] and  \
            self.point_stream.index != [np.nan]:

            if type(self.point_stream.source) == hv.DynamicMap:
                gdf_stream = self.point_stream.source.data[()].data
            else:
                gdf_stream = self.point_stream.source.data
    
            selected_point = gdf_stream.iloc[self.point_stream.index[0]]
            point = selected_point['geometry']
            self.point_name = selected_point['name']
            if 'primary_location_id' in gdf_stream.columns:   
                self.point_id = selected_point['primary_location_id']
            else:
                self.point_id = selected_point['location_id']
            
            if self.ts_polygons == 'usgs_basins':
                self.ts_poly_id = self.point_id
                if not self.ts_poly_id in self.ts_polys_gdf['id'].to_list():
                    self.ts_poly_id = None
            else:
                nearest_poly = self.ts_polys_gdf[
                    self.ts_polys_gdf['geometry'].contains(point)
                ]
                self.ts_poly_id = nearest_poly['id'].iloc[0]

        # if a polygon exists for the selected locations
        # check if data do not exist or not already obtained for this location, 
        # in either of those cases, get the data
        if self.ts_poly_id:
            try:
                if self.precip_obs_ts.loc[0,'location_id'] != self.ts_poly_id:
                    self.count_precip_queries += 1
                    self.get_precip_timeseries_by_poly()
            except:
                self.count_precip_queries += 1
                self.get_precip_timeseries_by_poly()

    def get_precip_obs_polygons_total(self):
        '''
    
        '''
        obs_gv = gv.Polygons(self.precip_obs_gdf, vdims=['sum'])
        obs_raster = rasterize(
            obs_gv, 
            aggregator=ds.mean('sum'), 
            precompute=True
        )
        obs_raster.opts(
            cmap=self.precip_cmap,
        )
        return obs_raster
    
    def get_precip_ave_difference(self):
        '''
    
        '''
        gdf = self.precip_metrics_gdf
        ave_df = gdf[
            ['primary_location_id','sum_diff','geometry']
            ].groupby('primary_location_id').agg(
            {'sum_diff':'mean', 'geometry':'first'}
            ).reset_index()
        
        ave_gdf = gpd.GeoDataFrame(ave_df)
        ave_gv = gv.Polygons(
            ave_gdf, 
            vdims=['sum_diff']
        )
        diff_raster = rasterize(
            ave_gv, 
            aggregator=ds.mean('sum_diff'), 
            precompute=True
        )
        diff_raster.opts(
            cmap=self.precip_diff_cmap,
        )           
        return diff_raster

    def get_precip_polygon_centroids(self):
        '''
    
        '''
        gdf = self.ts_polys_gdf.copy()
        gdf['centroid'] = gdf['geometry'].to_crs(3857).centroid.to_crs(4326)
        gdf.drop('geometry', axis=1, inplace=True)
        gdf.rename(columns = {'centroid':'geometry'}, inplace=True)
        gdf = gdf.merge(
            self.precip_obs_gdf[['location_id','sum']], 
            how='left', 
            left_on = 'id', 
            right_on = 'location_id'
        )
        
        return gv.Points(gdf, vdims=['id','sum'])
            
    def get_precip_timeseries_plot_opts(self):
        '''
    
        '''
        tick_labels = self.get_xtick_date_labels()
        xlim=(
            self.analysis_value_time_list_df['value_time'].min(), 
            self.analysis_value_time_list_df['value_time'].max()
        )
        
        self.precip_hourly_ts_opts = dict(
            self.static_ts_opts,
            width=600,
            ylim=(
                -self.precip_hourly_max*0.05, 
                self.precip_hourly_max*1.05
            ),
            xlim=xlim,
            xticks=tick_labels,
            ylabel='MAP Hourly ' + self.precip_unit_label,
            framewise=True,
        )
        self.precip_cumul_ts_opts = dict(
            self.static_ts_opts, 
            width=600,
            xlim=xlim,
            xticks=tick_labels, 
            ylabel='MAP Cumul. ' + self.precip_unit_label,
            #yaxis='right',
            framewise=True,
        ) 

    def get_empty_bars(self, opts):
        '''
    
        '''
        bars = hv.Rectangles((0,0,0,0)).opts(
            **opts,
            tools=["pan","box_zoom","reset","hover"],
            title='NO DATA (Polygon unavailable or outside region)',
            color='white', 
            line_color = None,
        )
        return bars

    def get_empty_curve(self, opts):
        '''
    
        '''
        curve = hv.Curve((self.ref_time_list[0], 0)).opts(
            **opts,     
            tools=["pan","box_zoom","reset","hover"],
            title='NO DATA (Polygon unavailable or outside region)',
            line_color = 'white',
        )
        return curve
            
    @param.depends("ref_time_str")
    def get_precip_obs_polygons_reftime(self):
        '''
    
        '''        
        ref_time = dt.datetime.strptime(self.ref_time_str, '%Y-%m-%d %Hz')
        precip_sums_ref = self.precip_metrics_gdf[
            self.precip_metrics_gdf['reference_time'] == ref_time
        ]
        return gv.Polygons(precip_sums_ref, vdims=['primary_sum',])
    
    @param.depends("ref_time_str")
    def get_precip_fcst_polygons_reftime(self):
        '''
    
        '''
        ref_time = dt.datetime.strptime(self.ref_time_str, '%Y-%m-%d %Hz')
        precip_sums_ref = self.precip_metrics_gdf[
            self.precip_metrics_gdf['reference_time'] == ref_time
        ]                                            
        return gv.Polygons(precip_sums_ref, vdims=['secondary_sum'])
                                            
    @param.depends("ref_time_str")
    def get_precip_diff_polygons_reftime(self):
        '''
    
        '''
        ref_time = dt.datetime.strptime(self.ref_time_str, '%Y-%m-%d %Hz')
        precip_sums_ref = self.precip_metrics_gdf[
            self.precip_metrics_gdf['reference_time'] == ref_time
        ]                                            
        return gv.Polygons(precip_sums_ref, vdims=['sum_diff']) 
    
    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index")
    def get_precip_obs_timeseries_hourly_bars(self):
        '''
    
        '''
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None:      
            bars = self.get_empty_bars(self.precip_hourly_ts_opts)
        else:      
            d50 = dt.timedelta(minutes=50)
            d20 = dt.timedelta(minutes=20)
            rectangle_data = (
                self.precip_obs_ts['value_time'] - d50, 
                0, 
                self.precip_obs_ts['value_time'] - d20, 
                self.precip_obs_ts['value_fill_missing']
            ) 
            hover = HoverTool(
                tooltips=[('Analysis', '@y1'),
                          ('DateTime', '@x1{%m-%d %Hz}')],
                formatters={'@x1' : 'datetime'}
            )
            bars = hv.Rectangles(rectangle_data, label='p').opts(
                **self.precip_hourly_ts_opts,
                tools=["pan","box_zoom","reset", hover],
                title = f"MAP: {self.ts_poly_id}",
                color='blue', 
                line_color = None, 
                alpha=1,
            )        
        return bars
    
    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index", "ref_time_str")
    def get_precip_fcst_timeseries_hourly_bars(self):
        '''
    
        '''
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None: 
            bars = self.get_empty_bars(self.precip_hourly_ts_opts)    
        else:
            ref_time = dt.datetime.strptime(self.ref_time_str, '%Y-%m-%d %Hz')
            df = self.precip_all_fcst_ts[
                self.precip_all_fcst_ts['reference_time'] == ref_time
            ]
            d40 = dt.timedelta(minutes=40)
            d10 = dt.timedelta(minutes=10)
            rectangle_data = (
                df['value_time'] - d40, 
                0, 
                df['value_time'] - d10, 
                df['value']
            )
            hover = HoverTool(tooltips=[('Forecast', '@y1'),
                                        ('DateTime', '@x1{%m-%d %Hz}')],
                              formatters={'@x1' : 'datetime'})
            bars = hv.Rectangles(rectangle_data, label='p').opts(
                **self.precip_hourly_ts_opts,
                tools=["pan","box_zoom","reset", hover],
                title = f"MAP: {self.ts_poly_id}",
                color='fuchsia', 
                line_color = None, 
                alpha= 1,
            )
        return bars    

    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index")
    def get_precip_obs_timeseries_hourly_curve(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None:    
            curve = self.get_empty_curve(self.precip_hourly_ts_opts)
        else:      
            df = self.precip_obs_ts
            hover = HoverTool(
                tooltips=[('Analysis', '@value'),
                          ('DateTime', '@value_time{%m-%d %Hz}')],
                formatters={'@value_time' : 'datetime'}
            )
            curve = hv.Curve(df, "value_time", "value", label='p').opts(
                **self.precip_hourly_ts_opts,
                tools=["pan","box_zoom","reset",hover],
                title = f"MAP: {self.ts_poly_id}",
                line_color = 'blue', 
                line_width = 2,
            )        
        return curve

    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index")
    def get_precip_fcst_timeseries_hourly_all_curve(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None:      
            all_curves = [self.get_empty_curve(self.precip_hourly_ts_opts)]
        else:
            all_curves = []

            if 'ts_cmap' in dir(self):
                cmap = self.ts_cmap
            else:
                cmap = ['gray']
            cint = len(cmap) / len(self.ref_time_list)
            
            df = self.precip_all_fcst_ts
            for i, ref_time in enumerate(self.ref_time_list):
                ci=round(i*cint)
                if ci > len(cmap) - 1:
                    ci = len(cmap) - 1
        
                df = self.precip_all_fcst_ts[
                    self.precip_all_fcst_ts['reference_time'] == ref_time
                ]
                hover = HoverTool(
                    tooltips=[('RefTime', '@reference_time{%m-%d %Hz}')],
                    formatters={'@reference_time' : 'datetime'}
                )            
                curve = hv.Curve(
                    df, 
                    kdims=["value_time"], 
                    vdims=["value","reference_time"], 
                    label='p'
                ).opts(
                    **self.precip_hourly_ts_opts,
                    tools=["pan","box_zoom","reset", hover],
                    title = f"MAP: {self.ts_poly_id}",
                    line_color=cmap[ci], 
                    alpha=1,
                    line_width = 1,
                    # line_color = 'gray'
                    # alpha= 0.3,
                )
                all_curves.append(curve)
                
        return hv.Overlay(all_curves)

    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index", "ref_time_str")
    def get_precip_fcst_timeseries_hourly_curve(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None:      
            curve = self.get_empty_curve(self.precip_hourly_ts_opts)
        else:
            ref_time = dt.datetime.strptime(self.ref_time_str, '%Y-%m-%d %Hz')
            df = self.precip_all_fcst_ts[
                self.precip_all_fcst_ts['reference_time'] == ref_time
                ][['value_time','value']]
            
            hover = HoverTool(tooltips=[('Forecast', '@value'),
                                        ('DateTime', '@value_time{%m-%d %Hz}')],
                              formatters={'@value_time' : 'datetime'})            
            curve = hv.Curve(df, "value_time", "value", label='p').opts(
                **self.precip_hourly_ts_opts,
                tools=["pan","box_zoom","reset",hover],
                title = f"Basin: {self.ts_poly_id}",
                line_color = 'fuchsia', 
                line_width = 2,
                alpha= 1,
            )       
        return curve
    
    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index", "ref_time_str")
    def get_precip_fcst_timeseries_hourly_window(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None: 
            bars = self.get_empty_bars(self.precip_hourly_ts_opts)      
        else:
            ref_time = dt.datetime.strptime(
                self.ref_time_str, 
                '%Y-%m-%d %Hz'
            )
            rectangle_data = (
                ref_time, 
                0, 
                ref_time + self.dates.forecast_duration, 
                5
            )
            bars = hv.Rectangles(rectangle_data).opts(
                **self.precip_hourly_ts_opts,
                tools=["pan","box_zoom","reset"],
                title = f"MAP: {self.ts_poly_id}",
                color='gray', 
                line_color = 'gray', 
                alpha= 0.3,
            )
        return bars    
    
    @param.depends("coord_stream.x", "coord_stream.y", "point_stream.index")
    def get_precip_obs_timeseries_cumulative(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None:    
            curve = self.get_empty_curve(self.precip_hourly_ts_opts)
        else:      
            df = self.precip_obs_ts
            hover = HoverTool(
                tooltips=[('Analysis (Cumul.)', '@cumulative'),
                          ('DateTime', '@value_time{%m-%d %Hz}')],
                formatters={'@value_time' : 'datetime'})
            curve = hv.Curve(
                df, 
                "value_time", 
                "cumulative"
            ).opts(
                **self.precip_cumul_ts_opts,
                tools=["pan","box_zoom","reset",hover],
                title = f"MAP: {self.ts_poly_id}",
                line_color = 'blue', 
                line_width = 2,
                ylim=(-0.1, self.precip_cumulative_max*1.01),
            )        
        return curve
 
    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index")
    def get_precip_fcst_timeseries_cumulative_all(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None:      
            all_curves = [self.get_empty_curve(self.precip_hourly_ts_opts)]
        else:
            all_curves = []

            if 'ts_cmap' in dir(self):
                cmap = self.ts_cmap
            else:
                cmap = ['gray']
            cint = len(cmap) / len(self.ref_time_list)
            
            df = self.precip_all_fcst_with_t0
            for i, ref_time in enumerate(self.ref_time_list):
                ci=round(i*cint)
                if ci > len(cmap) - 1:
                    ci = len(cmap) - 1
                
                df = self.precip_all_fcst_with_t0[
                     self.precip_all_fcst_with_t0['reference_time'] == ref_time
                ]
                hover = HoverTool(
                    tooltips=[('RefTime', '@reference_time{%m-%d %Hz}')],
                    formatters={'@reference_time' : 'datetime'}
                )            
                curve = hv.Curve(
                    df, 
                    kdims=["value_time"], 
                    vdims=["cumulative_from_t0","reference_time"]
                ).opts(
                    **self.precip_cumul_ts_opts,
                    tools=["pan","box_zoom","reset",hover],
                    title = f"MAP: {self.ts_poly_id}",
                    line_color=cmap[ci], 
                    alpha=1,                 
                    line_width = 1,
                    ylim=(-0.1, self.precip_cumulative_max*1.01),
                )
                all_curves.append(curve)
                
        return hv.Overlay(all_curves)

    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index", "ref_time_str")
    def get_precip_fcst_timeseries_cumulative(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None:      
            curve = self.get_empty_curve(self.precip_hourly_ts_opts)
        else:
            ref_time = dt.datetime.strptime(
                self.ref_time_str, 
                '%Y-%m-%d %Hz'
            )
            df = self.precip_all_fcst_with_t0[
                self.precip_all_fcst_with_t0['reference_time'] == ref_time
            ]
            df = df[['value_time','cumulative_from_t0']]
            
            hover = HoverTool(
                tooltips=[('Forecast', '@cumulative_from_t0'),
                          ('DateTime', '@value_time{%m-%d %Hz}')],
                formatters={'@value_time' : 'datetime'}
            )            
            curve = hv.Curve(
                df, 
                "value_time", 
                "cumulative_from_t0"
            ).opts(
                **self.precip_cumul_ts_opts,
                tools=["pan","box_zoom","reset",hover],
                title = f"MAP: {self.ts_poly_id}",
                line_color = 'fuchsia', 
                line_width = 2,
                alpha= 1,
                ylim=(-0.1, self.precip_cumulative_max*1.01),
            )       
        return curve
    
    @param.depends("coord_stream.x", "coord_stream.y", 
                   "point_stream.index", "ref_time_str")
    def get_precip_fcst_timeseries_cumulative_window(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_precip_timeseries_for_selected_location()
        
        if self.ts_poly_id is None: 
            bars = self.get_empty_bars(self.precip_hourly_ts_opts)        
        else:
            ref_time = dt.datetime.strptime(
                self.ref_time_str, 
                '%Y-%m-%d %Hz'
            )
            rectangle_data = (
                ref_time, 
                0, 
                ref_time + self.dates.forecast_duration, 
                30)
            bars = hv.Rectangles(rectangle_data, label='p').opts(
                **self.precip_cumul_ts_opts,
                tools=["pan","box_zoom","reset"],
                title = f"MAP: {self.ts_poly_id}",
                color='gray', 
                line_color = 'gray', 
                alpha= 0.3,
            )
        return bars   

    ##### streamflow methods

    def get_flow(self):
        '''
    
        '''
        self.get_flow_obs_summary()
        self.get_flow_metrics()

    def get_flow_obs_summary(self):
        '''
    
        '''
        ## observed flow signatures (not joined with forecasts)
        
        # get observed flow signatures for the full analysis period 
        # (1st timestep of 1st forecast to last timestep of last forecast)
        flow_obs_gdf = data.teehr_get_obs_flow_chars(
            self.paths, 
            self.event, 
            self.dates, 
        )

        # add attributes
        flow_obs_gdf = flow_obs_gdf.merge(
            self.geo.usgs_points_subset[['id','name'] + self.geo.attribute_list], 
            how='left', 
            left_on='location_id', 
            right_on='id')
        
        flow_obs_gdf = flow_obs_gdf.drop(columns=['id'])

        # add metrics (not yet avail in teehr)
        flow_obs_gdf = data.add_normalized_peakflow(
            flow_obs_gdf, 
            self.paths
        )
        flow_obs_gdf= data.add_normalized_volume(
            flow_obs_gdf, 
            self.paths
        )
        self.flow_obs_gdf = flow_obs_gdf

        # initialize location max values (will be updated)
        self.flow_location_max = flow_obs_gdf['max'].max()
        self.flow_location_max_cum = flow_obs_gdf['sum_norm'].max()


    def get_flow_metrics(self):
        '''
    
        '''
        ## get flow metrics (joining forecasts to obs)

        flow_metrics_gdf = data.teehr_get_flow_metrics(
            self.paths, 
            self.event, 
            self.dates, 
        )
        # # pull off locations/forecasts with nan for QA, usually reservoir features
        # self.nan_gdf = flow_metrics_gdf[flow_metrics_gdf.isnull().any(axis=1)]

        # drop NaN locations, cause errors in dashboards
        # flow_metrics_gdf.dropna(inplace=True)

        # add attributes
        flow_metrics_gdf = flow_metrics_gdf.merge(
            self.geo.usgs_points_subset[['id','name'] + self.geo.attribute_list], 
            how='left', 
            left_on='primary_location_id', 
            right_on='id'
        )
        flow_metrics_gdf = flow_metrics_gdf.drop(columns=['id'])

        # pull off locations/forecasts with nan for QA, usually reservoir features
        self.nan_gdf = flow_metrics_gdf[flow_metrics_gdf.isnull().any(axis=1)]

        # drop NaN locations, cause errors in dashboards
        flow_metrics_gdf.dropna(inplace=True)
        
        # add metrics (not yet avail in teehr)
        flow_metrics_gdf = data.add_percent_difference(flow_metrics_gdf)
        flow_metrics_gdf = data.add_flow_exceedence(flow_metrics_gdf)   
        flow_metrics_gdf = data.add_prior_signal_time(flow_metrics_gdf)
        flow_metrics_gdf = data.add_normalized_peakflow(
            flow_metrics_gdf, 
            self.paths
        )
        flow_metrics_gdf = data.add_normalized_volume(
            flow_metrics_gdf, 
            self.paths
        )      
        
        # get the set of unique points returned by the flow metrics query 
        # (leaves out any points with no data)
        flow_points_gdf = flow_metrics_gdf[
            ['primary_location_id','geometry','name','drainage_area','hw_threshold']
        ].groupby('primary_location_id').first().reset_index()   

        # add the average metric per point (across reference times) 
        # (could do a groupby reference_time in teehr instead)
        df = flow_metrics_gdf[[
            'primary_location_id',
            'max_value_delta', 
            'max_norm_diff',        
            'peak_percent_diff',
            'peak_time_diff_hours',
            'vol_percent_diff',
            'vol_norm_diff'
        ]].copy()
        df_mean = df.groupby('primary_location_id').mean().reset_index()
        rename_columns = ['mean_' + c for c in df_mean.columns 
                          if c not in ['primary_location_id']]

        df_mean = df.groupby('primary_location_id').mean().reset_index()
        rename_columns = ['primary_location_id'] \
            + ['mean_' + c for c in df_mean.columns 
               if c not in ['primary_location_id']]
        df_mean.columns = rename_columns
        flow_points_gdf = flow_points_gdf.merge(
            df_mean, 
            how='left', 
            on='primary_location_id'
        )

        # add boolean that is True if threshold is exceeded in any forecast
        df_exceed = flow_metrics_gdf[
            ['primary_location_id','obs_exceed','fcst_exceed']
        ].groupby('primary_location_id').any().reset_index() 
        df_exceed.rename(
            columns = {
                'obs_exceed' : 'any_obs_exceed',
                'fcst_exceed' : 'any_fcst_exceed'
            },
            inplace=True
        )
        flow_points_gdf = flow_points_gdf.merge(
            df_exceed, 
            how='left', 
            on='primary_location_id'
        )
        # add max prior_hw_signal_time
        df_max_signal_time = flow_metrics_gdf[
            ['primary_location_id','prior_hw_signal_time']
        ].groupby('primary_location_id').max().reset_index() 
        df_max_signal_time.rename(
            columns = {
                'prior_hw_signal_time' : 'max_hw_signal_time',
            },
            inplace=True
        )
        flow_points_gdf = flow_points_gdf.merge(
            df_max_signal_time, 
            how='left', 
            on='primary_location_id'
        )

        # add count of True prior_hw_signal forecasts
        df_hw_signal_count = flow_metrics_gdf[
            ['primary_location_id','prior_hw_signal']
        ].groupby('primary_location_id').sum().reset_index() 
        df_hw_signal_count.rename(
            columns = {
                'prior_hw_signal' : 'prior_hw_signal_count',
            },
            inplace=True
        )
        flow_points_gdf = flow_points_gdf.merge(
            df_hw_signal_count, 
            how='left', 
            on='primary_location_id'
        )        

        # add normalized hw_threshold
        flow_points_gdf = data.calc_normalized_flow(
            flow_points_gdf, 
            'hw_threshold', 
            self.paths.units, 
            flow_points_gdf ['drainage_area']
        )

        # add the contingency table counts
        df_table = pd.crosstab(
            flow_metrics_gdf['primary_location_id'], 
            flow_metrics_gdf['contingency_matrix']
        )
        df_table = df_table.rename(
            columns = {0:'true_negative', 
                       1:'true_positive', 
                       2:'false_negative', 
                       3:'false_positive'}
        )
        flow_points_gdf = flow_points_gdf.merge(
            df_table, 
            how='left', 
            on='primary_location_id'
        )
        
        # pull off locations with zero drainage area and/or zero threshold for QA
        self.zero_gdf = flow_points_gdf[
            (flow_points_gdf['drainage_area'] <= 1) | 
            (flow_points_gdf['hw_threshold'] <= 1)
        ]
        
        self.flow_metrics_gdf = flow_metrics_gdf
        self.flow_points_gdf = flow_points_gdf

        # get max peak
        self.flow_points_max = flow_metrics_gdf.loc[
            flow_metrics_gdf['primary_maximum_norm'] 
                == flow_metrics_gdf['primary_maximum_norm'].max()
        ]
        self.all_peaks_max = flow_metrics_gdf[
            ['primary_maximum','secondary_maximum']].max().max()
        
        self.all_peaks_max_norm = flow_metrics_gdf[
            ['primary_maximum_norm','secondary_maximum_norm']].max().max()



    def get_flow_colorbar_lims(self):
        '''
    
        '''
        self.flow_clims = {}
        self.flow_clims['peak_norm']=(0, 0.1)
        self.flow_clims['vol_norm']=(0, 1)
        self.flow_clims['peak_norm_diff']=(-0.1, 0.1)
        self.flow_clims['vol_norm_diff']=(-1, 1)
        self.flow_clims['percdiff']=(-100, 100)
           
    def get_flow_colormaps(self):
        '''
        
        '''        
        self.peakflow_cmap = cmap=cc.CET_L16[::-1][25:220]
        cmap1 = cc.CET_L6[85:]
        cmap2 = [cmap1[i] for i in range(0, len(cmap1), 3)]
        ext = [cmap2[-1] + aa for aa in ['00','10','30','60','99']]
        self.vol_cmap = ext + cmap2[::-1] + cc.CET_R1 
        
        #difference colormap, white at 0
        cmap_base = cc.CET_D1A[::-1]
        l = len(cmap_base)
        half = int(l/2)
        cmap1 = [cmap_base[i] for i in range(0,half-2)]
        cmap2 = [cmap_base[i] for i in range(half+2,l)]
        white = [
             '#efebf5',
             '#f9f7fb',
             '#ffffff',
             '#fcf7f6',
             '#f8eae8']
        self.flow_diff_cmap = cmap1 + white + cmap2 

        #contingency colormap
        cmap_base = cc.CET_D9[::-1]
        l = len(cmap_base)
        half = int(l/2)
        cmap_half = [cmap_base[i] for i in range(half,l)]
        self.contingency_cmap = ['#ffffff'] + cmap_half
    
    def get_flow_timeseries_by_point(self):
        '''
    
        '''

        # (currently must query obs and fcst separately to return 
        #      timesteps where obs are missing)
        # query primary (observed) flow timeseries
        flow_obs_ts = data.teehr_get_obs_flow_timeseries(
            self.point_id, 
            self.paths, 
            self.dates
        )
        
        # remove duplicates due to overlapping timesteps in the AnA, 
        # keep most recent
        flow_obs_ts = flow_obs_ts.groupby('value_time').first().reset_index()

        # fill with -1 to indicate missing and prevent time gaps on x-axis - 
        # REVISIT THIS DECISION
        self.flow_obs_ts = self.data_value_time_list_df.merge(
            flow_obs_ts[['value_time','value','location_id']], 
            how='left', 
            left_on='value_time', 
            right_on='value_time'
        )
        self.flow_obs_ts['value_fill_missing'] \
            = self.flow_obs_ts['value'].fillna(-1.0)

        # get hw threshold and upstream area
        location_metrics_gdf = self.flow_metrics_gdf[
            self.flow_metrics_gdf['primary_location_id'] == self.point_id
        ]
        self.hw_threshold = location_metrics_gdf['hw_threshold'].iloc[0]
        self.drainage_area = location_metrics_gdf['drainage_area'].iloc[0]

        # add cumulative normalized volume
        self.flow_obs_ts = data.add_normalized_timeseries(
            self.flow_obs_ts, 
            self.paths, 
            self.drainage_area
        )
        self.flow_obs_ts['cumulative'] \
            = self.flow_obs_ts['value_norm'].cumsum()
        
        # query forecasts 
        # !! currently not yet dealing with missing forecast data !!
        if self.get_location_forecasts:
            nwm_id = utils.locations.get_crosswalked_id_list(
                [self.point_id], 
                self.geo.cross_usgs_nwm
            )
            nwm_id = nwm_id[0]
            self.flow_all_fcst_ts = data.teehr_get_fcst_flow_timeseries(
                nwm_id, 
                self.paths, 
                self.dates
            ) 
            # get no-da
            self.flow_noda_ts = data.teehr_get_noda_flow_timeseries(
                nwm_id, 
                self.paths, 
                self.dates
            ) 
        # ymax value for ts plot
        if self.get_location_forecasts:
            self.flow_location_max = max(
                self.flow_all_fcst_ts['value'].max(), 
                self.flow_obs_ts['value'].max(), 
                self.flow_noda_ts['value'].max(), 
                self.hw_threshold
            )
            self.flow_location_max_cum = self.flow_obs_ts['cumulative'].max()
        else:
            self.flow_location_max = max(
                self.flow_obs_ts['value'].max(), 
                self.hw_threshold*1.1
            )
            self.flow_location_max_cum = self.flow_obs_ts['cumulative'].max()

        self.flow_obs_ts['cumulative_scaled'] \
            = self.flow_obs_ts['cumulative']  \
            * self.flow_location_max          \
            / self.flow_location_max_cum      \

    def update_flow_timeseries_for_selected_point(self):
        '''
        Get obs and forecast flow time series for selected gage or 
        nwm_reach nearest to selected x, y point
        '''
        # first pass, point_id set in initialization
        if self.point_stream.index == [np.nan]:
            pass
            
        # If outside region, set to None.
        elif self.point_stream.index == []:
            self.point_id = None
            self.point_name = None
             
        # Otherwise, query data if new point
        else:
            if type(self.point_stream.source) == hv.DynamicMap:
                gdf_stream = self.point_stream.source.data[()].data
            else:
                gdf_stream = self.point_stream.source.data
            selected_point = gdf_stream.iloc[self.point_stream.index[0]]
            self.point_name = selected_point['name']
            if 'primary_location_id' in gdf_stream.columns:               
                self.point_id = selected_point['primary_location_id']
            else:
                self.point_id = selected_point['location_id']
                

        # if data do not exist or not already obtained for this location, 
        # get the data
        try:
            if self.flow_obs_ts.loc[0,'location_id'] != self.point_id:
                self.count_flow_queries += 1
                self.get_flow_timeseries_by_point()
        except:
            if self.point_id:
                self.count_flow_queries += 1
                self.get_flow_timeseries_by_point()

    def get_summary_points(self, column):
        '''

        '''
        points = gv.Points(
            self.flow_points_gdf, 
            vdims=[column,'primary_location_id','name']
        ).opts(
            color = column,
        )    
        return points
    
    def get_flow_obs_exceed_points(self):
        '''
    
        '''
        gdf_exceed = self.flow_points_gdf[self.flow_points_gdf['any_obs_exceed']]
        points = gv.Points(
            gdf_exceed, 
            vdims=['any_obs_exceed','primary_location_id']
        )       
        return points


    def get_flow_volume_obs(self):
        '''
    
        '''
        points = gv.Points(
            self.flow_obs_gdf, 
            vdims=['sum_norm','location_id','name']
        ).opts(
            color = 'sum_norm',
            cmap=self.vol_cmap,
            clim=self.flow_clims['vol_norm'],  
        )           
        return points

    def get_roc_obs(self):

        precip = self.precip_usgs_gdf[
            ['location_id','sum']
        ].rename(
            columns = {'sum' : 'precip'}
        )
        flow = self.flow_obs_gdf[
            ['location_id','sum_norm','geometry','drainage_area']
        ].rename(
            columns = {'sum_norm' : 'runoff'}
        )
        roc_gdf = flow.merge(precip, on='location_id', how='inner')
        roc_gdf['roc'] = roc_gdf['runoff'] / roc_gdf['precip']
        roc_gdf = roc_gdf.replace(np.inf, 10e6)
        roc_gdf = gpd.GeoDataFrame(roc_gdf).reset_index().drop('index', axis=1)
            
        return gv.Points(
            roc_gdf, 
            vdims=['roc','location_id']
        ).opts(
            color = 'roc',
            cmap=self.vol_cmap,
        )       

    def get_contingency_matrix_count(self, matrix_category='true_positive'):
        '''
    
        '''
        points = gv.Points(
            self.flow_points_gdf, 
            vdims=[
                'true_positive',
                'false_positive',
                'false_negative',
                'true_negative',
                'primary_location_id',
                'name'
            ]
        ).opts(
            color = matrix_category,
            cmap=self.contingency_cmap,
            clim=(0,self.flow_points_gdf[matrix_category].max()),
            title=f"# {matrix_category}",
        )           
        return points

    def get_flow_metrics_ref(self):
        '''
    
        '''
        ref_time = dt.datetime.strptime(
            self.ref_time_str, 
            '%Y-%m-%d %Hz'
        )
        try:
            if self.flow_metrics_ref['reference_time'].iloc[0] != ref_time:
                self.flow_metrics_ref = self.flow_metrics_gdf[
                    self.flow_metrics_gdf['reference_time'] == ref_time
                ]
        except:
            self.flow_metrics_ref = self.flow_metrics_gdf[
                self.flow_metrics_gdf['reference_time'] == ref_time
            ]

    @param.depends("ref_time_str")
    def get_obs_vol_norm_points_reftime(self):
        '''
    
        '''
        column = 'primary_sum_norm'
        self.get_flow_metrics_ref()
        return gv.Points(
            self.flow_metrics_ref, 
            vdims=[column,'primary_location_id','name']
        )
    @param.depends("ref_time_str")
    def get_fcst_vol_norm_points_reftime(self):
        '''
    
        '''
        column = 'secondary_sum_norm'
        self.get_flow_metrics_ref()
        return gv.Points(
            self.flow_metrics_ref, 
            vdims=[column,'primary_location_id','name']
        )
    @param.depends("ref_time_str")
    def get_vol_norm_diff_points_reftime(self):
        '''
    
        '''
        column = 'vol_norm_diff'
        self.get_flow_metrics_ref()
        return gv.Points(
            self.flow_metrics_ref, 
            vdims=[column,'primary_location_id','name']
        )
    
    
    @param.depends("ref_time_str")
    def get_peakflow_obs_exceed_points_reftime(self):
        '''
    
        '''
        self.get_flow_metrics_ref()
        gdf_exceed = self.flow_metrics_ref[
            self.flow_metrics_ref['obs_exceed']
        ]        
        return gv.Points(
            gdf_exceed, vdims=['obs_exceed']
        )
    

    @param.depends("ref_time_str")
    def get_peakflow_fcst_exceed_points_reftime(self):
        '''
    
        '''
        self.get_flow_metrics_ref()
        gdf_exceed = self.flow_metrics_ref[
            self.flow_metrics_ref['fcst_exceed']
        ]        
        return gv.Points(
            gdf_exceed, 
            vdims=['fcst_exceed']
        )
                                        
    
    def get_flow_timeseries_plot_opts(self):
        '''
    
        '''
        self.flow_hourly_ts_opts = dict(
            self.static_ts_opts,
            xlim=(
                self.analysis_value_time_list_df['value_time'].min(), 
                self.analysis_value_time_list_df['value_time'].max()
            ),
            xticks = self.get_xtick_date_labels(),
            ylabel='Streamflow ' + self.flow_unit_label,
            framewise=True,
            margin=(0,0,0,0)
        )   

    @param.depends("point_stream.index")
    def get_flow_obs_timeseries(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_flow_timeseries_for_selected_point()
        
        if self.point_id is None:
            curve = self.empty_ts_curve.opts(
                title='NO DATA (No point selected)'
            )
        else:      
            df = self.flow_obs_ts
            hover = HoverTool(
                tooltips=[('Observed', '@value'),  
                          ('DateTime', '@value_time{%m-%d %Hz}')],
                formatters={'@value_time' : 'datetime'}
            )
            curve = hv.Curve(
                df, 
                "value_time", 
                "value", 
                label='flow'
            ).opts(
                **self.flow_hourly_ts_opts,
                tools=["pan","box_zoom","reset",hover],
                title = f"Streamflow: {self.point_id} - {self.point_name[:30]}",            
                line_color = 'black',
                line_dash = 'dashed',
                line_width = 2,
                ylim=(
                    0-self.flow_location_max*0.05, 
                    self.flow_location_max*1.05
                )
            )        
        return curve

    @param.depends("point_stream.index")
    def get_flow_obs_timeseries_cumulative_scaled(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_flow_timeseries_for_selected_point()
        
        if self.point_id is None:
            curve = self.empty_ts_curve.opts(
                title='NO DATA (No point selected)'
            )
        else:      
            df = self.flow_obs_ts
            curve = hv.Curve(
                df, 
                "value_time", 
                "cumulative_scaled"
            ).opts(
                **self.flow_hourly_ts_opts,
                tools=["pan","box_zoom","reset"],
                title = f"Streamflow: {self.point_id}",            
                line_color = 'blue',
                line_width = 1,
                ylim=(
                    0-self.flow_location_max*0.05, 
                    self.flow_location_max*1.05
                )
            )        
        return curve

    @param.depends("point_stream.index")
    def get_hw_threshold(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_flow_timeseries_for_selected_point()
        
        if self.point_id is None or self.point_stream.index == []:      
            line = hv.HLine(0).opts(
                **self.flow_hourly_ts_opts,
                tools=["pan","box_zoom","reset"],
                title='NO DATA (Location outside analysis region)', 
                line_color = 'white',
            )  
            text = hv.Text(0,0, " ")
        else:      
            line = hv.HLine(
                self.hw_threshold, 
                label='high water threshold'
            ).opts(
                **self.flow_hourly_ts_opts,
                tools=["pan","box_zoom","reset"],
                title = f"Streamflow: {self.point_id} - {self.point_name[:30]}",            
                line_color = 'dimgray',
                line_dash = 'dotted',
                line_width = 2,
                ylim=(
                    0-self.flow_location_max*0.05, 
                    self.flow_location_max*1.05
                )
            )        
            text = hv.Text(
                self.dates.analysis_time_start,
                #self.flow_obs_ts['value_time'].max(), 
                self.hw_threshold, 
                "  HW threshold "
            ).opts(
                tools=["pan","box_zoom","reset"],
                text_align='left', 
                text_baseline="bottom", 
                text_font_size='11px', 
                text_color = 'dimgray'
            )
        return line * text

    @param.depends("point_stream.index")
    def get_flow_noda_timeseries(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_flow_timeseries_for_selected_point()
        
        if self.point_id is None:
            curve = self.empty_ts_curve.opts(
                title='NO DATA (No point selected)'
            )
        else:      
            df = self.flow_noda_ts
            hover = HoverTool(
                tooltips=[('no-DA Analysis', '@value'),
                          ('DateTime', '@value_time{%m-%d %Hz}')],
                formatters={'@value_time' : 'datetime'}
            )
            curve = hv.Curve(
                df, 
                "value_time", 
                "value",
                label='flow'
            ).opts(
                **self.flow_hourly_ts_opts,
                tools=["pan","box_zoom","reset",hover],
                title = f"Streamflow: {self.point_id} - {self.point_name[:30]}",
                line_color = 'blue',
                line_width = 2,
                ylim=(
                    0-self.flow_location_max*0.05, 
                    self.flow_location_max*1.05
                )
            )        
        return curve
 
    @param.depends("point_stream.index")
    def get_flow_fcst_timeseries_all(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_flow_timeseries_for_selected_point()
        
        if self.point_id is None:
            all_curves = [
                self.empty_ts_curve.opts(
                    title='NO DATA (No point selected)'
                )
            ]
        else:
            all_curves = []

            if 'ts_cmap' in dir(self):
                cmap = self.ts_cmap
            else:
                cmap = ['gray']
            cint = len(cmap) / len(self.ref_time_list)
            
            df = self.flow_all_fcst_ts
            for i, ref_time in enumerate(self.ref_time_list):
                ci=round(i*cint)
                if ci > len(cmap) - 1:
                    ci = len(cmap) - 1
    
                df = self.flow_all_fcst_ts[
                    self.flow_all_fcst_ts['reference_time'] == ref_time
                ]
                hover = HoverTool(
                    tooltips=[('RefTime', '@reference_time{%m-%d %Hz}')],
                    formatters={'@reference_time' : 'datetime'})            
                curve = hv.Curve(
                    df, 
                    kdims=["value_time"], 
                    vdims=["value","reference_time"], 
                    label='flow'
                ).opts(
                    **self.flow_hourly_ts_opts,
                    tools=["pan","box_zoom","reset",hover],
                    title = f"Streamflow: {self.point_id} - {self.point_name[:30]}",
                    line_color=cmap[ci], 
                    alpha=1,
                    #line_color = 'gray', 
                    line_width = 1,
                    #alpha= 0.8,
                    ylim=(
                        0-self.flow_location_max*0.05, 
                        self.flow_location_max*1.05
                    )
                )
                all_curves.append(curve)
                
        return hv.Overlay(all_curves)

    @param.depends("point_stream.index")
    def get_norm_axis(self):
        '''
    
        '''

        # update timeseries if not already
        self.update_flow_timeseries_for_selected_point()

        if self.point_id is None:
            curve = hv.Curve((self.ref_time_list[0], 0))
        else:
            ylim = (
                0-self.flow_location_max*0.05, 
                self.flow_location_max*1.05
            )
            df_norm = pd.DataFrame({
                'value_time': [0, 0],
                'value': [ylim[0], ylim[1]],
                'drainage_area': [self.drainage_area, self.drainage_area]
            })
            df_norm = data.calc_normalized_flow(
                df_norm, 
                'value', 
                self.paths.units, 
                self.drainage_area
            )
            curve = hv.Curve(
                df_norm, 
                "value_time", 
                "value_norm"
            ).opts(
                line_color=None,
                ylim=(df_norm['value_norm'].iloc[0], df_norm['value_norm'].iloc[1]),
                width=50,
                height=300,
                xaxis=None,
                toolbar=None,
                yaxis='right',
                margin=(0,0,0,0),
                ylabel='Normalized ' + self.norm_flow_unit_label,
                shared_axes=False,
                framewise=True
            ) 
        return curve
        

    @param.depends("point_stream.index", "ref_time_str")
    def get_flow_fcst_timeseries(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_flow_timeseries_for_selected_point()
        
        if self.point_id is None:
            curve = self.empty_ts_curve.opts(
                title='NO DATA (No point selected)'
            )
        else:
            ref_time = dt.datetime.strptime(
                self.ref_time_str, 
                '%Y-%m-%d %Hz'
            )
            df = self.flow_all_fcst_ts[
                self.flow_all_fcst_ts['reference_time'] == ref_time
            ]
            df = df[['value_time','value']]
            hover = HoverTool(
                tooltips=[('Forecast', '@value'),
                          ('DateTime', '@value_time{%m-%d %Hz}')],
                formatters={'@value_time' : 'datetime'}
            )            
            curve = hv.Curve(
                df, 
                "value_time", 
                "value", 
                label='flow'
            ).opts(
                **self.flow_hourly_ts_opts,
                tools=["pan","box_zoom","reset",hover],
                title = f"Streamflow: {self.point_id} - {self.point_name[:30]}",
                line_color = 'fuchsia', 
                line_width = 2,
                alpha= 1,
                ylim=(
                    0-self.flow_location_max*0.05, 
                    self.flow_location_max*1.05
                )
            )       
        return curve
    
    @param.depends("point_stream.index", "ref_time_str")
    def get_flow_fcst_timeseries_window(self):
        '''
    
        '''
        
        # update timeseries if not already
        self.update_flow_timeseries_for_selected_point()
        
        if self.point_id is None or self.point_stream.index == []:
            bars = hv.Rectangles((0,0,0,0)).opts(
                **self.flow_hourly_ts_opts,
                tools=["pan","box_zoom","reset"],
                title='NO DATA (No point selected)', 
                color='white', 
                line_color = None,
            )      
        else:
            ref_time = dt.datetime.strptime(
                self.ref_time_str, 
                '%Y-%m-%d %Hz'
            )
            rectangle_data = (
                ref_time, 
                0-self.flow_location_max*0.05, 
                ref_time + self.dates.forecast_duration, 
                self.flow_location_max*1.1
            )
            bars = hv.Rectangles(rectangle_data, label="flow").opts(
                **self.flow_hourly_ts_opts,
                tools=["pan","box_zoom","reset"],
                title = f"{self.point_id}",  
                color='gray', 
                line_color = 'gray', 
                alpha= 0.3,
                ylim=(
                    0-self.flow_location_max*0.05, 
                    self.flow_location_max*1.05
                )
            )
        return bars  

   ######################## methods for both precip and streamflow


    @param.depends("coord_stream.x", "coord_stream.y")
    def get_selected_point_from_xy(self):
        '''
    
        '''
        if self.coord_stream.x is np.nan:
            centroid = self.precip_poly_max['geometry'].iloc[0].centroid
            point_gv = gv.Points(
                (centroid.x, centroid.y), 
                label='Selected location (click to select)'
            )
        else: 
            point_gv = gv.Points(
                (self.coord_stream.x, self.coord_stream.y), 
                label='Selected location (click to select)'
            )
        return point_gv.opts(
            line_color="black", 
            fill_color='white', 
            marker="o", 
            line_width=1, 
            size=8
        )

    @param.depends("point_stream.index")
    def get_selected_point(self):
        '''
    
        '''
        
        # update selected point and timeseries if not already
        self.update_flow_timeseries_for_selected_point()

        if self.point_id is None or self.point_stream.index == []:
            point = gv.Points([])
        else:
            try:
                gdf = self.flow_points_gdf[
                    self.flow_points_gdf['primary_location_id'] == self.point_id
                ]
                gdf = gdf[['geometry','name']]
                point = gv.Points(
                    gdf, 
                    label='Selected point (click here)'
                ).opts(
                    size=10,
                    fill_color='none',
                    line_color='fuchsia',
                    line_width=3
                )
            except:
                self.point_id = None
                point = gv.Points([])
                
        return point

    @param.depends("point_stream.index")
    def get_selected_usgs_basin(self):
        '''
    
        '''
        
        # update selected point and timeseries if not already
        self.update_flow_timeseries_for_selected_point()

        if self.point_id is None or self.point_stream.index == []:
            poly = gv.Polygons([])
        else:
            try:
                gdf = self.geo.usgs_basins[
                    self.geo.usgs_basins['id'] == self.point_id]
                gdf = gdf[['geometry']]
                poly = gv.Polygons(
                    gdf, 
                    label='Upstream basin (if available)'
                ).opts(
                    fill_color='none',
                    line_color='dimgray',
                    line_width=2
                )
            except:
                self.point_id = None
                poly = gv.Polygons([])
        return poly    
        
    def get_date_lists(self, gdf):
        '''
    
        '''
        self.ref_time_list = sorted(gdf['reference_time'].unique())
        self.ref_time_list_str = [
            t.strftime('%Y-%m-%d %Hz') for t in self.ref_time_list
        ]    

        #initialize the ref_time showing in dashbaord on launch 
        # as the ref_time in which the max precip occurred
        if self.explore_precip:
            self.ref_time_str = self.precip_poly_max[
                'reference_time'].iloc[0].strftime('%Y-%m-%d %Hz')
        else:
            self.ref_time_str = self.flow_points_max[
                'reference_time'].min().strftime('%Y-%m-%d %Hz')
            
        self.analysis_value_time_list_df = pd.DataFrame(
            pd.date_range(
                self.dates.analysis_time_start, 
                self.dates.analysis_time_end, 
                freq='1H'
            ),
            columns=['value_time']
        )        
        self.analysis_value_time_list_df \
            = self.analysis_value_time_list_df.astype('datetime64[us]')

        self.data_value_time_list_df = pd.DataFrame(
            pd.date_range(
                self.dates.data_value_time_start, 
                self.dates.data_value_time_end, 
                freq='1H'
            ),
            columns=['value_time']
        )        
        self.data_value_time_list_df \
            = self.data_value_time_list_df.astype('datetime64[us]')

        # empty_ts_curve
        self.empty_ts_curve = hv.Curve(
            (self.ref_time_list[0], 0)
        ).opts(   
            tools=["pan","box_zoom","reset"],
            title='NO DATA (No point selected)',
            line_color = 'white'
        )  
    
    def get_unit_labels(self):
        '''
    
        '''
        if self.paths.units == 'metric':
            self.precip_unit_label = '(mm)'
            self.flow_unit_label = '(cms)'
            self.norm_flow_unit_label = '(mm/hr)'
            self.norm_vol_unit_label = '(mm)'
            self.area_label = '(km^2)'
        else:
            self.precip_unit_label = '(in)'
            self.flow_unit_label = '(cfs)'
            self.norm_flow_unit_label = '(in/hr)'
            self.norm_vol_unit_label = '(in)'
            self.area_label = '(mi^2)'
            
        if self.dates.forecast_config in ['short_range','SRF','short range']:
            self.precip_fcst_source = 'HRRR'
        else:
            self.precip_fcst_source = 'GFS'
            
        if 'extend' in str(self.paths.forcing_filepaths['primary_filepath']):
            self.precip_obs_source = 'Stage IV'
        else:
            self.precip_fcst_source = 'MRMS'
        
    @param.depends("ref_time_str")
    def ref_time_text(self):
        '''
    
        '''
        ref_time = dt.datetime.strptime(
            self.ref_time_str, 
            '%Y-%m-%d %Hz'
        )
        end_value_time_str = (
            ref_time + self.dates.forecast_duration
            ).strftime('%Y-%m-%d %Hz')
        
        return f"Forecast Period: {self.ref_time_str} - {end_value_time_str}"

    
    def get_xtick_date_labels(self):
        '''
    
        '''
        if self.dates.forecast_config in ['medium_range','medium_range_mem1']:
            
            tick_labels = [
                (d, d.strftime('%m-%d %Hz')) if d.hour in [0,12] \
                 else (d, '') if d.hour in [6,18] \
                 else 0 for d in self.analysis_value_time_list_df['value_time']
            ] 
            tick_labels = [t for t in tick_labels if t !=0]
            
        else:
            tick_labels = [
                (d, d.strftime('%m-%d %Hz')) if d.hour in [0,12] \
                else (d, '') for d in self.analysis_value_time_list_df['value_time']
            ]     
        return tick_labels

    def get_xtick_date_labels_daily(self):
        '''
    
        '''
        tick_labels = [
            (d, d.strftime('%-m/%d')) \
            for d in self.analysis_value_time_list_df['value_time'] \
            if d.hour in [0]
        ]
        return tick_labels

