'''
Parameterized class for linked scatter-map-histogram dashboard
'''
import param
import hvplot
import hvplot.pandas
import datetime as dt
import pandas as pd
import numpy as np
import colorcet as cc
import holoviews as hv
hv.extension('bokeh', logo=False)

from bokeh.models import DatetimeTickFormatter

from .. import utils
from .. import config
from . import data

class ScatterExplorer(param.Parameterized):
    '''

    '''
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
    scatter_variable_options = param.List(
        ['Peak Flow','Peak Time'], 
        item_type=str
    )
    color_variable_options = param.List(
        ['Stream Order','Ecoregion'], 
        item_type=str
    )
    scatter_variable = param.Selector(objects=[])
    color_variable = param.Selector(objects=[])
    
    def initialize(
        self, 
        scatter_variable_options=['Peak Flow'], 
        color_variable_options=['Stream Order']
    ):
        
        self.get_unit_labels() 
        self.paths.set_streamflow_paths(
            nwm_version = self.event.nwm_version, 
            domain=self.event.domain
        )
        self.scatter_variable = self.scatter_variable_options[0]
        self.color_variable = self.color_variable_options[0]
        self.get_flow_metrics()

    def get_flow_metrics(self):  
        '''
    
        '''
        # get peak flows, threshold exceedance and cont. matrix
        flow_metrics_gdf = data.teehr_get_flow_metrics(
            self.paths, 
            self.event, 
            self.dates,
        )
        # drop nan locations (where flow forecasts were not generated, usually reservoir features)
        flow_metrics_gdf.dropna(inplace=True)
        
        # add attributes and easting/northing to facilitate linked plots
        flow_metrics_gdf = flow_metrics_gdf.merge(
            self.geo.usgs_points_subset[['id','name'] + self.geo.attribute_list], 
            how='left', 
            left_on='primary_location_id', 
            right_on='id'
        )
        flow_metrics_gdf = flow_metrics_gdf.drop(columns=['id'])
        flow_metrics_gdf = self.add_ecoregion_int(flow_metrics_gdf)
        flow_metrics_gdf['latitude'] = flow_metrics_gdf['geometry'].y

        projected = flow_metrics_gdf.to_crs("EPSG:3857")
        flow_metrics_gdf['easting'] = projected.geometry.x
        flow_metrics_gdf['northing'] = projected.geometry.y        

        # if drainage area is extremely small or zero, 
        # normalized flows blow up, 
        # REMOVE FOR NOW - REVISIT LATER
        flow_metrics_gdf = flow_metrics_gdf[
            flow_metrics_gdf['drainage_area'] > 0.1
        ]
        flow_metrics_gdf = flow_metrics_gdf[
            ~flow_metrics_gdf['drainage_area'].isna()
        ]
        flow_metrics_gdf['log_drainage_area'] = np.log(
            flow_metrics_gdf['drainage_area']
        )

        # convert to df, remove lat/lon geometry 
        df = flow_metrics_gdf[[
            c for c in flow_metrics_gdf.columns 
            if c not in ['geometry','measurement_unit']
        ]].copy()

        # add metrics (not yet avail in teehr)
        df = data.add_percent_difference(df)
        df = data.add_flow_exceedence(df)  
        df = data.add_prior_signal_time(df)
        df = data.add_normalized_peakflow(df, self.paths)
        df = data.add_normalized_volume(df, self.paths)

        # add time_to_peak as hour from min reftime as type float
        # same dtype as flow required to switch back and forth between
        # scatter variables

        dt = (df['primary_max_value_time'] - df['reference_time'].min())
        df['primary_max_value_hour'] = dt / pd.Timedelta(hours=1)
        dt = df['secondary_max_value_time'] - df['reference_time'].min()
        df['secondary_max_value_hour'] = dt / pd.Timedelta(hours=1)
        self.flow_metrics_df = df
        
        # get the set of unique points
        df_points_unique = df[
            ['primary_location_id','easting','northing','name']
        ].groupby('primary_location_id').first().reset_index()

        # get unique number based on gage list order
        df_points_unique['gage_rank'] = df_points_unique.index

        # add gage_rank to flow metrics
        self.flow_metrics_df = self.flow_metrics_df.merge(
            df_points_unique[['primary_location_id','gage_rank']], 
            how='left', 
            on='primary_location_id'
        )

        # get the average metric per point
        df_metrics_mean = self.flow_metrics_df[
            ['primary_location_id'] 
            + self.get_scatter_column_headers()
            + self.get_color_column_headers()
        ].groupby(
            'primary_location_id'
        ).mean().reset_index()
        
        # convert stream order and ecoregion to type int
        df_metrics_mean['stream_order']  \
            = df_metrics_mean['stream_order'].astype('int')
        df_metrics_mean['ecoregion_int'] \
            = df_metrics_mean['ecoregion_int'].astype('int')
        
        # re-add easting/northing to points
        self.points_df = df_points_unique[
            ['primary_location_id','easting','northing','name']
        ].merge(
            df_metrics_mean, 
            how='left', 
            on='primary_location_id'
        )
        self.points_df.replace([np.inf, 10e6], inplace=True)

    @param.depends("scatter_variable")
    def get_scatter_diagonal(self):
        '''

        '''
        scatter_variable_dict = self.get_scatter_variable_dict()
        kdims=     scatter_variable_dict['kdims']
        vdims=     scatter_variable_dict['vdims']
        lims=      scatter_variable_dict['lims']
        xlabel=    scatter_variable_dict['xlabel']
        ylabel=    scatter_variable_dict['ylabel']
        formatter= scatter_variable_dict['formatter']
        xrotation= scatter_variable_dict['xrotation']

        curve = hv.Curve(
            [
                [lims[0],lims[0]],
                [lims[1],lims[1]]
            ]
        ).opts(
            color='gray',
            alpha=0.5,
        )

        return curve
    
    @param.depends("scatter_variable","color_variable")
    def get_scatter(self):
        '''

        '''        
        scatter_variable_dict = self.get_scatter_variable_dict()
        kdims=     scatter_variable_dict['kdims']
        vdims=     scatter_variable_dict['vdims']
        lims=      scatter_variable_dict['lims']
        xlabel=    scatter_variable_dict['xlabel']
        ylabel=    scatter_variable_dict['ylabel']
        formatter= scatter_variable_dict['formatter']
        xrotation= scatter_variable_dict['xrotation']
        ticks=     scatter_variable_dict['tick_labels']

        color_variable_dict = self.get_color_variable_dict()
        color_column=   color_variable_dict['column']
        cmap=           color_variable_dict['cmap']
        clim=           color_variable_dict['clim']
        histlims=       color_variable_dict['histlims']
        cnorm=          color_variable_dict['cnorm']
        clabel=         color_variable_dict['label']
        sort_ascending= color_variable_dict['sort_ascending']

        all_columns = [
            'primary_location_id', 
            'easting', 
            'northing', 
            'reference_time',
            'name'
        ] \
            + self.get_scatter_column_headers() \
            + self.get_color_column_headers()
        
        df = self.flow_metrics_df[all_columns]
        vdims = vdims + [c for c in all_columns if c not in kdims + vdims]
        
        scatter = hv.Scatter(
            df, 
            kdims=kdims, 
            vdims=vdims
        ).opts(
            color=color_column, 
            cmap=cmap, 
            cnorm=cnorm, 
            clim=clim, 
            clabel=None, #clabel,
            ylim=lims, 
            xlim=lims, 
            xlabel=xlabel, 
            ylabel=ylabel, 
            xrotation=xrotation,
            xformatter=formatter, 
            yformatter=formatter,
            framewise=True
        )
        
        return scatter

    @param.depends("color_variable")
    def get_points(self):
        '''

        '''
        color_variable_dict = self.get_color_variable_dict()
        color_column=   color_variable_dict['column']
        cmap=           color_variable_dict['cmap']
        clim=           color_variable_dict['clim']
        cnorm=          color_variable_dict['cnorm']

        all_columns = ['primary_location_id','easting','northing','name'] \
            + self.get_scatter_column_headers() \
            + self.get_color_column_headers()
        
        df = self.points_df[all_columns]
        kdims=['easting','northing']
        vdims = [color_column] + [
            c for c in all_columns if c not in kdims + [color_column]
        ]

        points = hv.Points(
            df, 
            kdims=kdims, 
            vdims=vdims
        ).opts(
            color=hv.dim(color_column), 
            cmap=cmap, 
            cnorm=cnorm,  
            clim=clim,
            colorbar=True, 
            clabel=None,
            size=5, 
            xaxis=None, 
            yaxis=None, 
            tools=['hover'])

        return points

    def get_histogram(
        self, 
        var, 
        nbins=50
    ):
        '''
    
        '''
        all_columns                                        \
            = ['primary_location_id','easting','northing'] \
            + self.get_scatter_column_headers()            \
            + self.get_color_column_headers()

        df = self.points_df[all_columns]
        
        color_variable_dict = self.get_color_variable_dict(
            color_variable=var
        )
        column= color_variable_dict['column']
        histlims= color_variable_dict['histlims']
        label= color_variable_dict['label']
        frequencies, edges = np.histogram(
            df[column], 
            nbins
        )

        return df.hvplot.hist(
            y=column, 
            bins=nbins, 
            bin_range=histlims, 
            xlabel=label
        )
    
    def get_categorical_histogram(
        self,
        var, 
        labels=None
    ):
        '''
    
        '''
        all_columns                                        \
            = ['primary_location_id','easting','northing'] \
            + self.get_scatter_column_headers()            \
            + self.get_color_column_headers()
        
        df = self.points_df[all_columns]
        
        color_variable_dict = self.get_color_variable_dict(
            color_variable=var
        )
        histlims= color_variable_dict['histlims']
        column= color_variable_dict['column']
        label= color_variable_dict['label']
        
        nbins=len(df[column].unique())
        frequencies, edges = np.histogram(df[column], nbins)
        centers = list(edges[:-1] + (edges[1:] - edges[:-1])/2)
        if labels is None:
            labels = list(range(1,nbins+1))
        xticks=[(centers[i], labels[i]) for i in range(0,nbins)]

        return df.hvplot.hist(
            y=column, 
            bins=nbins, 
            xlabel=label
        ).opts(
            xticks=xticks
        )
        
    def get_scatter_variable_dict(
        self, 
        scatter_variable = None
    ):
        '''
    
        '''
        df = self.flow_metrics_df
        
        if not scatter_variable:
            scatter_variable = self.scatter_variable

        if scatter_variable == 'Peak Flow':
            kdim = 'primary_maximum'
            vdim = 'secondary_maximum'
            scatter_variable_dict = {
                'kdims':[kdim],
                'vdims':[vdim],
                'lims':self.get_scatter_lims(
                    df, 
                    kdim, 
                    vdim)
                ,
                'xlabel':'Observed Peak Flow ' + self.flow_unit_label,
                'ylabel':'Forecast Peak Flow ' + self.flow_unit_label,
                'formatter': '%f',
                'xrotation': 45,
                'tick_labels': []
            }
        elif scatter_variable == 'Peak Time':
            kdim = 'primary_max_value_hour' #_time
            vdim = 'secondary_max_value_hour' #_time           
            scatter_variable_dict = {
                'kdims':[kdim],
                'vdims':[vdim],
                'lims':self.get_scatter_lims(
                    df, 
                    kdim, 
                    vdim, 
                    var_type='time'
                ),
                'xlabel':'Observed Peak Time (hr from analysis t0)',
                'ylabel':'Forecast Peak Time (hr from analysis t0)',
                'formatter': '%f',
                # 'formatter': DatetimeTickFormatter(
                #     hours = '%m-%d %Hz', 
                #     strip_leading_zeros=False
                # ),
                'xrotation': 45,
                'tick_labels': self.get_scatter_time_ticks(df)
            }
        elif scatter_variable == 'Norm. Peak Flow':
            kdim = 'primary_maximum_norm'
            vdim = 'secondary_maximum_norm'
            scatter_variable_dict = {
                'kdims':[kdim],
                'vdims':[vdim],
                'lims':self.get_scatter_lims(
                    df, 
                    kdim, 
                    vdim
                ),
                'xlabel':'Observed Norm. Peak Flow ' \
                    + self.norm_flow_unit_label,
                'ylabel':'Forecast Norm. Peak Flow ' \
                    + self.norm_flow_unit_label,
                'formatter': '%f',
                'xrotation': 45,
                'tick_labels': []
            }
        elif scatter_variable == 'Norm. Volume':
            kdim = 'primary_sum_norm'
            vdim = 'secondary_sum_norm'
            scatter_variable_dict = {
                'kdims':[kdim],
                'vdims':[vdim],
                'lims':self.get_scatter_lims(
                    df, 
                    kdim, 
                    vdim, 
                ),
                'xlabel':'Observed Norm. Flow Volume ' \
                    + self.norm_vol_unit_label,
                'ylabel':'Forecast Norm. Flow Volume ' \
                    + self.norm_vol_unit_label,
                'formatter': '%f',
                'xrotation': 45,
                'tick_labels': []
            }
        return scatter_variable_dict

    def get_scatter_lims(
        self, 
        df, 
        kdim, 
        vdim, 
        var_type='flow'
    ):
        '''

        '''
        if var_type == 'flow':
            max_value = max(df[kdim].max(), df[vdim].max())
            lims=(max_value*-0.1, max_value*1.1) 
        elif var_type == 'time':
            min_value = min(df[kdim].min(), df[vdim].min())
            max_value = max(df[kdim].max(), df[vdim].max())
            dt = (max_value - min_value)*0.1
            lims=(min_value - dt, max_value + dt)   
        return lims    

    def get_scatter_time_ticks(
        self, 
        df, 
    ):
        
        hourmax = df[['primary_max_value_hour',
                      'secondary_max_value_hour']].max().max()
        dmin = df['reference_time'].min()
        dmax = df['reference_time'].min() + dt.timedelta(hours=hourmax)
        d = pd.date_range(dmin, dmax, freq='H')
        h = [float(hr) for hr in range(0, int(hourmax))]
        tick_labels = [
            (h[i], d[i].strftime('%m-%d %Hz')) if d[i].hour in [0,12] \
            else (h[i], '') for i in range(0, len(h))
        ]
        return tick_labels

    def get_color_variable_dict(
        self, 
        color_variable = None
    ):
        '''
    
        '''
        df = self.flow_metrics_df
        
        if not color_variable:
            color_variable = self.color_variable
        
        if color_variable == 'Peak % Error':
            column = 'peak_percent_diff'
            color_variable_dict = {
                'column': column,
                'cmap': self.get_difference_colormap(),
                'clim': (-100,100),
                'histlims': (-100,300),
                'cnorm': 'linear',
                'label': 'Ave. Peak Error (%)',
                'sort_ascending': True, 
            }
        elif color_variable == 'Volume % Error':
            column = 'vol_percent_diff'
            color_variable_dict = {
                'column': column,
                'cmap': self.get_difference_colormap(),
                'clim': (-100,100),
                'histlims': (-100,300),
                'cnorm': 'linear',
                'label': 'Ave. Volume Error (%)',
                'sort_ascending': True, 
            }
        if color_variable == 'Peak Error':
            column = 'max_norm_diff'
            color_variable_dict = {
                'column': column,
                'cmap': self.get_difference_colormap(),
                'clim': (-0.1,0.1),
                'histlims': (df[column].min(), df[column].max()),
                'cnorm': 'linear',
                'label': 'Ave. Peak Error (in/hr)',
                'sort_ascending': True, 
            }
        elif color_variable == 'Volume Error':
            column = 'vol_norm_diff'
            color_variable_dict = {
                'column': column,
                'cmap': self.get_difference_colormap(),
                'clim': (-0.5, 0.5),
                'histlims': (df[column].min(), df[column].max()),
                'cnorm': 'linear',
                'label': 'Ave. Volume Error (in)',
                'sort_ascending': True, 
            }
        elif color_variable == 'Peak Timing Error':
            duration = 240
            if self.paths.forecast_config == 'short_range':
                duration = 18
            column = 'peak_time_diff_hours'
            color_variable_dict = {
                'column': column,
                'cmap': self.get_difference_colormap(),
                'clim': (-duration,duration),
                'histlims': (-duration,duration),
                'cnorm': 'linear',
                'label': 'Peak Timing Error (hrs)',
                'sort_ascending': True, 
            }
        elif color_variable == 'Stream Order':
            column = 'stream_order'
            color_variable_dict = {
                'column': column,
                'cmap': self.get_category_cmap(
                    df['stream_order'], 
                    cc.rainbow
                ),
                'clim': (df[column].min(), df[column].max()),
                'histlims': (df[column].min(), df[column].max()),
                'cnorm': 'linear',
                'label': 'Stream Order',
                'sort_ascending': True, 
            }
        elif color_variable == 'Ecoregion':
            column = 'ecoregion_int'
            color_variable_dict = {
                'column': column,
                'cmap': self.get_category_cmap(
                    df['ecoregion_int'], 
                    cc.rainbow
                ),
                'clim': (df[column].min(), df[column].max()),
                'histlims': (df[column].min(), df[column].max()),
                'cnorm': 'linear',
                'label': 'Ecoregion (Level II)',
                'sort_ascending': False, 
            }
        elif color_variable == 'Drainage Area':
            column = 'log_drainage_area'
            color_variable_dict = {
                'column': column,
                'cmap': cc.rainbow,
                'clim': (df[column].min(), df[column].max()),
                'histlims': (df[column].min(), df[column].max()),
                'cnorm': 'linear',
                'label': 'Log Drainage Area ' + self.area_unit_label,
                'sort_ascending': True, 
            }
        elif color_variable == 'Latitude':
            column = 'latitude'
            color_variable_dict = {
                'column': column,
                'cmap': cc.rainbow,
                'clim': (df[column].min(), df[column].max()),
                'histlims': (df[column].min(), df[column].max()),
                'cnorm': 'linear',
                'label': 'Latitude',
                'sort_ascending': False, 
            }
        elif color_variable == 'Gage Rank':
            column = 'gage_rank'
            color_variable_dict = {
                'column': column,
                'cmap': cc.rainbow,
                'clim': (df[column].min(), df[column].max()),
                'histlims': (df[column].min(), df[column].max()),
                'cnorm': 'linear',
                'label': 'Gage Rank',
                'sort_ascending': True, 
            }
        return color_variable_dict
  

    def get_category_cmap(
        self, 
        df, 
        cmap
    ):
        '''

        '''
        # if categorical attribute, get colormap subset
        ncats = df.nunique()
        cstep = int(np.floor(len(cmap) / ncats))
        return [cmap[cstep * t] for t in range(0,ncats)]  

    def get_difference_colormap(self):
        '''
    
        '''
        #difference, white at 0
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
     
        return cmap1 + white + cmap2   

    def get_scatter_column_headers(self):
        '''

        '''
        scatter_column_headers = []
        for var in self.scatter_variable_options:
            scatter_variable_dict = self.get_scatter_variable_dict(
                scatter_variable=var
            )
            scatter_column_headers = scatter_column_headers \
                + scatter_variable_dict['kdims'] \
                + scatter_variable_dict['vdims']
        return scatter_column_headers
    
    def get_color_column_headers(self):
        '''

        '''
        color_column_headers = []
        for var in self.color_variable_options:
            color_variable_dict = self.get_color_variable_dict(
                color_variable=var
            )
            color_column_headers.append(
                color_variable_dict['column']
            )
        return color_column_headers

    def add_ecoregion_int(
        self, 
        gdf
    ):
        '''

        '''
        
        # turn the string ecoregion into unique 
        # integers to enable histograms
        eco_df = pd.DataFrame(gdf['ecoregion'].unique())
        eco_df['num']=eco_df[0].str[0:4].astype('float')
        eco_df = eco_df.sort_values('num').reset_index()
        eco_list=list(eco_df[0])
        self.ecoregion_labels = eco_df['num']
        gdf['ecoregion_int'] = [
            eco_list.index(e)+1 for e in gdf['ecoregion']
        ]    
        return gdf

    @param.depends("color_variable")
    def get_colorbar_label(self):
        '''

        '''
        opts = dict(
            xlim=(0,2),
            ylim=(0,2), 
            height=150, 
            width=50, 
            toolbar=None, 
            xaxis=None, 
            yaxis=None,
            show_frame=False,
            text_align='center', 
            text_font_size='10pt',
            text_font_style='italic',
            angle=90
        ) 
        color_variable_dict = self.get_color_variable_dict()
        clabel = color_variable_dict['label']
        clim = color_variable_dict['clim']
        overlay = hv.Curve([(1,1),(1,1)]).opts(color=None) * \
                  hv.Text(1,1,clabel).opts(**opts, color='black')
        if clim[0] < -10 or clim[1] > 100:
            margin = (0,0,0,0)
        else:
            margin = (0,20,0,0)
        return overlay.opts(margin=margin)
    
    def get_unit_labels(self):
        '''

        '''
        if self.paths.units == 'metric':
            self.precip_unit_label = '(mm)'
            self.flow_unit_label = '(cms)'
            self.norm_flow_unit_label = '(mm/hr)'
            self.norm_vol_unit_label = '(mm)'
            self.area_unit_label = '(km\u00b2)'
        else:
            self.precip_unit_label = '(in)'
            self.flow_unit_label = '(cfs)'
            self.norm_flow_unit_label = '(in/hr)'
            self.norm_vol_unit_label = '(in)'
            self.area_unit_label = '(mi\u00b2)'

