import geopandas as gpd
import param
import holoviews as hv
import geoviews as gv
import cartopy.crs as ccrs
hv.extension('bokeh', logo=False)
gv.extension('bokeh', logo=False)

from .. import config

class Extents(param.Parameterized):
    '''
    Main class for visualization extents selector dashboard
    '''
    #dir_name = param.String(default="YYYYMM_name")
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

    def get_selected_huc2s(self):
        
        if not self.geo.huc2_subset.empty:
            polys = gv.Polygons(
                self.geo.huc2_subset.to_crs(3857), 
                vdims=['id'], 
                crs=ccrs.GOOGLE_MERCATOR, 
                label='selected HUC2s'
            ).opts(
                color_index=None, 
                fill_color='none', 
                line_color='red', 
                line_width=2, 
                nonselection_alpha=1
            )
        else:
            polys = gv.Polygons([])

        return polys

    def get_event_polygon(self):
        
        if self.geo.region_polygon:
            poly_gdf = gpd.GeoDataFrame(
                index=[0], 
                geometry=[self.geo.region_polygon]
            )
            poly = gv.Polygons(
                poly_gdf, 
                label='bounding polygon'
            ).opts(
                fill_color='none', 
                line_color='darkorange', 
                line_dash='dashed', 
                line_width=3
            ) 
        else:
            poly = gv.Polygons([])
        
        return poly

    def get_zoom_polygon(self):
        return gv.Polygons([]).opts(
            fill_color='none', 
            line_color='darkgreen', 
            line_dash='solid', 
            line_width=5, 
            active_tools=['poly_draw']
        )    
