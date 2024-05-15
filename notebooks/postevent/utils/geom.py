'''
geometry related utilities
'''
import geopandas as gpd
import holoviews as hv
hv.extension('bokeh')

from typing import List, Union
from shapely import Polygon

def get_states_subset_overlapping_huc2_subsets(
    huc2_gdf: gpd.GeoDataFrame,
    huc2_list: List[str], 
    states_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    '''
    get a subset of the states geodataframe
    '''
    
    overlaps = []
    
    if huc2_list:
        huc2_subset = huc2_gdf[huc2_gdf.index.isin(huc2_list)]
        for index, row in states_gdf.iterrows():
            poly = row.geometry
            for poly2 in huc2_subset.geometry:
                if poly.intersection(poly2):
                    overlaps.append(row['NAME'])
        list(set(overlaps))
        states_subset = states_gdf[states_gdf['NAME'].isin(overlaps)]
        states_subset.index = states_subset.index.rename('ST_IND')
    else:
        states_subset = states_gdf
        states_subset.index = states_subset.index.rename('ST_IND')
        
    return states_subset

def get_states_subset_overlapping_latlon_polygon(
    states: gpd.GeoDataFrame,
    polygon: Polygon
) ->  gpd.GeoDataFrame:
    '''
    get a subset of the states geodataframe
    '''
    
    overlaps = []
    for index, row in states.to_crs('epsg:4326').iterrows():
        poly = row.geometry
        if poly.intersection(polygon):
            overlaps.append(row['NAME'])
    list(set(overlaps))
    states_subset = states[states['NAME'].isin(overlaps)]
    states_subset.index = states_subset.index.rename('ST_IND')
    
    return states_subset

def get_map_latlon_limits(
    huc2_gdf: gpd.GeoDataFrame, 
    huc2_list: List[str] = [], 
    huc10_gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(),
    huc10_list: List[str] = [],
    poly: Polygon = Polygon(), 
    poly_stream: hv.streams.PolyDraw = None
) -> List[tuple]:
    '''
    get lat/lon limits for maps
    '''    
       
    if huc2_list:
        huc2_gdf_subset = huc2_gdf[huc2_gdf.index.isin(huc2_list)]
    else:
        huc2_gdf_subset = huc2_gdf       
    huc2_bounds = huc2_gdf_subset['geometry'].to_crs('4326').total_bounds
    huc2_lat_limits = (huc2_bounds[1], huc2_bounds[3])
    huc2_lon_limits = (huc2_bounds[0], huc2_bounds[2])

    if huc10_list:
        huc10_gdf_subset = huc10_gdf[huc10_gdf.index.isin(huc10_list)]
        huc10_bounds = huc10_gdf_subset['geometry'].to_crs('4326').total_bounds
        huc10_lat_limits = (huc10_bounds[1], huc10_bounds[3])
        huc10_lon_limits = (huc10_bounds[0], huc10_bounds[2])        
    else:
        huc10_lat_limits = huc2_lat_limits
        huc10_lon_limits = huc2_lon_limits       

    # plot limits are the smaller of huc2, static polygon, 
    # and stream polygon limits
    lat_min = max(
        huc2_lat_limits[0], 
        huc10_lat_limits[0], 
        region_lat_limits[0], 
        zoom_lat_limits[0]
    )
    lat_max = min(
        huc2_lat_limits[1], 
        huc10_lat_limits[1], 
        region_lat_limits[1], 
        zoom_lat_limits[1]
    )
    lon_min = max(
        huc2_lon_limits[0], 
        huc10_lon_limits[0], 
        region_lon_limits[0], 
        zoom_lon_limits[0]
    )
    lon_max = min(
        huc2_lon_limits[1], 
        huc10_lon_limits[1], 
        region_lon_limits[1], 
        zoom_lon_limits[1]
    )   

    print(huc2_lat_limits, huc2_lon_limits)
    print(huc10_lat_limits, huc10_lon_limits)

    xlims = (lon_min*1.001, lon_max*0.999)
    ylims = (lat_min*0.999, lat_max*1.001)    
    
    return xlims, ylims

def get_box_coords_from_lims(
    xlims: tuple, 
    ylims: tuple
):
    
    x=list(xlims)
    y=list(ylims)
    xcoords = x + x[::-1] + [x[0]]
    ycoords = [y[0]] + y + y[::-1]
    
    return xcoords, ycoords
        
def get_polygon_from_coords(
    coords: List[float],
) -> Polygon:
    '''
    Get a polygon object based on lat/lon coordinates
    coords must be a list of lists:
    [ [x_coords], [y_coords] ]
    '''
    if len(coords) == 2:
        x_coords = coords[0]
        y_coords = coords[1]   
        if y_coords and x_coords:
            if len(y_coords) != len(x_coords):
                raise ValueError(
                    'x and y polygon coordinates in event ' \
                    'definitions file are not the same length'
                )  
            poly = Polygon(
                [[x_coords[i], y_coords[i]] for i in range(len(y_coords))]
            )
        else:
            poly = Polygon()
    else:
        poly = Polygon()
        
    return poly

def get_polygon_from_poly_stream(
    stream: Union[hv.streams.PolyEdit, hv.streams.PolyDraw, None],
) -> Polygon:

    if stream is None:
        poly = Polygon()
        
    else:
        poly_selection_data = stream.data

        if poly_selection_data['xs']:
            x_coords = list(poly_selection_data['xs'][0])
            y_coords = list(poly_selection_data['ys'][0])
            poly = get_polygon_from_coords([x_coords, y_coords])
        else:
            poly = Polygon()
    
    return poly

def get_domain_limits(
    domain: str
) -> dict[tuple]:
    
    if domain == 'conus':
        xlims_lon = (-130.0, -61.0)
        ylims_lat = (20.0, 54.0)
    elif domain == 'hawaii':
        xlims_lon = (-154.0, -161.0)
        ylims_lat = (18.0, 23.0)        
    elif domain == 'alaska':
        xlims_lon = (-180.0, -125.0)
        ylims_lat = (52.0, 73.0)
    elif domain == 'puertorico':
        xlims_lon = (-68.5, -65.0)
        ylims_lat = (17.8, 18.6)
        
    xlims_mercator, ylims_mercator = project_limits_to_mercator(
        xlims_lon, ylims_lat
    )
    
    return dict(
        xlims_lon=xlims_lon,
        ylims_lat=ylims_lat,
        xlims_mercator=xlims_mercator,
        ylims_mercator=ylims_mercator
    )

def project_limits_to_mercator(
    xlims_lon: tuple, 
    ylims_lat: tuple,
) -> tuple:

    xcoords, ycoords = get_box_coords_from_lims(xlims_lon, ylims_lat)
    poly_gdf = gpd.GeoDataFrame(
        index=[0], 
        geometry=[get_polygon_from_coords([xcoords, ycoords])], 
        crs='EPSG:4326'
    )
    bounds_mercator = poly_gdf['geometry'].to_crs('3857').total_bounds
    xlims_mercator = (bounds_mercator[0], bounds_mercator[2])
    ylims_mercator = (bounds_mercator[1], bounds_mercator[3])
    
    return xlims_mercator, ylims_mercator

def adjust_square_map_limits(
    map_limits: dict[tuple],
) -> dict[tuple]:
    
    xrange=map_limits['xlims_lon'][1]-map_limits['xlims_lon'][0]
    yrange=map_limits['ylims_lat'][1]-map_limits['ylims_lat'][0]

    target_ratio = 1.3
    if xrange/yrange < target_ratio:
        xrange_new = yrange * target_ratio
        xdelta = (xrange_new - xrange)/2
        map_limits['xlims_lon'] = (
            map_limits['xlims_lon'][0] - xdelta, 
            map_limits['xlims_lon'][1] + xdelta
        )
    else:
        yrange_new = xrange / target_ratio
        ydelta = (yrange_new - yrange)/2
        map_limits['ylims_lat'] = (
            map_limits['ylims_lat'][0] - ydelta, 
            map_limits['ylims_lat'][1] + ydelta
        )
        
    map_limits['xlims_mercator'], map_limits['ylims_mercator'] = \
        project_limits_to_mercator(map_limits['xlims_lon'], map_limits['ylims_lat'])
    
    return map_limits
