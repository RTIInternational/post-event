'''
parameterized class for data selector dashboard
'''

import param
from .. import config

class DataSelector_NWMOperational(param.Parameterized):

    variable = param.ListSelector(
        default = [
            'streamflow',
            'mean areal precipitation'
        ],
        objects=[
            'streamflow',
            'mean areal precipitation'
        ]
    )
    forecast_config = param.Selector(
        objects=[
            'short_range',
            'medium_range_mem1',
            'none'
        ]
    )
    verify_config = param.ListSelector(
        default = [
            'USGS*',
            'analysis_assim_extend', 
            'analysis_assim_extend_no_da*'
        ],
        objects=[
            'USGS*',
            'analysis_assim_extend', 
            'analysis_assim_extend_no_da*', 
            'analysis_assim', 
            'analysis_assim_no_da*', 
            'none']
    )
    reach_set = param.Selector(
        objects=['gaged reaches','all reaches']
    )
    map_polygons = param.ListSelector(
        default= ['HUC10','usgs_basins'],
        objects=['HUC10','usgs_basins']
    )
    overwrite_flag = param.Selector(
        objects=[False, True], 
        default=False
    )
    paths = param.ClassSelector(
        class_=config.Paths, 
        default=config.Paths(None)
    ) 
    event = param.ClassSelector(
        class_=config.Event, 
        default=config.Event(None)
    )   
    dates = param.ClassSelector(
        class_=config.Dates, 
        default=config.Dates(None, None)
    )

