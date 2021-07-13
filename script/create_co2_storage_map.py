import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.ops import unary_union
from shapely.geometry import *

def remove_third_dimension(geom):
    if geom.is_empty:
        return geom

    if isinstance(geom, Polygon):
        exterior = geom.exterior
        new_exterior = remove_third_dimension(exterior)

        interiors = geom.interiors
        new_interiors = []
        for int in interiors:
            new_interiors.append(remove_third_dimension(int))

        return Polygon(new_exterior, new_interiors)

    elif isinstance(geom, LinearRing):
        return LinearRing([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, LineString):
        return LineString([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, Point):
        return Point([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, MultiPoint):
        points = list(geom.geoms)
        new_points = []
        for point in points:
            new_points.append(remove_third_dimension(point))

        return MultiPoint(new_points)

    elif isinstance(geom, MultiLineString):
        lines = list(geom.geoms)
        new_lines = []
        for line in lines:
            new_lines.append(remove_third_dimension(line))

        return MultiLineString(new_lines)

    elif isinstance(geom, MultiPolygon):
        pols = list(geom.geoms)

        new_pols = []
        for pol in pols:
            new_pols.append(remove_third_dimension(pol))

        return MultiPolygon(new_pols)

    elif isinstance(geom, GeometryCollection):
        geoms = list(geom.geoms)

        new_geoms = []
        for geom in geoms:
            new_geoms.append(remove_third_dimension(geom))

        return GeometryCollection(new_geoms)

    else:
        raise RuntimeError("Currently this type of geometry is not supported: {}".format(type(geom)))

def generate_storage_capacity_map(table_path = 'Hydrocarbon_Storage_Units.csv', map_path = 'StorageUnits_March13.geojson'):
    '''
    Input:
         table_path -> str: path to the file that store co2 storage_unit capacity data
         map_path -> str: path to geojson file that store geographical data of co2 storage_unit

    Output:
         storage_unit_map_lite -> geopandas.GeoDataFrame: geogrpahical data with its storage_unit capacity
    '''
    # 1) load and basic dataset cleaning------------------------------------------------------------------------------
    # load capacity
    storage_unit = pd.read_csv(table_path)

    # load geographical data
    storage_unit_map = gpd.read_file(map_path)
    # select useful columns
    storage_unit_map_lite = storage_unit_map[['COUNTRY','COUNTRYCOD','ID','geometry']]
    # Combine (multi-)polygons with the same id into one multi-polygon
    storage_unit_map_lite = storage_unit_map_lite.groupby(['COUNTRY','COUNTRYCOD','ID']).agg(unary_union).reset_index()
    storage_unit_map_lite = gpd.GeoDataFrame(storage_unit_map_lite, crs = 'EPSG:4326')

    # 2) Create our estimation value---------------------------------------------------------------------------------
    #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # EST_STORECAP_ as main body, use STORE_CAP_ to fill missing value, lastly add capacity in STORE_CAP_HCDAUGHTER 
    #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # conservative estimate: use MIN
    storage_unit['conservative estimate Mt'] = storage_unit['EST_STORECAP_MIN'].replace(0,np.nan)
    storage_unit['conservative estimate Mt'].fillna(storage_unit['STORE_CAP_MIN'],inplace = True)
    storage_unit['conservative estimate Mt'] = storage_unit['conservative estimate Mt'] + storage_unit['STORE_CAP_HCDAUGHTER'] 

    # neutral estimate: use MEAN
    storage_unit['neutral estimate Mt'] = storage_unit['EST_STORECAP_MEAN'].replace(0,np.nan)
    storage_unit['neutral estimate Mt'].fillna(storage_unit['STORE_CAP_MEAN'],inplace = True)
    storage_unit['neutral estimate Mt'] = storage_unit['neutral estimate Mt'] + storage_unit['STORE_CAP_HCDAUGHTER']

    # optimistic estimate: use MAX
    storage_unit['optimistic estimate Mt'] = storage_unit['EST_STORECAP_MAX'].replace(0,np.nan)
    storage_unit['optimistic estimate Mt'].fillna(storage_unit['STORE_CAP_MAX'],inplace = True)
    storage_unit['optimistic estimate Mt'] = storage_unit['optimistic estimate Mt'] + storage_unit['STORE_CAP_HCDAUGHTER']

    #++++++++++++++++++++++++++++++++++++
    # cross level missing value filling
    #++++++++++++++++++++++++++++++++++++
    # replace 0 with np.nan. In this dataset 0 means missing value
    storage_unit['neutral estimate Mt'].replace(0,np.nan,inplace= True)
    storage_unit['optimistic estimate Mt'].replace(0,np.nan,inplace= True)
    # use conservative estimation to fill missing value in neural estimation
    storage_unit['neutral estimate Mt'].fillna(storage_unit['conservative estimate Mt'],inplace = True)
    # use neutral estimation to fill missing value in optimistic estimation
    storage_unit['optimistic estimate Mt'].fillna(storage_unit['neutral estimate Mt'],inplace = True)
    # replace remaining missing vlaue with 0
    storage_unit.fillna(0,inplace = True)

    #+++++++++++++++++++++++++++
    # only keep useful columns
    #+++++++++++++++++++++++++++
    capacity_list = ['conservative estimate Mt','neutral estimate Mt','optimistic estimate Mt']
    storage_unit_lite = storage_unit[['STORAGE_UNIT_ID']+capacity_list]


    # 3) Add capacity data (storage_unit_lite) to the map---------------------------------------------------------------
    storage_unit_map_lite = storage_unit_map_lite.merge(storage_unit_lite, left_on = 'ID', 
                                                        right_on = 'STORAGE_UNIT_ID', how = 'left')
    storage_unit_map_lite.drop('STORAGE_UNIT_ID',axis=1,inplace = True)
    
    # 4) save capacity map----------------------------------------------------------------------------------------------
    storage_unit_map_lite.to_file('storage_unit_map_lite.geojson',driver='GeoJSON')
    return storage_unit_map_lite


def generate_trap_capacity_map(table_path = ['Hydrocarbon_Traps.csv',
                                     'Hydrocarbon_Traps_Temp.csv',
                                     'Hydrocarbon_Traps1.csv'], map_path = 'DaughterUnits_March13.geojson'):

    '''
    Input:
         table_path -> str: path to the file that store co2 daughter units capacity data
         map_path -> str: path to geojson file that store geographical data of co2 daughter units

    Output:
         storage_unit_map_lite -> geopandas.GeoDataFrame: geogrpahical data with its daughter units capacity
    '''
    # 1) load data-------------------------------------------------------------------------------
    # capacity tables
    if isinstance(table_path,str):
        trap = pd.read_csv(table_path)
    elif isinstance(table_path,list):
        table_list = []
        for path in table_path:
            table_list.append(pd.read_csv(path))
             # combine all three sub table into one table
            trap = pd.concat(table_list)
            trap.reset_index(drop = True, inplace = True)
    else:
        raise ValueError('table_path is neither string nor list')
    # map
    trap_map = gpd.read_file(map_path)
    trap_map_lite = trap_map[['COUNTRY','COUNTRYCOD','ID','geometry']]
    trap_map_lite = trap_map_lite.groupby(['COUNTRY','COUNTRYCOD','ID']).agg(unary_union).reset_index()
    trap_map_lite = gpd.GeoDataFrame(trap_map_lite, crs = 'EPSG:4326')

    # 2) Create our estimation value------------------------------------------------------------------------------------
        #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        # EST_STORECAP_ as main body, use STORE_CAP_ to fill missing value, lastly add capacity in STORE_CAP_HCDAUGHTER 
        #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # conservative estimate: use MIN
    trap['conservative estimate aquifer Mt'] = trap['EST_STORECAP_MIN'].replace(0,np.nan)
    trap['conservative estimate aquifer Mt'].fillna(trap['STORE_CAP_MIN'],inplace = True)

    trap['conservative estimate OIL Mt'] = trap['MIN_EST_STORE_CAP_OIL'].replace(0,np.nan)
    trap['conservative estimate OIL Mt'].fillna(trap['MIN_CALC_STORE_CAP_OIL'],inplace = True)

    trap['conservative estimate GAS Mt'] = trap['MIN_EST_STORE_CAP_GAS'].replace(0,np.nan)
    trap['conservative estimate GAS Mt'].fillna(trap['MIN_CALC_STORE_CAP_GAS'],inplace = True)

    trap['conservative estimate Mt'] = trap[['conservative estimate aquifer Mt',
                                                    'conservative estimate OIL Mt',
                                                    'conservative estimate GAS Mt']].sum(axis=1)

    # neural estimate: use MEAN
    trap['neutral estimate aquifer Mt'] = trap['EST_STORECAP_MEAN'].replace(0,np.nan)
    trap['neutral estimate aquifer Mt'].fillna(trap['STORE_CAP_MEAN'],inplace = True)
    

    trap['neutral estimate OIL Mt'] = trap['MEAN_EST_STORE_CAP_OIL'].replace(0,np.nan)
    trap['neutral estimate OIL Mt'].fillna(trap['MEAN_CALC_STORE_CAP_OIL'],inplace = True)

    trap['neutral estimate GAS Mt'] = trap['MEAN_EST_STORE_CAP_GAS'].replace(0,np.nan)
    trap['neutral estimate GAS Mt'].fillna(trap['MEAN_CALC_STORE_CAP_GAS'],inplace = True)

    trap['neutral estimate Mt'] = trap[['neutral estimate aquifer Mt',
                                                    'neutral estimate OIL Mt',
                                                    'neutral estimate GAS Mt']].sum(axis=1)

    # optimistic estimate: use MAX
    trap['optimistic estimate aquifer Mt'] = trap['EST_STORECAP_MAX'].replace(0,np.nan)
    trap['optimistic estimate aquifer Mt'].fillna(trap['STORE_CAP_MAX'],inplace = True)
    

    trap['optimistic estimate OIL Mt'] = trap['MAX_EST_STORE_CAP_OIL'].replace(0,np.nan)
    trap['optimistic estimate OIL Mt'].fillna(trap['MAX_CALC_STORE_CAP_OIL'],inplace = True)

    trap['optimistic estimate GAS Mt'] = trap['MAX_EST_STORE_CAP_GAS'].replace(0,np.nan)
    trap['optimistic estimate GAS Mt'].fillna(trap['MAX_CALC_STORE_CAP_GAS'],inplace = True)

    trap['optimistic estimate Mt'] = trap[['optimistic estimate aquifer Mt',
                                                    'optimistic estimate OIL Mt',
                                                    'optimistic estimate GAS Mt']].sum(axis=1)    
    
    # cross level missing value filling
    trap['neutral estimate Mt'].replace(0,np.nan,inplace= True)
    trap['optimistic estimate Mt'].replace(0,np.nan,inplace= True)

    trap['neutral estimate Mt'].fillna(trap['conservative estimate Mt'],inplace = True)
    trap['optimistic estimate Mt'].fillna(trap['neutral estimate Mt'],inplace = True)

    trap.fillna(0,inplace = True)

    # only keep useful columns
    trap_capacity_list = ['optimistic estimate Mt', 
                    'neutral estimate Mt', 
                    'conservative estimate Mt',
    'optimistic estimate aquifer Mt',
    'optimistic estimate OIL Mt',
    'optimistic estimate GAS Mt',
    'neutral estimate aquifer Mt',
    'neutral estimate OIL Mt',
    'neutral estimate GAS Mt',
    'conservative estimate aquifer Mt',
    'conservative estimate OIL Mt',
    'conservative estimate GAS Mt']    
    trap_lite = trap.loc[:,['TRAP_ID']+trap_capacity_list]        

    # 3) Add capacity data (storage_unit_lite) to the map---------------------------------------------------------------
    trap_map_lite = trap_map_lite.merge(trap_lite, left_on = 'ID', right_on = 'TRAP_ID', how = 'left')
    trap_map_lite.drop('TRAP_ID',axis=1,inplace = True)

    # 4) Save
    trap_map_lite.to_file('trap_map_lite.geojson', driver='GeoJSON')
    return trap_map_lite


def combination(trap_map,storage_map):
    '''
    Input: 
          storage_map -> geopandas.GeoDataFrame: geogrpahical data with its storage_unit capacity
          trap_map -> geopandas.GeoDataFrame: geogrpahical data with its daughter units capacity 
    Output:
          complete_map -> geopandas.GeoDataFrame: union of two co2 storage capacity map
    '''
    union_columns = set(list(trap_map.columns) + list(storage_map.columns))
    for column in union_columns:
        if column not in trap_map.columns:
            trap_map[column] = 0
        if column not in storage_map.columns:
            storage_map[column] = 0

    storage_map['geometry'] = storage_map['geometry'].apply(remove_third_dimension)
    trap_map['geometry'] = trap_map['geometry'].apply(remove_third_dimension)

    complete_map = gpd.GeoDataFrame(pd.concat([storage_map, trap_map]), crs = 'EPSG:4326')
    #complete_map.to_file('co2_storage_capacity_all_type.geojson', driver='GeoJSON')
    return complete_map
    

if __name__ == '__main__':
    if 'snakemake' not in globals():
        from vresutils.snakemake import MockSnakemake
        #import os
        #os.chdir('.')
        snakemake = MockSnakemake(
            wildcards=dict(network='elec', simpl='', clusters='37', lv='1.0',
                            opts='', planning_horizons='2020',
                            sector_opts='168H-T-H-B-I'),

            input=dict(sto_table = 'data/Hydrocarbon_Storage_Units.csv',
                        sto_map = 'data/StorageUnits_March13.geojson',
                        traps_table1 = 'data/Hydrocarbon_Traps.csv',
                        traps_table2 = 'data/Hydrocarbon_Traps_Temp.csv',
                        traps_table3 = 'data/Hydrocarbon_Traps1.csv',
                        traps_map = 'data/DaughterUnits_March13.geojson'
                        ),
            output=dict(complete_map_path='data/complete_map_{planning_horizons}_unit_Mt.geojson'),
        )
        #import yaml
        #with open('../config.default.yaml', encoding='utf8') as f:
        #    snakemake.config = yaml.safe_load(f)

    storage_map = generate_storage_capacity_map(snakemake.input.sto_table,snakemake.input.sto_map)
    trap_map = generate_trap_capacity_map([snakemake.input.traps_table1,
                                                 snakemake.input.traps_table2,
                                                 snakemake.input.traps_table3],snakemake.input.traps_map)
    complete_map = combination(trap_map, storage_map)
    complete_map.geometry = complete_map.geometry.buffer(0)
    complete_map.to_file(snakemake.output.complete_map_path, driver= 'GeoJSON')
    
    
