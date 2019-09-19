# StationId, StationName, StationMap
import time
import json
from pymongo import MongoClient
import os
import pandas as pd
import numpy as np

HOST = '127.0.0.1'
PORT = 27017
DB = 'XRNN'

# path = './data/region_config/loc_aq_mete_5km.csv'
path = './data/region_config/loc_aq_mete_10km.csv'
# mete_path = './data/region_config/loc_info_aq.csv'
problem_path = './data/region_config/problematic_aq.csv'

PM25_path = './data/PM25_dif.csv'

wind_path = './data/wind_dif.csv'

PM25_path_3km = './data/PM25_3h_abs.csv'
wind_path_3km = './data/WSPD_3h_abs.csv'
winddir_path_3km = './data/WDIR_3h_abs.csv'
class DataService:
    def __init__(self):
        pass
        """
        Hard code
        """
        self.region_df = pd.read_csv(path)
        # self.region_dicts = self.region_df.drop(columns=['col_index', 'row_index']).to_dict('records')
        self.region_dicts = self.region_df.to_dict('records')
    def get_regions(self):
        return self.region_dicts

    def read_feature_data(self):

        wind_df = pd.read_csv(wind_path_3km)
        PM25_df = pd.read_csv(PM25_path_3km)
        winddir_df = pd.read_csv(winddir_path_3km)
        wind_df.fillna('null', inplace = True)
        PM25_df.fillna('null', inplace = True)
        winddir_df.fillna('null', inplace = True)

        data = [
            {
                'feature': 'PM25',
                'value': PM25_df.to_dict('records')
            },
            {
                'feature': 'wind',
                'value': wind_df.to_dict('records')
            },
            {
                'feature': 'windDir',
                'value': winddir_df.to_dict('records')
            }
        ]

        return data

    def read_station_cmaq_obs(self, station_id, hour = 1):
        PM25_path_3km = './data/station/{}_PM25_{}h.csv'.format(station_id, hour)
        PM25_df = pd.read_csv(PM25_path_3km)
        PM25_df.fillna('null', inplace=True)
        return PM25_df.to_dict('records')


if __name__ == '__main__':
    dataService = DataService(None)
    dataService.get_recent_records(0, 100)
