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

aq_station_path = './data/region_config/loc_aq_2016_3km.csv'
mete_station_path = './data/region_config/loc_mete_2016_3km.csv'
class DataService:
    def __init__(self):
        pass
        """
        Hard code
        """
        self.region_df = pd.read_csv(path)
        self.aq_station_df = pd.read_csv(aq_station_path)
        self.mete_station_df = pd.read_csv(mete_station_path)
        # self.region_dicts = self.region_df.drop(columns=['col_index', 'row_index']).to_dict('records')
        self.region_dicts = self.region_df.to_dict('records')


        self.pm25_json = None
        self.pm25_cmaq_json = None
        self.wind_json = None
        self.winddir_json = None
        self.wind_wrf_json = None
        self.winddir_wrf_json = None
    def get_regions(self):
        return self.region_dicts

    def read_aq_stations(self):
        """
        None
        :return:  stations data{id, longitude, latitude, WRF_id, CMAQ_id, [missing_rate_<feature_name>]}
        """
        aq_station_df = pd.read_csv(aq_station_path)
        data = aq_station_df.to_dict('records')
        return data


    def read_mete_station(self):
        """
        None
        :return:  stations data{id, longitude, latitude, WRF_id, CMAQ_id, [missing_rate_<feature_name>]}
        """
        mete_station_df = pd.read_csv(mete_station_path)
        data = mete_station_df.to_dict('records')
        return data

    def read_station_info_test(self):

        self.aq_station_df = pd.read_csv(aq_station_path)
        self.mete_station_df = pd.read_csv(aq_station_path)

        return {
            'aq_stations': self.aq_station_df.to_dict('records'),
            'mete_stations': self.mete_station_df.to_dict('records')
        }


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
            # {
            #     'feature': 'wind',
            #     'value': wind_df.to_dict('records')
            # },
            # {
            #     'feature': 'windDir',
            #     'value': winddir_df.to_dict('records')
            # }
        ]

        return data

    def read_station_cmaq_obs(self, station_id, hour = 1):
        PM25_path_3km = './data/station/{}_PM25_{}h.csv'.format(station_id, hour)
        PM25_df = pd.read_csv(PM25_path_3km)
        PM25_df.fillna('null', inplace=True)
        return PM25_df.to_dict('records')

    def read_AQ_by_stations(self):
        """
        :return:
        """
        _temp_path = './data/PM25_obs_by_stations.csv'
        if self.pm25_json is None:
            PM25_df = pd.read_csv(_temp_path)
            PM25_df.fillna('null', inplace=True)
            self.pm25_json = PM25_df.to_dict('records')
        return self.pm25_json

    def read_CMAQ_by_stations(self):
        """
        :return:
        """
        _temp_path = './data/PM25_CMAQ_by_stations.csv'
        if self.pm25_cmaq_json is None:
            PM25_df = pd.read_csv(_temp_path)
            PM25_df.fillna('null', inplace=True)
            self.pm25_cmaq_json = PM25_df.to_dict('records')
        return self.pm25_cmaq_json

    def read_wind_by_stations(self):
        """
        :return:
        """
        _temp_path = './data/Wind_obs_by_stations.csv'
        if self.wind_json is None:
            PM25_df = pd.read_csv(_temp_path)
            PM25_df.fillna('null', inplace=True)
            self.wind_json = PM25_df.to_dict('records')
        return self.wind_json

    def read_winddir_by_stations(self):
        """
        :return:
        """
        _temp_path = './data/WindDir_obs_by_stations.csv'
        if self.winddir_json is None:
            PM25_df = pd.read_csv(_temp_path)
            start_time = time.time()
            PM25_df.fillna('null', inplace=True)
            self.winddir_json = PM25_df.to_dict('records')
            print('WindDir obs usage time: ', time.time() - start_time)
        return self.winddir_json

    def read_wind_WRF_by_stations(self):
        """
        :return:
        """
        _temp_path = './data/Wind_WRF_by_stations.csv'
        if self.wind_wrf_json is None:
            PM25_df = pd.read_csv(_temp_path)
            start_time = time.time()
            PM25_df.fillna('null', inplace=True)
            self.wind_wrf_json = PM25_df.to_dict('records')

            print('Wind WRF obs usage time: ', time.time() - start_time)
        return self.wind_wrf_json

    def read_winddir_WRF_by_stations(self):
        """

        :return:
        """
        _temp_path = './data/WindDir_WRF_by_stations.csv'
        if self.winddir_wrf_json is None:
            PM25_df = pd.read_csv(_temp_path)
            start_time = time.time()
            PM25_df.fillna('null', inplace=True)
            self.winddir_wrf_json = PM25_df.to_dict('records')
            print('WindDir WRF obs usage time: ', time.time() - start_time)
        return self.winddir_wrf_json
if __name__ == '__main__':
    dataService = DataService(None)
    dataService.get_recent_records(0, 100)
