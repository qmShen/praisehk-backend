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
        """
        Hard code
        """
        self.client = MongoClient('127.0.0.1', 27017)
        self.db = self.client['Praise_hk_label']
        self.collection = self.db['label']

        self.region_df = pd.read_csv(path)
        self.aq_station_df = pd.read_csv(aq_station_path)
        self.mete_station_df = pd.read_csv(mete_station_path)
        self.region_dicts = self.region_df.to_dict('records')


        self.pm25_json = None
        self.pm25_cmaq_json = None
        self.wind_json = None
        self.winddir_json = None
        self.wind_wrf_json = None
        self.winddir_wrf_json = None
        self.cmaqid_2_stationid_map = None

    def get_regions(self):
        return self.region_dicts

    def read_aq_stations(self):
        """
        None
        :return:  stations data{id, longitude, latitude, WRF_id, CMAQ_id, [missing_rate_<feature_name>]}
        """
        aq_station_path = './data/version0/aq_stations.csv'
        aq_station_df = pd.read_csv(aq_station_path)
        data = aq_station_df.to_dict('records')
        self.cmaqid_2_stationid_map = dict(zip(aq_station_df['CMAQ_id'].astype(str), aq_station_df['id'].astype(str)))
        return data


    def read_mete_station(self):
        """
        None
        :return:  stations data{id, longitude, latitude, WRF_id, CMAQ_id, [missing_rate_<feature_name>]}
        """
        mete_station_path = './data/version0/mete_stations.csv'
        mete_station_df = pd.read_csv(mete_station_path)
        self.wrfid_2_stationid_map = dict(zip(mete_station_df['WRF_id'].astype(str), mete_station_df['id'].astype(str)))
        data = mete_station_df.to_dict('records')
        return data

    def read_station_info_test(self):

        self.aq_station_df = pd.read_csv(aq_station_path)
        self.mete_station_df = pd.read_csv(aq_station_path)

        return {
            'aq_stations': self.aq_station_df.to_dict('records'),
            'mete_stations': self.mete_station_df.to_dict('records')
        }


    def read_feature_data(self, start_time = None, end_time = None):
        PM25_path_3km = './data/version0/PM25_error_agg3h.csv'
        wind_path_3km = './data/version0/Wind_error_agg3h.csv'

        winddir_path_3km = './data/version0/WindDir_error_agg3h.csv'

        wind_df = pd.read_csv(wind_path_3km)
        PM25_df = pd.read_csv(PM25_path_3km)
        winddir_df = pd.read_csv(winddir_path_3km)


        if start_time is not None and end_time is not None:
            PM25_df = PM25_df[(PM25_df['timestamp'] >= start_time) & (PM25_df['timestamp'] <= end_time)]
            wind_df = wind_df[(wind_df['timestamp'] >= start_time) & (wind_df['timestamp'] <= end_time)]
            winddir_df = winddir_df[(winddir_df['timestamp'] >= start_time) & (winddir_df['timestamp'] <= end_time)]
            wind_df.fillna('null', inplace=True)
            PM25_df.fillna('null', inplace=True)
            winddir_df.fillna('null', inplace=True)
            print('columns', PM25_df.columns)

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
        # Now
        PM_obs_path = './data/version0/PM25_obs_agg1h.csv'
        PM_CMAQ_path = './data/version0/PM25_cmaq_agg1h.csv'
        PM_CMAQ_df = pd.read_csv(PM_CMAQ_path)
        PM_obs_df = pd.read_csv(PM_obs_path)
        PM_CMAQ_df = PM_CMAQ_df.rename(columns=self.cmaqid_2_stationid_map)

        _PM_CMAQ_df = PM_CMAQ_df[['timestamp', station_id]].rename(columns={station_id: 'val_cmaq'})
        _PM_obs_df = PM_obs_df[['timestamp', station_id]].rename(columns={station_id: 'val_aq'})
        merge_new_df = pd.merge(_PM_CMAQ_df, _PM_obs_df, how='outer', left_on='timestamp', right_on='timestamp')
        merge_new_df.fillna('null', inplace=True)

        return merge_new_df.to_dict('records')
        # PM25_path_3km = './data/station/{}_PM25_{}h.csv'.format(station_id, hour)
        # PM25_df = pd.read_csv(PM25_path_3km)
        # PM25_df.fillna('null', inplace=True)
        # return PM25_df.to_dict('records')

    def read_AQ_by_stations(self, start_time = None, end_time = None):
        """
        :return:
        """
        _temp_path = './data/PM25_obs_by_stations.csv'
        _temp_path = './data/version0/PM25_obs_agg1h.csv'
        PM25_df = pd.read_csv(_temp_path)
        print('start_time, end_time', start_time, end_time)
        if start_time is not None and end_time is not None:
            PM25_df = PM25_df[(PM25_df['timestamp'] >= start_time) & (PM25_df['timestamp'] <= end_time)]
            print('t', PM25_df.shape)
        PM25_df.fillna('null', inplace=True)
        PM_json = PM25_df.to_dict('records')
        return PM_json

    def read_CMAQ_by_stations(self, start_time = None, end_time = None):
        """
        :return:
        """
        _temp_path = './data/version0/PM25_cmaq_agg1h.csv'
        df = pd.read_csv(_temp_path)
        print('read data ', _temp_path, start_time, end_time)

        df = df.rename(columns = self.cmaqid_2_stationid_map)
        if start_time is not None and end_time is not None:
            df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
            print('t', df.shape)
        df.fillna('null', inplace=True)
        PM_json = df.to_dict('records')
        return PM_json

    def read_wind_by_stations(self, start_time = None, end_time = None):
        """
        :return:
        """
        _temp_path = './data/Wind_obs_by_stations.csv'
        _temp_path = './data/version0/Wind_obs_agg1h.csv'
        df = pd.read_csv(_temp_path)
        if start_time is not None and end_time is not None:
            df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
            print('t', df.shape)
        df.fillna('null', inplace=True)
        mete_json = df.to_dict('records')
        return mete_json

    def read_winddir_by_stations(self, start_time = None, end_time = None):
        """
        :return:
        """
        _temp_path = './data/WindDir_obs_by_stations.csv'
        _temp_path = './data/version0/WindDir_obs_agg1h.csv'
        df = pd.read_csv(_temp_path)

        if start_time is not None and end_time is not None:
            df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
            print('t', df.shape)
        df.fillna('null', inplace=True)
        mete_json = df.to_dict('records')
        return mete_json

    def read_wind_WRF_by_stations(self, start_time = None, end_time = None):
        """
        :return:
        """
        _temp_path = './data/Wind_WRF_by_stations.csv'
        _temp_path = './data/version0/Wind_wrf_agg1h.csv'
        df = pd.read_csv(_temp_path)
        df = df.rename(columns = self.wrfid_2_stationid_map)
        if start_time is not None and end_time is not None:
            df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
            print('t', df.shape)
        df.fillna('null', inplace=True)
        mete_json = df.to_dict('records')
        return mete_json

    def read_winddir_WRF_by_stations(self, start_time = None, end_time = None):
        """

        :return:
        """
        _temp_path = './data/WindDir_WRF_by_stations.csv'
        _temp_path = './data/version0/WindDir_wrf_agg1h.csv'
        df = pd.read_csv(_temp_path)
        df = df.rename(columns=self.wrfid_2_stationid_map)
        if start_time is not None and end_time is not None:
            df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
            print('t', df.shape)
        df.fillna('null', inplace=True)
        mete_json = df.to_dict('records')
        return mete_json


    def read_PM25_mean_error(self, start_time = None, end_time = None):
        _temp_path = './data/version0/PM25_error_agg1h.csv'
        df = pd.read_csv(_temp_path)
        if start_time is not None and end_time is not None:
            df = df[(df['timestamp'] >= start_time) & (df['timestamp'] < end_time)]

        HongKongStationList = [67, 68, 70, 74, 77, 78, 79, 80, 81, 82, 83, 84, 85, 87, 89, 90]
        HKS = [str(i) for i in HongKongStationList]
        error = df[HKS].mean(1)
        result = pd.DataFrame()
        result['timestamp']  = df['timestamp']
        result['error'] = error

        result.fillna('null', inplace=True)
        mete_json = result.to_dict('records')
        return mete_json

    def save_label_data(self, start_time = None, end_time = None, user = None, label = None, feature = None):
        """

        :return:
        """
        _temp_path = './data/labeling_data_by_user.csv'
        with open(_temp_path, 'a+') as file:
            file.write('{}, {}, {}, {}, {}\n'.format(user, label, feature, start_time, end_time))


    def save_label_to_db(self, start_time = None, end_time = None, user = None, label = None, feature = 'PM25', stationId = None, labelType = None):
        """
        startTime, endTime, userName, label
        :return:
        """
        self.collection.insert_one({
            "id": "{}_{}".format(user, time.time()),
            'startTime': start_time,
            'endTime': end_time,
            'userName': user,
            'label': label,
            'feature': feature,
            'stationId': stationId,
            'labelType': labelType
        })
        pass
if __name__ == '__main__':
    dataService = DataService(None)
    dataService.get_recent_records(0, 100)
