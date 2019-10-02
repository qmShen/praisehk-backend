# -*- coding: UTF-8 -*-
from app import app
import json
from app.DataService.DataService import DataService
from flask import request
import pandas as pd
import numpy as np
import time

dataService = DataService()
print('here')

@app.route('/')
def index():
    return app.send_static_file('index.html')


@app.route('/test')
def test():
    return "test"

@app.route('/cmaq_region')
def getregion():
    print('run here')
    return json.dumps(dataService.get_regions())

@app.route('/feature_data', methods = ['POST'])
def get_feature_data():
    start_time = time.time()
    post_data = json.loads(request.data.decode())
    print('Time range !!!!!!!!!!: ', post_data)
    st = post_data['startTime'] if 'startTime' in post_data else None
    et = post_data['endTime'] if 'endTime' in post_data else None
    data = dataService.read_feature_data(st, et)
    print("Use timex", time.time() - start_time)
    # print(data)
    return json.dumps(data)



@app.route('/load_cmaq_obs', methods = ['POST'])
def get_cmaq_obs_data():
    post_data = json.loads(request.data.decode())
    print('Get region sector ', post_data)
    print(post_data)
    data = dataService.read_station_cmaq_obs(post_data['station_id'])
    return json.dumps(data)


# version 0

@app.route('/aq_stations')
def read_aq_stations():
    start_time = time.time()
    data = dataService.read_aq_stations()
    print('Get AQ location data, use time: ', time.time() - start_time)
    return json.dumps(data)

@app.route('/mete_stations')
def read_mete_stations():
    start_time = time.time()
    data = dataService.read_mete_station()
    print('Get mete location data, use time: ', time.time() - start_time)
    return json.dumps(data)


@app.route('/load_observation', methods = ['POST'])
def read_AQ_by_station():
    post_data = json.loads(request.data.decode())
    # print('Post data 1', post_data)
    st = post_data['startTime'] if 'startTime' in post_data else None
    et = post_data['endTime'] if 'endTime' in post_data else None
    start_time = time.time()
    if post_data['feature'] == 'PM25':
        data = dataService.read_AQ_by_stations(st, et)
    elif post_data['feature'] == 'wind':
        data = dataService.read_wind_by_stations(st, et)
    elif post_data['feature'] == 'winddir':
        data = dataService.read_winddir_by_stations(st, et)
    print('model {}: '.format(post_data['feature']), time.time() - start_time)
    return json.dumps(data)

@app.route('/load_model_value', methods = ['POST'])
def read_CMAQ_by_station():
    # print('--------------------------------------')
    # print(request.data.decode())
    post_data = json.loads(request.data.decode())
    # print('Post data 2', post_data)
    st = post_data['startTime'] if 'startTime' in post_data else None
    et = post_data['endTime'] if 'endTime' in post_data else None
    start_time = time.time()
    if post_data['feature'] == 'PM25':
        data = dataService.read_CMAQ_by_stations(st, et)
    elif post_data['feature'] == 'wind':
        data = dataService.read_wind_WRF_by_stations(st, et)
    elif post_data['feature'] == 'winddir':
        data = dataService.read_winddir_WRF_by_stations(st, et)

    print('model {}: '.format(post_data['feature']), time.time() - start_time)
    return json.dumps(data)



@app.route('/load_mean_error', methods = ['POST'])
def read_mean_error():
    post_data = json.loads(request.data.decode())
    st = post_data['startTime'] if 'startTime' in post_data else None
    et = post_data['endTime'] if 'endTime' in post_data else None
    start_time = time.time()
    data = dataService.read_PM25_mean_error(st, et)
    print('Get mean error of HK stations, use time: ', time.time() - start_time)
    return json.dumps(data)

@app.route('/save_labels', methods = ['POST'])
def save_label_names():
    post_data = json.loads(request.data.decode())
    st = post_data['startTime'] if 'startTime' in post_data else None
    et = post_data['endTime'] if 'endTime' in post_data else None
    user = post_data['username'] if 'username' in post_data else None
    label = post_data['label'] if 'label' in post_data else None
    feature = post_data['feature'] if 'feature' in post_data else None
    if st is not None | et is not None | user is not None | label is not None | feature is not None:
        dataService.save_label_data(st, et, user, label, feature)
    return app.send_static_file('index.html')



if __name__ == '__main__':
    pass
