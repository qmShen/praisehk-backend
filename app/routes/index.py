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

@app.route('/cmaq_region')
def getregion():
    print('run here')
    return json.dumps(dataService.get_regions())

@app.route('/feature_data')
def get_feature_data():
    start_time = time.time()
    data = dataService.read_feature_data()
    print("Use timex", time.time() - start_time)
    # print(data)
    return json.dumps(data)



@app.route('/load_cmaq_obs', methods = ['POST'])
def get_cmaq_obs_data():
    post_data = json.loads(request.data.decode())
    print('Get region sector ', post_data)
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
    print('Post data', post_data)
    start_time = time.time()
    if post_data['feature'] == 'PM25':
        data = dataService.read_AQ_by_stations()
    elif post_data['feature'] == 'wind':
        data = dataService.read_wind_by_stations()
    elif post_data['feature'] == 'winddir':
        data = dataService.read_winddir_by_stations()
    print('model {}: '.format(post_data['feature']), time.time() - start_time)
    return json.dumps(data)

@app.route('/load_model_value', methods = ['POST'])
def read_CMAQ_by_station():
    print('--------------------------------------')
    print(request.data.decode())
    post_data = json.loads(request.data.decode())
    print('Post data', post_data)
    start_time = time.time()
    if post_data['feature'] == 'PM25':
        data = dataService.read_CMAQ_by_stations()
    elif post_data['feature'] == 'wind':
        data = dataService.read_wind_WRF_by_stations()
    elif post_data['feature'] == 'winddir':
        data = dataService.read_winddir_WRF_by_stations()

    print('model {}: '.format(post_data['feature']), time.time() - start_time)
    return json.dumps(data)



if __name__ == '__main__':
    pass
