
from app.DataService.DataService import DataService

if __name__ == '__main__':
    dataService = DataService()
    # dataService.get_recent_records(0, 100)
    # print(dataService.read_aq_stations())
    # print(dataService.read_mete_stations())
    # print(dataService.read_obs_feature('all', 'WindDir', timeRange = 12))
    # print(dataService.read_error_feature('all', 'WindDir', timeRange = 6))
    print(dataService.read_model_feature('all', 'WindDir', timeRange = 3))