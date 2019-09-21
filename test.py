
from app.DataService.DataService import DataService

if __name__ == '__main__':
    dataService = DataService()
    # dataService.get_recent_records(0, 100)
    print(dataService.read_mete_stations())