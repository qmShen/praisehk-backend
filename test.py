from app.DataService.DataService import DataService

dataService = DataService()

dataService.save_label_to_db(start_time = 12, end_time = 13, user = 'qiaomu', label = "test", feature = "PM25")