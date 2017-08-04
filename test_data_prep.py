import DataPreparation
import pandas

energy_data = pandas.read_csv("./test_data/data_filter.csv", parse_dates = ['date'])
DataPreparation.TrainingDataPreparation(energy_data, stock_filter_flag = True, B_top = 10, B_bottom = 15, target_label_holding_period = 1, preprocessing_file = "preprocessing.json")
