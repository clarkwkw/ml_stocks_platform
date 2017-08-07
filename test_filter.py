import pandas
from DataPreparation import TestingDataPreparation

df = pandas.read_csv("./test_data/Materials-test.csv", parse_dates = ['date'], na_values = ['nan'])
test_data = TestingDataPreparation(df, True, "./test_data/Materials_preprocessing_info.json")