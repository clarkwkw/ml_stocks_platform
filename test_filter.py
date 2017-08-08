import os
import pandas
from DataPreparation import TrainingDataPreparation, TestingDataPreparation

train_end_date = "2010-01-29"
test_date = "2010-02-01"

# Under ./test_output folder
preprocessing_file = "Materials_preprocessing_info.json"

df = pandas.read_csv("./test_data/Materials-test.csv", parse_dates = ['date'], na_values = ['nan'])
train_df = df.loc[df['date'] <= pandas.Timestamp(train_end_date)]
test_df = df.loc[df['date'] == pandas.Timestamp(test_date)]

try:
	os.makedirs("./test_output")
except OSError:
	pass
os.chdir("./test_output")

train_data = TrainingDataPreparation(train_df, True, 10, 20, 1, preprocessing_file)
test_data = TestingDataPreparation(test_df, True, preprocessing_file)