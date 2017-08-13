import os
import pandas
import DataPreparation

train_end_date = "2010-01-29"
test_date = "2010-02-01"

# Under ./test_output folder
preprocessing_file = "Materials_preprocessing_info.json"

df = pandas.read_csv("./test_data/Materials-test.csv", parse_dates = ['date'], na_values = ['nan'])

try:
	os.makedirs("./test_output")
except OSError:
	pass
os.chdir("./test_output")

train_data = DataPreparation.TrainingDataPreparation(df, True, 25, 25, 1, preprocessing_file)

print(DataPreparation.runtime)