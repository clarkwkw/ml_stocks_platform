import errno
import json
import os
import warnings

__ml_model_dev_paras = ["model_flag", "stock_filter_flag"]

def raise_warning(msg):
	warnings.warn(msg, Warning)

def set_equal_weights(stocks_df):
	n_stocks = stocks_df.shape[0]
	stocks_df['Weight'] = 1/n_stocks
	return stocks_df

def cwd(directory):
	os.chdir(directory)

def create_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def read_simulation_config(config_file):
	with open(config_file, "r") as f:
		config_dict = json.load(f)
		return config_dict