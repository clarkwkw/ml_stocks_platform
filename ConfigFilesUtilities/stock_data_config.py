import json
from utils import get_input_str, get_input_bool, get_input_number, save_dict, get_input_str_list

output_filename = "stock_data_config.json"

def generate_stock_data_config():
	config_dict = {}

	config_dict['market_id'] =  get_input_str("Model flag", options = ["US"])
	config_dict['stock_data_code'] =  get_input_str("Stock data code")
	config_dict['market_cap'] = get_input_number("Minimum market capitalization (-1 if not filtering)", lower_limit = -1)
	if config_dict['market_cap'] == -1:
		del config_dict['market_cap']
	else:
		config_dict['include_null_cap'] = get_input_bool("Include stocks whose market capitalization is NULL")

	config_dict['sectors'] = get_input_str_list("Sectors")

	config_dict['period'] = {}
	config_dict['period']['start'] = get_input_str("Dataset start date (YYYY-MM-DD)")
	config_dict['period']['end'] = get_input_str("Dataset end date (YYYY-MM-DD)")

	config_dict['factors'] = get_input_str_list("Factors")

	config_dict['use_loaded_data'] = get_input_bool("Use previously downloaded data")

	config_dict['save_downloaded_data'] = get_input_bool("Save downloaded data to file")

	print("Generating config file '%s'.."%output_filename)
	save_dict(config_dict, output_filename)