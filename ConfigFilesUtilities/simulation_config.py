import json
from utils import get_input_str, get_input_bool, get_input_number, save_dict

output_filename = "simulation_config.json"

def generate_simulation_config():
	config_dict = {}

	print("Basic settings: ")
	config_dict['run_code'] = get_input_str("Run code")

	config_dict['model_flag'] = get_input_str("Model flag", options = ["SVM", "NN", "Custom"])

	if config_dict['model_flag'] == "Custom":
		config_dict['model_class_dir'] = get_input_str("Custom model directory (parent directory of Model)")

	config_dict['stock_filter_flag'] = get_input_bool("Filter stocks by LIQ, DTV and Price filters")


	print("Metaparameters settings: ")

	is_para_tune = get_input_bool("Need to tune parameters")

	if is_para_tune:
		config_dict['meta_paras'] = []
		config_dict['rolling_training_data'] = True
		config_dict['para_tune_holding_flag'] = get_input_str("Which kind of position to evaluate when selecting metaparameters", options = ["long", "short", "long_short"], end = "?")
		config_dict['reserve_train_data'] = get_input_number("No. year of data to be reserved for the 1st model", lower_limit = 1, is_int = True)
		config_dict['para_tune_data_split_period'] = get_input_number("No. of year of data per fold", lower_limit = 1, upper_limit = config_dict['para_tune_reserve_data'], is_int = True)
		
		print("> You will need to manually edit the 'meta_paras' field in '%s', it should be"%output_filename)
		print("> 1. a list of dictionaries, where each dicitionary represents a parameter set, or")
		print("> 2. 'model_def', if you have defined metaparameters set in the custom model script.")

	else:
		config_dict['meta_paras'] = {}
		config_dict['rolling_training_data'] = get_input_bool("Construct training data in a rolling way")
		config_dict['reserve_train_data'] = get_input_number("No. year of data to be reserved for the 1st model", lower_limit = 0, is_int = True)

		print("> If you wish to pass arguments to the model,")
		print("> you will need to manually edit the 'meta_paras' field in '%s', it should be:"%output_filename)
		print("> a dicitionary of parameters to be passed to the model.")
	
	print("Other training settings: ")
	
	config_dict['B_top'], config_dict['B_bottom'] = 0, 0

	while True:
		config_dict['B_top'] = get_input_number("Percentage of the best performing stock into training data", lower_limit = 0, upper_limit = 100)
		
		config_dict['B_bottom'] = get_input_number("Percentage of the worst performing stock into training data", lower_limit = 0, upper_limit = 100)
		
		if config_dict['B_top'] + config_dict['B_bottom'] <= 0 or  config_dict['B_top'] + config_dict['B_bottom'] > 100:
			print("The sum of the 2 percentage must be > 0 and <= 100")
			continue
		
		break

	config_dict['model_training_frequency'] = get_input_number("Frequency (in days) to train a model", lower_limit = 1, is_int = True)
	
	config_dict['portfolio_holding_period'] = get_input_number("Duration (in days) to hold a portfolio", lower_limit = 1, is_int = True)
	
	config_dict['trading_stock_quantity'] = get_input_number("No. of stocks to trade for long and short positions", lower_limit = 1, is_int = True)
	
	config_dict['target_label_holding_period'] = get_input_number("No. of days of reutrn to consider when generating labels", lower_limit = 1, upper_limit = config_dict['portfolio_holding_period'], is_int = True)

	config_dict['strategy_performance_period'] = get_input_number("No. of months to consider when generating one strategy report", lower_limit = 1, is_int = True)

	print("Generating config file '%s'.."%output_filename)
	save_dict(config_dict, output_filename)