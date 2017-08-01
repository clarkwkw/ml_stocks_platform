import utils
import os
from DataSource import DownloadTableFileFromMySQL

def SimulateTradingProcess(simulation_config_dict, stock_data_config_dict):
	__setup_dirs(stock_data_config_dict['stock_data_code'], simulatetion_config_dict['run_code'], stock_data_config_dict['sectors'])
	with open("run_config.json", "w") as f:
		f.write(json.dump(simulatetion_config_dict), indent = 4)
	

def __setup_dirs(stock_data_code, run_code, sectors):
	utils.create_dir("./%s"%stock_data_code)
	utils.create_dir("./%s/%s"%(stock_data_code, run_code))
	os.cwd("./%s/%s"%(stock_data_code, run_code))

	utils.create_dir("./model")
	utils.create_dir("./model/preprocessing")
	for sector in sectors:
		utils.create_dir("./model/%s"%sector)

	utils.create_dir("./portfolio")
	utils.create_dir("./portfolio/full_portfolio")
	utils.create_dir("./portfolio/return_report")
	utils.create_dir("./portfolio/predicted_value")
	utils.create_dir("./portfolio/ranked_stock")
	utils.create_dir("./portfolio/sector_portfolio")
	for sector in sectors:
		utils.create_dir("./portfolio/%s"%sector)