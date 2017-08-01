import json
import os
import PortfolioConstructionUtilities as PortfolioUtils

def test():
	with open("./test_data/simulation_config.json") as f:
		simulation_config_dict = json.load(f)
	with open("./test_data/stock_data_config.json") as f:
		stock_data_config_dict = json.load(f)

	os.chdir("./test_output")
	PortfolioUtils.SimulateTradingProcess(simulation_config_dict, stock_data_config_dict)