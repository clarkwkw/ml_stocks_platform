import MachineLearningModelUtilities as MLUtils
import utils
import DataPreparation
import numpy as np
import pandas

def StockPerformancePrediction(stock_data, stock_filter_flag, preprocessing_file, model_savedir, predict_value_file):
	test_dataset = DataPreparation.TestingDataPreparation(stock_data, stock_filter_flag = stock_filter_flag, preprocessing_file = preprocessing_file)
	predict_df = LearnedModelExecution(test_dataset, model_savedir)
	predict_df['Buying Price'] = test_dataset['last_price']
	predict_df = predict_df.rename(columns = {'target':'Predicted Value', 'date': 'Date', 'ticker': 'Ticker'})
	predict_df.to_csv(predict_value_file, index = False)

def LearnedModelExecution(test_dataset, model_savedir):
	model = MLUtils.loadTrainedModel(model_savedir)
	return model.predict(test_dataset)

def StockRanking(stock_file, ranked_stock_file):
	df = pandas.read_csv(stock_file)
	df.sort(columns = ['Predicted Value'], inplace = True)
	df.to_csv(ranked_stock_file, index = False)

def StockSelection(ranked_stock_file, n, portfolio_file, weight_method = "equal"):
	ranked_stocks = pandas.read_csv(ranked_stock_file)
	n_stocks = ranked_stocks.shape[0]
	if n_stocks <= 0:
		raise Exception("No stock to select.")
	if n_stocks < 2*n:
		Utils.raise_warning("Not enough stocks to select, n = %d, found %d"%(n, n_stocks))

	short_index, long_index = None, None
	# If 2n > n_stocks >= n, long_index may overlap with short_index
	# In this case, long position would be in higher priority
	# If n > n_stocks, all stock will be in long position
	if n_stocks >= n:
		short_index = (n_stocks - n, n_stocks - 1)
		long_index = (0, n - 1)
	else:
		long_index = (0, n_stocks - 1)
	ranked_stocks["position"] = ""
	if short_index is not None:
		ranked_stocks.loc[short_index[0]:short_index[1], "position"] = "short"
	ranked_stocks[long_index[0]:long_index[1], "position"] = "long"

	selected_stocks = ranked_stocks.loc[ranked_stocks["position"] != ""]
	if weight_method == "equal":
		selected_stocks = utils.set_equal_weights(selected_stocks)
	else:
		raise Exception("Unexpected weight_method '%s'"%str(weight_method))
	selected_stocks.to_csv(portfolio_file, index = False)
	
	return selected_stocks

def SimulateTradingProcess(WorkingLocation, StockDataCode, simulate_config_file):
	os.cwd("./%s/%s"%(WorkingLocation, StockDataCode))
	config = util.read_simulation_config(simulate_config_file)
	utils.create_dir(config['run_code'])
	os.cwd("./%s"%config['run_code'])
	with open("run_config.json", "w") as f:
		f.write(json.dump(config), indent = 4)
	pass

# ML_sector_factors: a dictionary of sector and corresponding list of factors
def MachineLearningModelDevelopment(ML_sector_factors, ML_model_flag, paras_set, stock_filter_flag, B_top, B_bottom, target_label_holding_period, trading_stock_quantity, para_tune_holding_flag, period = None, date = None, customized_module_dir = ""):
	models_map = {}
	for sector in ML_sector_factors:
		stock_data = ML_sector_factors[sector]
		preprocessing_file_path = "./model/preprocessing/%s_preprocessing_info.json"%sector
		best_para = MLUtils.selectMetaparameters(ML_model_flag, stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, trading_stock_quantity, para_tune_holding_flag, period = period, date = date, customized_module_dir = customized_module_dir, paras_set = paras_set)
		model = MLUtils.buildModel(ML_model_flag, preprocessing_file_path, stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, customized_module_dir = customized_module_dir, **best_para)
		models_map[sector] = model
	return models_map

def PortfolioConstruction(ML_sector_factors, stock_filter_flag, n, trading_stock_quantity, stock_filter_flag, date_prefix, inter_sector_weight = "equal"):
	full_portfolio = None
	n_sectors = len(ML_sector_factors)
	full_portfolio_path = "./portfolio/full_portfolio/%s_full_portfolio.csv"%date_prefix
	for sector in ML_sector_factors:
		stock_data = ML_sector_factors[sector]
		preprocessing_file_path = "./model/preprocessing/%s_preprocessing_info.json"%sector
		model_path = "./model/%s/"%sector
		predicted_value_path = "./portfolio/predicted_value/%s_%s_predicted_value.csv"%(date_prefix, sector)
		ranked_stocks_path = "./portfolio/ranked_stock/%s_%s_ranked_stock.csv"%(date_prefix, sector)
		sector_portfolio_path = "./portfolio/sector_portfolio/%s_%s_portfolio.csv"%(date_prefix, sector)

		StockPerformancePrediction(stock_data, stock_filter_flag, preprocessing_file_path, model_path, predicted_value_path)
		StockRanking(predicted_value_path, ranked_stocks_path)
		sector_portfolio = StockSelection(ranked_stocks_path, n, sector_portfolio_path)
		sector_portfolio["sector"] = sector
		if inter_sector_weight == "equal":
			sector_portfolio["Weight"] = sector_portfolio["Weight"]/n_sectors
		elif type(inter_sector_weight) == dict:
			sector_portfolio["Weight"] = sector_portfolio["Weight"] * inter_sector_weight[sector]
		else:
			raise Exception("Unexprected type of inter_sector_weight")

		if full_portfolio is None:
			full_portfolio = sector_portfolio
		else:
			full_portfolio = full_portfolio.append(sector_portfolio)

	full_portfolio.to_csv(full_portfolio_path, index = False)
	return full_portfolio


