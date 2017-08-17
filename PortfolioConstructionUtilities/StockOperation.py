import utils
import DataPreparation
import pandas
import numpy as np
from ModelOperation import *

# buying_price: a dataframe containing Ticker and Buying Price columns
def StockPerformancePrediction(stock_data, buying_price, stock_filter_flag, preprocessing_file, model_savedir, predict_value_file, trained_model = None):
	test_dataset = DataPreparation.TestingDataPreparation(stock_data, stock_filter_flag = stock_filter_flag, preprocessing_file = preprocessing_file)
	predict_df = LearnedModelExecution(test_dataset, model_savedir, model = trained_model)
	predict_df = utils.fill_df(predict_df, "buying_price", buying_price, "price", "ticker")
	predict_df.dropna(subset=['buying_price'],inplace=True)
	predict_df = predict_df.rename(columns = {'pred':'predicted_value'})
	predict_df.to_csv(predict_value_file, index = False)

def StockRanking(stock_file, ranked_stock_file):
	df = pandas.read_csv(stock_file)
	df.sort_values(['predicted_value'], ascending = False, inplace = True)
	df.to_csv(ranked_stock_file, index = False)

def StockSelection(ranked_stock_file, n, portfolio_file, weight_method = "equal"):
	ranked_stocks = pandas.read_csv(ranked_stock_file)
	n_stocks = ranked_stocks.shape[0]
	if n_stocks <= 0:
		raise Exception("No stock to select.")
	if n_stocks < 2*n:
		utils.raise_warning("Not enough stocks to select, n = %d, found %d"%(n, n_stocks))

	short_index, long_index = None, None
	# If 2n > n_stocks >= n, long_index may overlap with short_index
	# In this case, long position would be in higher priority
	# If n > n_stocks, all stock will be in long position
	if n_stocks >= n:
		short_index = (n_stocks - n, n_stocks - 1)
		long_index = (0, n - 1)
	else:
		long_index = (0, n_stocks - 1)

	ranked_stocks.loc[:, "position"] = ""
	if short_index is not None:
		ranked_stocks.loc[short_index[0]:short_index[1], "position"] = "short"
	ranked_stocks.loc[long_index[0]:long_index[1], "position"] = "long"

	selected_indices = ranked_stocks["position"] != ""
	selected_stocks = None
	if weight_method == "equal":
		selected_stocks = utils.set_equal_weights(ranked_stocks, selected_indices)
	else:
		raise Exception("Unexpected weight_method '%s'"%str(weight_method))

	selected_stocks[selected_indices].to_csv(portfolio_file, index = False)

	return selected_stocks[selected_indices]