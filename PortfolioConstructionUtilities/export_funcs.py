import MachineLearningModelUtilities as MLUtils
import utils
import DataPreparation
import numpy as np
import pandas


def StockPerformancePrediction(stock_filter_flag, preprocessing_file, model_savedir, predict_value_file):
	test_dataset = DataPreparation.TestingDataPreparation(stock_filter_flag = stock_filter_flag, preprocessing_file = preprocessing_file)
	predict_df = LearnedModelExecution(test_dataset, model_savedir)
	with open(predict_value_file, "w") as f:
		pass

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
	# If 2n > n_stocks >= n,  long_index may overlap with short_index
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
