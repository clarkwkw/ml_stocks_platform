import MachineLearningModelUtilities as MLUtils
import Utils
import DataPreparation
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

def StockSelection(ranked_stock_file, n, portfolio_file):
	ranked_stocks = pandas.read_csv(ranked_stock_file)
	if ranked_stocks.shape[0] < 2*n:
		Utils.raise_warning("Not enough stocks to select, n = %d, found %d"%(n, ranked_stocks.shape[0]))
	