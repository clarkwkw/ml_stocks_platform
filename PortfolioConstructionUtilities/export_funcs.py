import MachineLearningModelUtilities as MLUtils
import DataPreparation

def StockPerformancePrediction(stock_filter_flag, preprocessing_file, model_savedir, predict_value_file):
	test_dataset = DataPreparation.TestingDataPreparation(stock_filter_flag = stock_filter_flag, preprocessing_file = preprocessing_file)

def LearnedModelExecution(test_dataset, model_savedir):
	model = MLUtils.loadTrainedModel(model_savedir)
	model.predict(test_dataset)

