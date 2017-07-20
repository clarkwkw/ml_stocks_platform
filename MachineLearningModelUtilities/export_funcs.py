from DataPreparation import TrainingDataPreparation, ValidationDataPreparation, DataPreprocessing
import json
import pandas
from utils import get_factors_from_df, import_custom_module, seperate_factors_target, raise_warning
from SimpleNNModel import SimpleNNModel
from SimpleSVMModel import SimpleSVMModel
import imp

# Setting __test_flag as True will:
# 1. Fill NA values with zero in stock_data
__test_flag = True

def buildModel(model_flag, preprocessing_file, stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, customized_module_name = "", customized_module_dir = "", **kwargs):
	train_data = TrainingDataPreparation(stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, preprocessing_file)
	if __test_flag:
		train_data.fillna(value = 0, inplace = True)

	train_factors, target = seperate_factors_target(train_data)
	factors = get_factors_from_df(train_factors)
	if model_flag == "SVM":
		model  = SimpleSVMModel(_factors = factors, **kwargs)
	elif model_flag == "NN":
		model = SimpleNNModel(_factors = factors, **kwargs)
	elif model_flag == "Customized":
		custom_module = import_custom_module(customized_module_name, customized_module_dir)
		model = custom_module.Model(_factors = factors, **kwargs)
	else:
		raise Exception("Unexpected model_flag '%s'"%str(model_flag))
	model.train(train_factors, target)
	return model

def selectMetaparameters(model_flag, stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, trading_stock_quantity, para_tune_holding_flag, period = None, date = None, customized_module_dir = "", paras_set = []):
	if model_flag == "SVM":
		Model_class = SimpleSVMModel
	elif model_flag == "NN":
		Model_class = SimpleNNModel
	elif model_flag == "Customized":
		custom_module = import_custom_module("CustomizedModel", customized_module_dir)
		Model_class = custom_module.Model
		paras_set = custom_module.metaparas_set
	else:
		raise Exception("Unexpected model_flag '%s'"%str(model_flag))

	dataset = ValidationDataPrepatation(stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, period, date)

	for i in range(len(dataset)):
		dataset[i] = DataPreprocessing(flag = "validate", stock_data = dataset[i][0], validate_data = dataset[i][1])

	best_meta_para = None
	best_quality = float('-inf')
	for meta_para in paras_set:
		n_folds = len(dataset)
		avg_quality = 0
		for train_data, valid_data in dataset:
			train_factors, train_target = seperate_factors_target(train_data)
			factors = get_factors_from_df(train_factors)

			model = Model_class(_factors = factors, **meta_para)
			model.train(train_factors, train_target)

			quality = evaluateModel(model, valid_data, trading_stock_quantity, para_tune_holding_flag)
			avg_quality += 1.0/n_folds*quality
		if avg_quality > best_quality:
			best_quality = avg_quality
			best_meta_para = meta_para

	return meta_para

def evaluateModel(trained_model, valid_data, trading_stock_quantity, para_tune_holding_flag):
	if __test_flag:
		valid_data.fillna(value = 0, inplace = True)

	valid_factors, _ = seperate_factors_target(valid_data)

	pred_target = trained_model.predict(valid_factors)

	return calculateQuality(valid_data, pred_target, trading_stock_quantity, para_tune_holding_flag)

def calculateQuality(valid_data, pred_target, trading_stock_quantity, para_tune_holding_flag, alpha = 0.5):
	df_analysis = pandas.concat([pred_target, valid_data['return']], axis = 1)
	intra_day_quality = df_analysis.groupby('date').apply(__intraday_quality, para_tune_holding_flag, trading_stock_quantity)
	overall_quality = intra_day_quality[0]
	for i in range(1, len(intra_day_quality)):
		overall_quality = alpha*overall_quality + (1-alpha) * intra_day_quality[i]
	return overall_quality

def loadTrainedModel(savedir):
	with open(savedir+'/model.conf') as f:
		conf_file = json.load(f)
		model_type = conf_file["model_type"]
	model = None
	if model_type == "NN":
		model = SimpleNNModel.load(savedir)
	elif model_type == "SVM":
		model = SimpleSVMModel.load(savedir)
	elif model_type == "custom":
		custom_module = import_custom_module("CustomizedModel", conf_file["modeldir"])
		model = custom_module.Model.load(savedir)
	return model

def __intraday_quality(df, para_tune_holding_flag, n):
	avg_return = 0
	n_stocks = df.shape[0]
	t = n_stocks
	
	if para_tune_holding_flag == 'long':
		t -= n
	elif para_tune_holding_flag == 'short':
		t -= n
	elif para_tune_holding_flag == 'long_short':
		t -= 2*n
	else:
		raise Exception("Unexpected para_tune_holding_flag '%s'"%str(para_tune_holding_flag))
	if t < 0:
		raise_warning("Insufficient no. of stock (%d) to evaluate on %s\nAll stocks will be considered and long position will be prioritized"%(df.shape[0], df['date'].unique()[0]))

	long_index, short_index = None, None
	if para_tune_holding_flag == 'long' or para_tune_holding_flag == 'long_short':
		long_index = (0, min(n_stocks - 1, n - 1))
	if para_tune_holding_flag == 'short' or para_tune_holding_flag == 'long_short':
		short_index = (max(0, n_stocks - n), n_stocks - 1)

	if para_tune_holding_flag == 'long_short' and long_index[1] >= short_index[0]:
		short_index = (long_index[1] + 1, short_index[1])
		if short_index[0] > short_index[1]:
			short_index = None

	df = df.sort(columns = ['pred'], ascending = False)

	if long_index is not None:
		for i in range(long_index[0], long_index[1] + 1):
			avg_return += 1.0/(2*n)*df.iloc[i]['return']
	if short_index is not None:
		for i in range(short_index[0], short_index[1] + 1):
			avg_return -= 1.0/(2*n)*df.iloc[i]['return']

	return avg_return