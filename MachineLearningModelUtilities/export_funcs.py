from DataPreparation import TrainingDataPreparation, ValidationDataPrepatation, DataNormalization
from MLUtils import get_factors_from_df, import_custom_module, seperate_factors_target
from SimpleNNModel import SimpleNNModel
from SimpleSVMModel import SimpleSVMModel
import imp

def buildModel(model_flag, preprocessing_file, stock_filter_flag, B, target_label_holding_period, customized_module_name = "", customized_module_dir = "", **kwargs):
	train_data = TrainingDataPreparation(stock_filter_flag = stock_filter_flag, B = B, preprocessing_file = preprocessing_file, target_label_holding_period = target_label_holding_period)
	train_factors, target = seperate_factors_target(train_data)

	factors = get_factors_from_df(train_factors)
	if model_flag == "SVM":
		model  = SimpleNNModel(_factors = factors, **kwargs)
	elif model_flag == "NN":
		model = SimpleNNModel(_factors = factors, **kwargs)
	elif model_flag == "Customized":
		custom_module = import_custom_module(customized_module_name, customized_module_dir)
		model = custom_module.Model(_factors = factors, **kwargs)
	else:
		raise Exception("Unexpected model_flag '%s'"%str(model_flag))
	model.train(train_factors, target)
	return model

def selectMetaparameters(model_flag, paras_set = [], stock_filter_flag, B, target_label_holding_period, trading_stock_quantity, para_tune_holding_flag, customized_module_name = "", customized_module_dir = ""):
	if model_flag == "SVM":
		Model_class = SimpleSVMModel
	elif model_flag == "NN":
		Model_class = SimpleNNModel
	elif model_flag == "Customized":
		custom_module = import_custom_module(customized_module_name, customized_module_dir)
		Model_class = custom_module.Model
		paras_set = custom_module.metaparas_set
	else:
		raise Exception("Unexpected model_flag '%s'"%str(model_flag))

	dataset = ValidationDataPrepatation(stock_filter_flag, B, target_label_holding_period)

	for i in range(len(dataset)):
		dataset[i][0], dataset[i][1] = DataPreprocessing(dataset[i][0], dataset[i][1], flag = 'validate')

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
	valid_factors, _ = seperate_factors_target(valid_data)

	pred_target = trained_model.predict(valid_factors)

	return calculateQuality(valid_data, pred_target, trading_stock_quantity, para_tune_holding_flag)

def calculateQuality(valid_data, pred_target, trading_stock_quantity, para_tune_holding_flag, alpha = 0.5):
	df_analysis = pandas.concat([pred_target, valid_data['return']], axis = 1)
	intra_day_quality = df_analysis.group_by('date').apply(__intraday_quality, para_tune_holding_flag, trading_stock_quantity)
	overall_quality = intra_day_quality[0]
	for i in range(1, len(intra_day_quality)):
		overall_quality = alpha*overall_quality + (1-alpha) * intra_day_quality[i]
	return quality

def __intraday_quality(df, para_tune_holding_flag, n):
	tickers_list = []
	avg_return = 0
	for ticker in df['ticker'].unique():
		tickers_list.append((df[df[ticker] == ticker, 'return'], df[df[ticker] == ticker, 'target'] ))
	t = len(tickers_list)
	if para_tune_holding_flag == 'long':
		t -= n
	elif para_tune_holding_flag == 'short':
		t -= n
	elif para_tune_holding_flag == 'long_short':
		t -= 2*n
	else:
		raise Exception("Unexpected para_tune_holding_flag '%s'"%str(para_tune_holding_flag))
	if t < 0:
		raise Exception("Insufficient no. of stock to evaluate on %s"%(df['date'].unique()[0]))

	# sort by target
	tickers_list = sorted(tickers_list, key = lambda x: x[1], reverse = True)
	for i in range(n):
		# long stock with high target
		if para_tune_holding_flag == 'long' or para_tune_holding_flag == 'long_short':
			avg_return += 1.0/(2*n)*tickers_list[i][0]
		# short stock with low target
		if para_tune_holding_flag == 'short' or para_tune_holding_flag == 'long_short':
			avg_return -= 1.0/(2*n)*tickers_list[n-i-1][0]

	return avg_return