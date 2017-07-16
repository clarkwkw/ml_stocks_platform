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

def calculateQuality(valid_data, pred_target, trading_stock_quantity, para_tune_holding_flag):
	