from DataPreparation import TrainingDataPreparation, ValidationDataPrepatation
from MLUtils import get_factors_from_df, import_custom_module
from SimpleNNModel import SimpleNNModel
from SimpleSVMModel import SimpleSVMModel
import imp

def buildModel(model_flag, preprocessing_file, stock_filter_flag, B, target_label_holding_period, customized_module_name = "", customized_module_dir = "", **kwargs):
	train_data = TrainingDataPreparation(stock_filter_flag = stock_filter_flag, B = B, preprocessing_file = preprocessing_file, target_label_holding_period = target_label_holding_period)
	factors = get_factors_from_df(train_data)
	if model_flag == "SVM":
		model  = SimpleNNModel(_factors = factors, **kwargs)
	elif model_flag == "NN":
		model = SimpleNNModel(_factors = factors, **kwargs)
	elif model_flag == "Customized":
		custom_module = _import_custom_module(customized_module_name, customized_module_dir)
		model = custom_module.Model(_factors = factors, **kwargs)
	else:
		raise Exception("Unexpected model_flag '%s'"%str(model_flag))
	model.train(train_data)
	return model

def selectMetaparameters(model_flag, paras_set = [], stock_filter_flag, B, target_label_holding_period, trading_stock_quantity, para_tune_holding_flag, customized_module_name = "", customized_module_dir = ""):
	if model_flag == "SVM":
		Model_class = SimpleSVMModel
	elif model_flag == "NN":
		Model_class = SimpleNNModel
	elif model_flag == "Customized":
		custom_module = _import_custom_module(customized_module_name, customized_module_dir)
		Model_class = custom_module.Model
		paras_set = custom_module.metaparas_set
	else:
		raise Exception("Unexpected model_flag '%s'"%str(model_flag))

	train_data, valid_data = ValidationDataPrepatation(stock_filter_flag, B, target_label_holding_period)
	factors = get_factors_from_df(train_data)

	for meta_para in paras_set:
		model = Model_class(_factors = factors, **meta_para)
	