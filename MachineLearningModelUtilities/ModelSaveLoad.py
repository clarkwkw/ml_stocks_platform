from DataPreparation import TrainingDataPreparation
import json
from utils import get_factors_from_df, import_custom_module, seperate_factors_target
from SimpleNNModel import SimpleNNModel
from SimpleSVMModel import SimpleSVMModel

def buildModel(model_flag, preprocessing_file, stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, customized_module_dir = "", **kwargs):
	train_data = TrainingDataPreparation(stock_data, stock_filter_flag = stock_filter_flag, B_top = B_top, B_bottom = B_bottom, target_label_holding_period = target_label_holding_period, preprocessing_file = preprocessing_file)
	train_factors, target = seperate_factors_target(train_data)
	factors = get_factors_from_df(train_factors)
	if model_flag == "SVM":
		model  = SimpleSVMModel(_factors = factors, **kwargs)
	elif model_flag == "NN":
		model = SimpleNNModel(_factors = factors, **kwargs)
	elif model_flag == "Customized":
		custom_module = import_custom_module("CustomizedModel", customized_module_dir)
		model = custom_module.Model(_factors = factors, **kwargs)
	else:
		raise Exception("Unexpected model_flag '%s'"%str(model_flag))
	model.train(train_factors, target)
	return model

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