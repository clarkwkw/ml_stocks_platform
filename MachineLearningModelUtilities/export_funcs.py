from DataPreparation import TrainingDataPreparation
from MLUtils import id_fields
from SimpleNNModel import SimpleNNModel
from SimpleSVMModel import SimpleSVMModel
import imp

def buildModel(model_flag, preprocessing_file, model_file, customized_module_name, customized_module_dir, stock_filter_flag, B, target_label_holding_period, **kwargs):
	train_data = TrainingDataPreparation(stock_filter_flag = stock_filter_flag, B = B, preprocessing_file = preprocessing_file, target_label_holding_period = target_label_holding_period)
	factors = list(train_data)
	for id_field in id_fields:
		if id_field in factors:
			factors.remove(id_field)
	if model_flag == "SVM":
		model  = SimpleNNModel(_factors = factors, **kwargs)
	elif model_flag == "NN":
		model = SimpleNNModel(_factors = factors, **kwargs)
	elif model_flag == "Customized":
		file, filename, desc = imp.find_module(customized_module_name, path = customized_module_dir)
		CustomizedModelModule = imp.load_module(customized_module_name, file, filename, desc)
		model = CustomizedModelModule.Model(_factors = factors, **kwargs)
	model.train(train_data)
	return model