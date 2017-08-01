import debug
import MachineLearningModelUtilities as MLUtils

# ML_sector_factors: a dictionary of sector and corresponding list of factors
def MachineLearningModelDevelopment(ML_sector_factors, ML_model_flag, paras_set, stock_filter_flag, B_top, B_bottom, target_label_holding_period, trading_stock_quantity, para_tune_holding_flag, period = None, date = None, customized_module_dir = ""):
	models_map = {}
	for sector in ML_sector_factors:
		debug.log("MachineLearningModelDevlopment: Developing model for %s sector.."%sector)
		stock_data = ML_sector_factors[sector]
		preprocessing_file_path = "./model/preprocessing/%s_preprocessing_info.json"%sector
		best_para = None
		if type(paras_set) is list:
			debug.log("MachineLearningModelDevlopment: Step 1. Finding best parameters..")
			best_para = MLUtils.selectMetaparameters(ML_model_flag, stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, trading_stock_quantity, para_tune_holding_flag, period = period, date = date, customized_module_dir = customized_module_dir, paras_set = paras_set)
		elif type(paras_set) is dict:
			best_para = paras_set
		else:
			raise Exception("Unexpected type of paras_set %s"%str(type(paras_set)))
		debug.log("MachineLearningModelDevlopment: Best parameter [%s]"%str(best_para))
		debug.log("MachineLearningModelDevlopment: Step 2. Building model..")
		model = MLUtils.buildModel(ML_model_flag, preprocessing_file_path, stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, customized_module_dir = customized_module_dir, **best_para)
		models_map[sector] = model
	return models_map

def LearnedModelExecution(test_dataset, model_savedir = "", model = None):
	if model is None:
		model = MLUtils.loadTrainedModel(model_savedir)
	return model.predict(test_dataset)