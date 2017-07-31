import config
import debug
from DataPreparation import ValidationDataPreparation, DataPreprocessing
from SimpleSVMModel import SimpleSVMModel
from SimpleNNModel import SimpleNNModel
from utils import get_factors_from_df, import_custom_module, seperate_factors_target
from concurrent.futures import ThreadPoolExecutor, as_completed
from ModelEvaluation import evaluateModel

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

	dataset = ValidationDataPreparation(stock_data, stock_filter_flag = stock_filter_flag, B_top = B_top, B_bottom = B_bottom, target_label_holding_period = target_label_holding_period, period = period, date = date)

	for i in range(len(dataset)):
		dataset[i] = DataPreprocessing(flag = "validate", stock_data = dataset[i][0], validate_data = dataset[i][1])

	best_meta_para = None
	best_quality = float('-inf')

	unallocated_para_id = 0
	unique_id = 0
	avg_quality_list = []
	n_folds = len(dataset)
	with ThreadPoolExecutor(max_workers = config.max_thread) as executor:
		futures = []
		for meta_para in paras_set:
			avg_quality_list.append(0)
			for train_data, valid_data in dataset:
				futures.append(executor.submit(__selectMetaparameters_helper, unique_id, unallocated_para_id, train_data, valid_data, Model_class, trading_stock_quantity, para_tune_holding_flag, **meta_para))
				unique_id += 1
			unallocated_para_id += 1

		debug.log("MetaparameterSelection: %d meta-parameter(s) with %d fold(s) each."%(unallocated_para_id, n_folds))
		done = 0
		for future in as_completed(futures):
			quality, para_id, unique_id = future.result()
			avg_quality_list[para_id] += 1.0/n_folds*quality
			done += 1
			debug.log("MetaparameterSelection: Done %d [id = %d]."%(done, unique_id))

	for i in range(len(avg_quality_list)):
		if avg_quality_list[i] > best_quality:
			best_quality = avg_quality_list[i]
			best_meta_para = paras_set[i]

	return best_meta_para

def __selectMetaparameters_helper(unique_id, para_id, train_data, valid_data, Model_class, trading_stock_quantity, para_tune_holding_flag, **meta_para):
	train_factors, train_target = seperate_factors_target(train_data)
	factors = get_factors_from_df(train_factors)

	model = Model_class(_factors = factors, **meta_para)
	model.train(train_factors, train_target)

	quality = evaluateModel(model, valid_data, trading_stock_quantity, para_tune_holding_flag)
	return (quality, para_id, unique_id)
