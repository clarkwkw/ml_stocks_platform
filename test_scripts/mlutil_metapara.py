import pandas
import MachineLearningModelUtilities as MLUtils
import DataPreparation as DataPrep

model_savedir = "./test_output/"
customized_model_dir = "./test_scripts"
customized_model_name = "Model"

svm_paras = [{"kernel": "linear"}, {"kernel": "poly"}, {"kernel": "rbf"}, {"kernel": "sigmoid"}]
nn_paras = [{"_hidden_nodes": []}, {"_hidden_nodes": [1]}, {"_hidden_nodes": [2]}, {"_hidden_nodes": [3]}, {"_hidden_nodes": [4]}]
nb_paras = [{"alpha": 1}, {"alpha": 1.5}]
def test():
    print("Loading data...")
    stock_data = pandas.read_csv('./test_data/10tickers_wo_sector_fill.csv')
    stock_data.dropna(subset=['last_price'],inplace=True)
    stock_data.fillna(value = 0, inplace = True)

    best_para = MLUtils.selectMetaparameters("Custom", stock_data,  stock_filter_flag = False, B_top = 10, B_bottom = 15, target_label_holding_period = 3, trading_stock_quantity = 10, para_tune_holding_flag = "long", period = 5, paras_set = nb_paras, customized_module_dir = customized_model_dir, customized_module_name = customized_model_name)
    print("Custom [testscript paras]:", best_para)

    best_para = MLUtils.selectMetaparameters("Custom", stock_data,  stock_filter_flag = False, B_top = 10, B_bottom = 15, target_label_holding_period = 3, trading_stock_quantity = 10, para_tune_holding_flag = "long", period = 5, paras_set = "model_def", customized_module_dir = customized_model_dir, customized_module_name = customized_model_name)
    print("Custom [model paras]:", best_para)

    best_para = MLUtils.selectMetaparameters("SVM", stock_data,  stock_filter_flag = False, B_top = 10, B_bottom = 15, target_label_holding_period = 3, trading_stock_quantity = 10, para_tune_holding_flag = "long", period = 5, paras_set = svm_paras)
    print("SVM:", best_para)

    best_para = MLUtils.selectMetaparameters("NN", stock_data,  stock_filter_flag = False, B_top = 10, B_bottom = 15, target_label_holding_period = 3, trading_stock_quantity = 10, para_tune_holding_flag = "long", period = 5, paras_set = nn_paras)
    print("NN:", best_para)