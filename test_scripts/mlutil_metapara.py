import pandas
import MachineLearningModelUtilities as MLUtils
import DataPreparation as DataPrep

model_savedir = "./test_output/"
svm_paras = [{"kernel": "linear"}, {"kernel": "poly"}, {"kernel": "rbf"}, {"kernel": "sigmoid"}]
nn_paras = [{"_hidden_nodes": []}, {"_hidden_nodes": [1]}, {"_hidden_nodes": [2]}, {"_hidden_nodes": [3]}, {"_hidden_nodes": [4]}]
def test():
    print("Loading data...")
    stock_data = pandas.read_csv('./test_data/10tickers_wo_sector_fill.csv')
    stock_data.dropna(subset=['last_price'],inplace=True)
    stock_data.fillna(value = 0, inplace = True)

    best_para = MLUtils.selectMetaparameters("SVM", stock_data,  stock_filter_flag = False, B_top = 10, B_bottom = 15, target_label_holding_period = 3, trading_stock_quantity = 10, para_tune_holding_flag = "long", period = 5, paras_set = svm_paras)
    print("SVM:", best_para)

    best_para = MLUtils.selectMetaparameters("NN", stock_data,  stock_filter_flag = False, B_top = 10, B_bottom = 15, target_label_holding_period = 3, trading_stock_quantity = 10, para_tune_holding_flag = "long", period = 5, paras_set = nn_paras)
    print("NN:", best_para)