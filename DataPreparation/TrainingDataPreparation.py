from .Filter import StockFiltering
from .TargetLabelGeneration import TargetLabelGeneration
from .DataPreprocessing import DataPreprocessing

def TrainingDataPreparation(stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, preprocessing_file):
    #Filter the stock data
    if stock_filter_flag:
        stock_data = StockFiltering(stock_data, 'train', preprocessing_file)

    #Generate target label
    stock_data = TargetLabelGeneration(stock_data, B_top, B_bottom, target_label_holding_period)

    #Perform data preprocessing
    stock_data = DataPreprocessing('train', stock_data, preprocessing_file = preprocessing_file)

    return stock_data