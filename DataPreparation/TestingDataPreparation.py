from .Filter import StockFiltering
from .DataPreprocessing import DataPreprocessing

def TestingDataPreparation(stock_data, stock_filter_flag, preprocessing_file):
    #Filter the stock data
    if stock_filter_flag:
        stock_data = StockFiltering(stock_data)

    #Perform data preprocessing
    stock_data = DataPreprocessing('test', stock_data, preprocessing_file = preprocessing_file)
    
    return stock_data