from .Filter import StockFiltering
from .TargetLabelGeneration import TargetLabelGeneration
from .ValidationSetSplit import ValidationSetSplit

def ValidationDataPreparation(stock_data, stock_filter_flag, B_top, B_bottom, target_label_holding_period, period = None, date = None):
    #Filter the stock data
    if stock_filter_flag:
        stock_data = StockFiltering(stock_data)

    #Generate target label
    stock_data = TargetLabelGeneration(stock_data, B_top, B_bottom, target_label_holding_period)

    #Split the stock data into train/validate pairs
    try:
        stock_data = ValidationSetSplit(stock_data, period = period, date = date)
    except:
        raise

    return stock_data