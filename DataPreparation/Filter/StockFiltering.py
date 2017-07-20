from .LIQFilter import LIQFilter
from .DTVFilter import DTVFilter
from  .PriceFilter import PriceFilter

import pandas as pd

def StockFiltering(stock_data):
    #LIQ Filter
    stock_data = LIQFilter(stock_data)

    #DTV and Price Filter
    DTV = DTVFilter(stock_data)
    Price = PriceFilter(stock_data)

    #Merge the result of DTV and Price Filter
    stock_data = pd.merge(DTV, Price, how='inner', on=list(DTV.columns))
    
    return stock_data