from .LIQFilter import LIQFilter
from .DTVFilter import DTVFilter
from  .PriceFilter import PriceFilter

import pandas as pd
import json

def StockFiltering(stock_data, flag, filterd_stock_file = None):
	if flag == 'train':
		#LIQ Filter
		stock_data = LIQFilter(stock_data)

		#DTV and Price Filter
		DTV = DTVFilter(stock_data)
		Price = PriceFilter(stock_data)

		#Merge the result of DTV and Price Filter
		stock_data = pd.merge(DTV, Price, how='inner', on=list(DTV.columns))

		if filterd_stock_file is not None:
			#Output the Filtered Stock List
			with open(filterd_stock_file.split('_')[0] + '_filtered_stock_list.json','w') as f:
				filtered_stocks = list(stock_data['ticker'].unique())
				json.dump(filtered_stocks,f,indent=4)

	elif flag == 'test':
		try:
			with open(filterd_stock_file.split('_')[0] + '_filtered_stock_list.json','r') as f:
				stock_data = stock_data[stock_data['ticker'].isin(json.load(f))]
		except IOError:
			print('Training Data has not performed Stock Filtering, Testing Data will also omit Stock Filtering.')
			pass
	else:
		raise Exception('Invalid flag for StockFiltering (train/test only)')

	return stock_data