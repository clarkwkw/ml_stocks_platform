import config
import pandas
from utils import seperate_factors_target, raise_warning
import string
import random

def evaluateModel(trained_model, valid_data, trading_stock_quantity, para_tune_holding_flag):
	valid_factors, _ = seperate_factors_target(valid_data)

	pred_target = trained_model.predict(valid_factors)

	return calculateQuality(valid_data, pred_target, trading_stock_quantity, para_tune_holding_flag)

def calculateQuality(valid_data, pred_target, trading_stock_quantity, para_tune_holding_flag, alpha = 0.5):
	df_analysis = pandas.concat([pred_target, valid_data['return']], axis = 1)
	intra_day_quality = df_analysis.groupby('date').apply(__intraday_quality, para_tune_holding_flag, trading_stock_quantity)
	intra_day_quality = intra_day_quality.dropna(inplace = False)
	overall_quality = intra_day_quality.iloc[0]
	for i in range(1, intra_day_quality.shape[0]):
		overall_quality = alpha*overall_quality + (1-alpha) * intra_day_quality.iloc[i]
	return overall_quality

def __intraday_quality(df, para_tune_holding_flag, n):
	avg_return = 0
	n_stocks = df.shape[0]
	stocks_used = 0
	t = n_stocks
	
	if para_tune_holding_flag == 'long':
		t -= n
	elif para_tune_holding_flag == 'short':
		t -= n
	elif para_tune_holding_flag == 'long_short':
		t -= 2*n
	else:
		raise Exception("Unexpected para_tune_holding_flag '%s'"%str(para_tune_holding_flag))
	if t < 0:
		pd_date = pandas.to_datetime(str(df['date'].unique()[0])) 
		date_str = pd_date.strftime(config.date_format)
		raise_warning("Insufficient no. of stock (%d) to evaluate on %s\nAll stocks will be considered and long position will be prioritized"%(df.shape[0], date_str))

	long_index, short_index = None, None
	if para_tune_holding_flag == 'long' or para_tune_holding_flag == 'long_short':
		long_index = (0, min(n_stocks - 1, n - 1))
	if para_tune_holding_flag == 'short' or para_tune_holding_flag == 'long_short':
		short_index = (max(0, n_stocks - n), n_stocks - 1)

	if para_tune_holding_flag == 'long_short' and long_index[1] >= short_index[0]:
		short_index = (long_index[1] + 1, short_index[1])
		if short_index[0] > short_index[1]:
			short_index = None

	if long_index is not None:
		stocks_used += long_index[1] - long_index[0] + 1
	if short_index is not None:
		stocks_used += short_index[1] - short_index[0] + 1

	df = df.sort(columns = ['pred'], ascending = False)

	if long_index is not None:
		avg_return += 1.0/stocks_used*df['return'][long_index[0]:long_index[1]].sum(skipna = True)
	if short_index is not None:
		avg_return += 1.0/stocks_used*df['return'][short_index[0]:short_index[1]].sum(skipna = True)
	return avg_return