import datetime
import numpy as np
import pandas

class DateQueue:
	def __init__(self, interval = 365):
		self.queue = []
		self.interval = interval
	def pop(self, cur_date):
		queue = self.queue
		while len(queue) > 1 and (cur_date - queue[1][0]).days >= self.interval and (cur_date - queue[0][0]).days >= self.interval:
			queue.pop(0)
		if len(queue) == 0 or (cur_date - queue[0][0]).days < self.interval:
			return None
		return queue[0]
	def push(self, date, val):
		self.queue.append((date, val))

def fill_complex_interval_change_factory(complex_name, src_name, interval, change_method):
	queue = DateQueue(interval)
	def fill(row):
		cur_date = row["date"]
		prev_tup = queue.pop(cur_date)
		if prev_tup is not None:
			prev_val = prev_tup[1]
			if change_method == 'absolute':
				row[complex_name] = row[src_name] - prev_val
			elif change_method == 'percent':
				row[complex_name] = (1.0*row[src_name]/prev_val - 1)*100
			elif change_method == 'relative':
				row[complex_name] = 1.0*row[src_name]/prev_val - 1
			else:
				raise Exception("Invalid change_method '%s'"%change_method)
		queue.push(cur_date, row[src_name])
		return row
	return fill

def fill_complex_mean_factory(complex_name, src_names, weights = None):
	def fill(df):
		n = float(len(src_names))
		df[complex_name] = df[src_names[0]]/n
		for i in range(1, len(src_names)):
			df[complex_name] += df[src_names[i]]/n
		return df
	return fill

def fill_complex_earning_mom_3m_fy1_to_fy2(df):
	df["earning_mom_3m_fy1_to_fy2"] = df["earning_mom_3m_mean_fy1_fy2"]/df["earnings_cv_mean"]
	return df

def fill_complex_financial_health(df):
	df['ROA'] = np.NAN
	df['CFO'] = np.NAN
	df['gross_margin'] = df['gross_profit'] / df['total_revenue']
	df['turn'] = np.NAN
	df['leverage'] = np.NAN
	df['current_ratio'] = df['total_current_assets'] / df['total_current_liabilities']

	df['d_ROA'] = np.NAN
	df['d_margin'] = np.NAN
	df['d_turn'] = np.NAN
	df['d_leverage'] = np.NAN
	df['d_current_ratio'] = np.NAN
	queue = DateQueue()
	for itertuple in df.iterrows():
		index = itertuple[0]
		row = itertuple[1]
		cur_date = row['date']
		yr_begin_date = datetime.datetime(cur_date.year, 1, 1, 0, 0)

		ROA, CFO, turn, leverage = (np.NAN, np.NAN, np.NAN, np.NAN)
		try:
			yr_begin_index = df.index.get_loc(yr_begin_date, method = 'backfill')
			yr_begin_row = df.iloc[yr_begin_index]
			ROA = row['net_income_before_xo_items'] / yr_begin_row['total_assets']
			CFO = row['cash_from_operating'] / yr_begin_row['total_assets']
			turn = row['total_revenue'] / yr_begin_row['total_assets']
			leverage = row['total_long_term_debt'] / yr_begin_row['total_assets']
			df.loc[index,'ROA'] = ROA
			df.loc[index,'turn'] = turn
			df.loc[index,'CFO'] = CFO
			df.loc[index,'leverage'] = leverage
		except KeyError:
			pass
		yr_prev_tup = queue.pop(cur_date)
		yr_prev_date = None
		if yr_prev_tup is not None:
			yr_prev_date = yr_prev_tup[0]
		queue.push(cur_date, None)
		if yr_prev_date is not None:
			yr_prev_row = df.loc[yr_prev_date]
			df.loc[index, 'd_ROA'] = ROA - yr_prev_row['ROA']
			df.loc[index, 'd_margin'] = row['gross_margin'] - yr_prev_row['gross_margin']
			df.loc[index, 'd_turn'] = turn - yr_prev_row['turn']
			df.loc[index, 'd_leverage'] = leverage - yr_prev_row['leverage']
			df.loc[index, 'd_current_ratio'] = row['current_ratio'] - yr_prev_row['current_ratio']

	F_accrual = (df['ROA'] - df['CFO']).apply(lambda_with_nan(lambda x: x > 0))
	F_ROA = df['ROA'].apply(lambda_with_nan(lambda x: x > 0))
	F_CFO = df['CFO'].apply(lambda_with_nan(lambda x: x > 0))
	F_dROA = df['d_ROA'].apply(lambda_with_nan(lambda x: x > 0))
	F_dmargin = df['d_margin'].apply(lambda_with_nan(lambda x: x > 0))
	F_dturn = df['d_turn'].apply(lambda_with_nan(lambda x: x > 0))
	F_dlever = df['d_leverage'].apply(lambda_with_nan(lambda x: x < 0))
	F_dcurrent = df['d_current_ratio'].apply(lambda_with_nan(lambda x: x > 0))
	df['financial_health'] = F_accrual + F_ROA + F_CFO + F_dROA + F_dmargin + F_dturn + F_dlever + F_dcurrent
	
	return df

def fill_complex_resist_levels_factory():
	prev_val = {'init':False}
	def fill(row):
		if prev_val['init']:
			pivot = (prev_val['high'] + prev['low'] + prev['last_price'])/3.0
			row['resist_level1'] = 2*pivot - prev_val['low']
			row['resist_level2'] = pivot + prev_val['high'] - prev_val['low']
			row['resist_level3'] = prev_val['high'] + 2*(pivot - prev_val['low'])
		prev_val['init'] = row['init']
		prev_val['high'] = row['high']
		prev_val['low'] = row['low']
		prev_val['last_price'] = row['last_price']
		return row
	return fill

def lambda_with_nan(fun):
	def new_fun(val):
		if pandas.isnull(val):
			return np.NAN
		else:
			return fun(val)
	return new_fun