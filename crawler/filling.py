import datetime
import numpy as np
import pandas
import re
from utilities import print_status

def lambda_with_nan(fun):
	def new_fun(val):
		if pandas.isnull(val):
			return np.NAN
		else:
			return fun(val)
	return new_fun

def fill_direct_prev(df, fields):
	prev_vals = {}
	for field in fields:
		fill_prev = fill_direct_prev_factory(field, prev_vals)
		df[field] = df[field].apply(fill_prev)
	return df

# Given a field 'field' and a dictionary 'prev_vals'
# Generates a function which	1. takes 'val' as parameter
#								2. stores 'val' value into 'prev_vals' if 'val' is non-na
# 								3. returns the most recent non-na value if 'val' is na
def fill_direct_prev_factory(field, prev_vals):
	prev_vals[field] = np.NAN
	def fill_prev(val):
		if not pandas.isnull(val):
			prev_vals[field] = val
			return val
		elif not pandas.isnull(prev_vals[field]):
			return prev_vals[field]
		else:
			return np.NAN
	return fill_prev

# Given a dataframe 'df' and a dictionary 'indirect_table' specifying calculation of indirect fields,
# Calculate the values of indirect fields accordingly
def fill_indirect(df, indirect_table):
	for field in indirect_table.keys():
		method = indirect_table[field]
		try:
			compiled_method = re.sub(r"%([^%]*)%", r"df['\1']", method)
			df[field] = eval(compiled_method)
		except Exception as e:
			print_status("Erroneous instruction for indirect field '%s':\n\t%s"%(field, method))
	return(df)

def fill_complex(df):
	for func in _fill_complex_funcs:
		df = func(df)
	return df

def fill_complex_accural_bal_sheet(df):
	queue = DateQueue()
	df["accural_bal_sheet"] = np.NAN
	for itertuple in df.iterrows():
		index = itertuple[0]
		row = itertuple[1]
		cur_date = row['date']
		yr_prev_date = queue.pop(cur_date)
		queue.push(cur_date)
		if yr_prev_date is not None:
			yr_prev_row = df.loc[yr_prev_date]
			df.loc[index, 'accural_bal_sheet'] = row['snapshot_accurual'] - yr_prev_row['snapshot_accurual']
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
		yr_prev_date = queue.pop(cur_date)
		queue.push(cur_date)
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
	df['f_score'] = F_accrual + F_ROA + F_CFO + F_dROA + F_dmargin + F_dturn + F_dlever + F_dcurrent
	
	df.drop(["ROA", "CFO", "gross_margin", "turn", "leverage", "current_ratio", "d_ROA", "d_margin", "d_leverage", "d_current_ratio", "d_turn"], axis = 1, inplace = True)
	return df

_fill_complex_funcs = [fill_complex_accural_bal_sheet, fill_complex_financial_health]

class DateQueue:
	def __init__(self, interval = 365):
		self.queue = []
		self.interval = interval
	def pop(self, cur_date):
		queue = self.queue
		while len(queue) > 1 and (cur_date - queue[1]).days >= self.interval and (cur_date - queue[0]).days >= self.interval:
			queue.pop(0)
		if len(queue) == 0 or (cur_date - queue[0]).days < self.interval:
			return None
		return queue[0]
	def push(self, date):
		self.queue.append(date)