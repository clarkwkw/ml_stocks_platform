import numpy as np
import pandas
import re
from utilities import print_status, keep_fields
from filling_complex import *

_fill_complex_funcs = [	
	fill_complex_mean_factory("earning_mom_1m_mean_fy1_fy2", ["earning_mom_1m_fy1", "earning_mom_1m_fy2"]),
	fill_complex_mean_factory("earning_mom_3m_mean_fy1_fy2", ["earning_mom_3m_fy1", "earning_mom_3m_fy2"]),
	fill_complex_mean_factory("earning_mom_mean_1m_3m", ["earning_mom_1m_mean_fy1_fy2", "earning_mom_3m_mean_fy1_fy2"]),
	fill_complex_earning_mom_3m_fy1_to_fy2,
	fill_complex_financial_health
]

_fill_complex_vect_funcs = [
	fill_complex_interval_change_factory("accrual_bal_sheet", "snapshot_accrual", 365, "absolute"),
	fill_complex_interval_change_factory("target_price_6m_change", "best_target_price", 365, "percent"),
	fill_complex_interval_change_factory("asset_turnover_12m_change", "asset_turnover", 365, "percent"),
	fill_complex_interval_change_factory("earning_mom_1m_fy1", "best_eps_fy1", 30, "percent"),
	fill_complex_interval_change_factory("earning_mom_1m_fy2", "best_eps_fy2", 30, "percent"),
	fill_complex_interval_change_factory("earning_mom_3m_fy1", "best_eps_fy1", 90, "percent"),
	fill_complex_interval_change_factory("earning_mom_3m_fy2", "best_eps_fy2", 90, "percent"),
	fill_complex_interval_change_factory("share_out_12m_change", "total_shares", 365, "percent"),
	fill_complex_interval_change_factory("mom_3m", "last_price", 90, "absolute"),
	fill_complex_interval_change_factory("mom_1m", "last_price", 30, "absolute"),
	fill_complex_interval_change_factory("vol_change_3m", "volume", 90, "absolute"),
	fill_complex_interval_change_factory("vol_change_1m", "volume", 30, "absolute"),
	fill_complex_interval_change_factory("price_accel_3m", "raw_beta", 90, "percent"),
	fill_complex_interval_change_factory("price_accel_6m", "raw_beta", 180, "percent"),
	fill_complex_interval_change_factory("rec_1m_change", "rec_consenus", 30, "percent"),
	fill_complex_interval_change_factory("rec_3m_change", "rec_consenus", 90, "percent"),
	fill_complex_resist_levels_factory()
]

def fill_complex(df):
	for func in _fill_complex_vect_funcs:
		df = df.apply(func, axis = 1)
	for func in _fill_complex_funcs:
		df = func(df)
	return df

def fill_direct_prev(df, fields, skip_keep_fields = True):
	prev_vals = {}
	for field in fields:
		if skip_keep_fields and field in keep_fields:
			continue
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