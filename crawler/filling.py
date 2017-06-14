import numpy as np
import pandas
import re
from utilities import print_status

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

def fill_complex(field):
	return

def fill_complex_accural_bal_sheet(date, val):
	return

def fill_complex_accural_cash_flow(date, val):
	return

def fill_complex_financial_health(date, val):
	return