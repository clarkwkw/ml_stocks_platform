import numpy as np
import pandas

_prev_vals = {}

def fill_prev(df, fields):
	for field in fields:
		fill_prev = fill_prev_factory(field)
		df[field] = df[field].apply(fill_prev)
	return df

def fill_prev_factory(field):
	_prev_vals[field] = np.NAN
	def fill_prev(val):
		if not pandas.isnull(val):
			_prev_vals[field] = val
			return val
		elif not pandas.isnull(_prev_vals[field]):
			return _prev_vals[field]
		else:
			return np.NAN
	return fill_prev

'''
def income_after_tax(date, income_before_tax, income_tax_expense):
	val = income_before_tax - income_tax_expense
	if pandas.isnull(val) and not pandas.isnull(vals_cache['income_after_tax']['lastval']):
		if 


def snapshot_accurual(date, val):
def accural_bal_sheet(date, val):
def accural_cash_flow(date, val):
def financial_health(date, val):
def working_capital(date, val):
def quick_ratio(date, val):
def dividend_payout_ratio(date, val):
def book_value_minus_debt(date, val):
def receivalbes_to_sales(date, val):
def debt_to_assets(date, val):
def debt_to_equity(date, val):
def cash_to_assets(date, val):
def liabilities_to_income(date, val):
def return_on_equity(date, val):
def sales_per_shares(date, val):
'''
