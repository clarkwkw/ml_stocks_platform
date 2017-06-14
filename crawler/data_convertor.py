from utilities import print_status, mysql_connection
import filling
import pandas
import json
import numpy as np

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'

raw_table = 'bloomberg_raw'
target_table = 'parsed_data'
tickers_json = "./test_tickers.json"
fields_json = "./bloomberg_fields_full.json"

print_status("Loading json settings...")
with open(tickers_json) as tickers_file:
	tickers_table = json.load(tickers_file)
with open(fields_json) as fields_file:
	fields_table = json.load(fields_file)

id_fields = ['date', 'ticker', 'sector']
direct_fields = list(fields_table.values())
indirect_fields = ['income_after_tax', 'snapshot_accurual', 'accural_bal_sheet', 'accural_cash_flow', 'financial_health', 'working_capital', 'quick_ratio', 'dividend_payout_ratio', 'book_value_minus_debt', 'receivalbes_to_sales', 'debt_to_assets', 'debt_to_equity', 'cash_to_assets', 'liabilities_to_income', 'return_on_equity', 'sales_per_shares']

def new_parsed_df(ticker, dates, sector):
	data = {}
	data['date'] = dates
	data['ticker'] = ticker
	data['sector'] = sector
	for field in direct_fields+indirect_fields:
		data[field] = np.NAN
	df = pandas.DataFrame(data, index = dates, columns = id_fields+direct_fields+indirect_fields)
	return df

mysql_conn = mysql_connection(host, database, username)

for sector in tickers_table.keys():
	for ticker in tickers_table[sector]:
		sql_query = "SELECT date, field, value FROM %s WHERE ticker = '%s' ORDER BY date asc;"%(raw_table, ticker)
		raw_df = pandas.read_sql(sql_query, mysql_conn, coerce_float = False, parse_dates = ["date"])
		parsed_df = new_parsed_df(ticker, raw_df.date.unique(), sector)
		for index, row in raw_df.iterrows():
			date = row['date']
			target_field = fields_table[row['field']]
			val = row['value']
			parsed_df.loc[date, target_field] = val
		parsed_df = filling.fill_prev(parsed_df, direct_fields)
