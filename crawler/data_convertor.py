from utilities import print_status, mysql_connection
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
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
fields_json = "./fields.json"
thread_no = 10

print_status("Loading json settings...")
with open(tickers_json) as tickers_file:
	tickers_table = json.load(tickers_file)
with open(fields_json) as fields_file:
	fields_table = json.load(fields_file)

id_fields = ['date', 'ticker', 'sector']
direct_fields = fields_table['direct_fields']
indirect_fields = fields_table['indirect_fields']

def new_parsed_df(ticker, dates, sector):
	data = {}
	data['date'] = dates
	data['ticker'] = ticker
	data['sector'] = sector
	for field in list(direct_fields.values())+list(indirect_fields.keys()):
		data[field] = np.NAN
	df = pandas.DataFrame(data, index = dates, columns = id_fields+list(direct_fields.values())+list(indirect_fields.keys()))
	return df

def parallel_fill_and_save(raw_df, ticker, sector, mutex, mysql_conn):
	parsed_df = new_parsed_df(ticker, raw_df.date.unique(), sector)
	for index, row in raw_df.iterrows():
		date = row['date']
		target_field = direct_fields[row['field']]
		val = row['value']
		parsed_df.loc[date, target_field] = val
	print_status("Filling direct values")
	parsed_df = filling.fill_direct_prev(parsed_df, list(direct_fields.values()))
	print_status("Filling indirect values")
	parsed_df = filling.fill_indirect(parsed_df, indirect_fields)
	print_status("Filled direct and indirect values")
	mutex.acquire()
	#parsed_df.to_sql(target_table, mysql_conn, if_exists = 'append', index = False)
	parsed_df.to_csv(ticker+'.csv', na_rep = 'nan')
	mutex.release()

mysql_conn = mysql_connection(host, database, username)

with ThreadPoolExecutor(max_workers = thread_no) as executor:
	mutex = multiprocessing.Lock()
	parallel_results = []
	for sector in tickers_table.keys():
		for ticker in tickers_table[sector]:
			sql_query = "SELECT date, field, value FROM %s WHERE ticker = '%s' ORDER BY date asc;"%(raw_table, ticker)
			raw_df = pandas.read_sql(sql_query, mysql_conn, coerce_float = False, parse_dates = ["date"])
			executor.submit(parallel_fill_and_save, raw_df, ticker, sector, mutex, mysql_conn)