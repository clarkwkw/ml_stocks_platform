from concurrent.futures import ThreadPoolExecutor
import filling
import json
import multiprocessing
import numpy as np
import pandas
import traceback
from utilities import print_status, mysql_connection

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'

raw_table = 'bloomberg_raw'
target_table = 'parsed_data'
tickers_json = "./test_tickers.json"
fields_json = "./fields.json"
max_thread_no = 4

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

def fill_by_ticker_and_save(ticker, sector, mysql_conn):
	try:
		global progress
		sql_query = "SELECT date, field, value FROM %s WHERE ticker = '%s' ORDER BY date asc;"%(raw_table, ticker)
		conn_mutex.acquire()
		raw_df = pandas.read_sql(sql_query, mysql_conn, coerce_float = False, parse_dates = ["date"])
		conn_mutex.release()
		parsed_df = new_parsed_df(ticker, raw_df.date.unique(), sector)
		for index, row in raw_df.iterrows():
			date = row['date']
			target_field = direct_fields[row['field']]
			val = row['value']
			parsed_df.loc[date, target_field] = val
		parsed_df = filling.fill_direct_prev(parsed_df, direct_fields.values())
		parsed_df = filling.fill_indirect(parsed_df, indirect_fields)
		conn_mutex.acquire()
		#parsed_df.to_sql(target_table, mysql_conn, if_exists = 'append', index = False)
		parsed_df.to_csv(ticker+'.csv', na_rep = 'nan')
		conn_mutex.release()
		progress_mutex.acquire()
		progress += 1
		progress_mutex.release()
		print_status("Processed %d/%d tickers"%(progress, tickers_count))
	except Exception as e:
		traceback.print_exc()

if __name__ == '__main__':
	mysql_conn = mysql_connection(host, database, username)
	conn_mutex = multiprocessing.Lock()
	tickers_count = 0
	progress = 0
	progress_mutex = multiprocessing.Lock()
	for sector in tickers_table.keys():
		tickers_count += len(tickers_table[sector])
	print_status("Firing request for %d tickers..."%tickers_count)
	with ThreadPoolExecutor(max_workers = max_thread_no) as executor:
		for sector in tickers_table.keys():
			for ticker in tickers_table[sector]:
				executor.submit(fill_by_ticker_and_save, ticker, sector, mysql_conn)
print_status("Done.")