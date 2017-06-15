from concurrent.futures import ThreadPoolExecutor
import filling
import json
import multiprocessing
import numpy as np
import pandas
import traceback
from utilities import print_status, mysql_connection, direct_parsed_fields, get_parsed_field, indirect_parsed_fields, indirect_fields_table, tickers_table

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'

raw_table = 'bloomberg_raw'
target_table = 'parsed_data'
max_thread_no = 4

def new_parsed_df(ticker, dates, sector):
	data = {}
	id_fields = ['date', 'ticker', 'sector']
	data['date'] = dates
	data['ticker'] = ticker
	data['sector'] = sector
	for field in direct_parsed_fields() + indirect_parsed_fields():
		data[field] = np.NAN
	df = pandas.DataFrame(data, index = dates, columns = id_fields + direct_parsed_fields() + indirect_parsed_fields())
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
			target_field = get_parsed_field(row['field'])
			val = row['value']
			parsed_df.loc[date, target_field] = val
		parsed_df = filling.fill_direct_prev(parsed_df, direct_parsed_fields())
		parsed_df = filling.fill_indirect(parsed_df, indirect_fields_table)
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