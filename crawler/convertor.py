from concurrent.futures import ThreadPoolExecutor, as_completed
import filling
import json
import multiprocessing
import numpy as np
import pandas
import schedule
import threading
import time
from tqdm import tqdm
import traceback
import utilities

print_status = utilities.print_status

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'

raw_table = 'US_bloomberg_factor'
target_table = 'US_machine_learning_factor'
last_date = "2016-12-31"
max_thread_no = 4

email_status_dest = "clarkwkw@yahoo.com.hk"
email_status_freq = 60

errs = []
err_tickers = []
err_mutex = multiprocessing.Lock()
tickers_count = 0
finished_count = 0
exit_flag = False

direct_parsed_fields = utilities.direct_parsed_fields()
indirect_parsed_fields = utilities.indirect_parsed_fields()
complex_fields = utilities.complex_fields
id_fields = ['date', 'ticker', 'sector']

def select_tickers(db_ticker, all_tickers):
	ticker_dict = {}
	exclude_count = 0
	for ticker in all_tickers:
		ticker_dict[ticker] = True
	for ticker in db_ticker:
		if ticker in ticker_dict:
			del ticker_dict[ticker]
			exclude_count += 1
	print_status("Excluded %d tickers"%exclude_count)
	return list(ticker_dict)

def new_parsed_df(ticker, dates, sector):
	data = {}
	data['date'] = dates
	data['ticker'] = ticker
	data['sector'] = sector
	for field in direct_parsed_fields + indirect_parsed_fields + complex_fields:
		data[field] = np.NAN
	df = pandas.DataFrame(data, index = dates, columns = id_fields + direct_parsed_fields + indirect_parsed_fields + complex_fields)
	return df

def fill_by_ticker_and_save(ticker, sector, mysql_conn, download_selected_only = True):
	try:
		global progress
		if download_selected_only:
			fields_str = "AND field IN ('"+"','".join(direct_parsed_fields)+"')"
		else:
			fields_str = ""
		sql_query = "SELECT date, field, value FROM %s WHERE ticker = '%s' %s ORDER BY date asc;"%(raw_table, ticker, fields_str)
		conn_mutex.acquire()
		raw_df = pandas.read_sql(sql_query, mysql_conn, coerce_float = False, parse_dates = ["date"])
		conn_mutex.release()
		parsed_df = new_parsed_df(ticker, raw_df.date.unique(), sector)
		for index, row in raw_df.iterrows():
			date = row['date']
			field = row['field']
			val = row['value']
			if download_selected_only or field in direct_parsed_fields:
				parsed_df.loc[date, field] = val
		parsed_df = filling.fill_direct_prev(parsed_df, direct_parsed_fields)
		parsed_df = filling.fill_indirect(parsed_df, utilities.indirect_fields_table)
		parsed_df = filling.fill_complex(parsed_df)
		parsed_df = parsed_df[id_fields+utilities.ml_fields()]
		parsed_df.replace([np.inf, -np.inf], np.nan, inplace = True)
		conn_mutex.acquire()
		parsed_df.to_sql(target_table, mysql_conn, if_exists = 'append', index = False, chunksize = 100)
		#parsed_df.to_csv(ticker+'.csv', na_rep = 'nan')
		conn_mutex.release()
	except Exception as e:
		traceback.print_exc()
		err_mutex.acquire()
		errs.append(traceback.format_exc())
		err_tickers.append(ticker)
		err_mutex.release()

def send_status():
	global errs
	subject = "Uploader Status Update"
	body = "Progress: %d/%d\n"%(finished_count, tickers_count)
	if len(err_tickers):
		body += "Error occured on these tickers:\n%s"%str(err_tickers)+"\n"
	if len(errs) > 0:
		err_mutex.acquire()
		body += "Error Message of Last %d mins:\n"%email_status_freq
		for errmsg in errs:
			body += '> ' + '\n> '.join(errmsg.rstrip("\n").split("\n")) + '\n'
			body += "--------------------\n"
		errs = []
		err_mutex.release()
	utilities.send_gmail(email_status_dest, subject, body)

def send_status_management():
	try:
		time.sleep(10)
		schedule.every(email_status_freq).minutes.do(send_status).run()
		while not exit_flag:
			schedule.run_pending()
			time.sleep(1)
	except Exception as e:
		traceback.print_exc()

def send_finish_msg():
	subject = "Uploader Status Update"
	body = "Finished uploading, uploader is going to terminate."
	utilities.send_gmail(email_status_dest, subject, body)

if __name__ == '__main__':
	mysql_conn = utilities.mysql_connection(host, database, username)
	conn_mutex = multiprocessing.Lock()
	email_thread = threading.Thread(target = send_status_management)
	email_thread.start()
	tickers_to_crawl = []
	for sector in utilities.tickers_table:
		tickers_to_crawl.extend(utilities.tickers_table[sector])
	old_tickers = pandas.read_sql("SELECT ticker from %s WHERE date = '%s';"%(target_table, last_date), mysql_conn, coerce_float = False)['ticker']
	tickers_to_crawl = select_tickers(old_tickers, tickers_to_crawl)
	tickers_count = len(tickers_to_crawl)

	print_status("Firing request for %d ticker(s)..."%tickers_count)
	futures = []
	with ThreadPoolExecutor(max_workers = max_thread_no) as executor:
		for ticker in tickers_to_crawl:
			futures.append(executor.submit(fill_by_ticker_and_save, ticker, sector, mysql_conn))
		for _ in tqdm(as_completed(futures)):
			finished_count += 1
print_status("Done.")
exit_flag = True
send_finish_msg()