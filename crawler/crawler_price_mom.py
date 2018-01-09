from __future__ import print_function
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import pandas
import schedule
import time
from tia.bbg import LocalTerminal
import traceback
import threading
import utilities

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'
raw_table = 'HK2_bloomberg_factor'
out_folder = "./historical data"
first_date = "2008-05-20"

email_status_dest = "clarkwkw@yahoo.com.hk"
email_status_freq = 60

periods = [(1, "bbg_mom_1m"), (3, "bbg_mom_3m"), (6, "bbg_mom_6m"), (12, "bbg_mom_12m")]
max_thread_no = 8
sectors = list(utilities.tickers_table.keys())

print_status = utilities.print_status
cur_sector = "INIT"
total_date = 0
finished_date = 0
sleep = False
errs = []
err_mutex = multiprocessing.Lock()
mysql_mutex = multiprocessing.Lock()
exit_flag = False

def select_tickers(df_tickers, required_tickers):
	result = []
	for ticker in df_tickers['ticker']:
		if ticker in required_tickers:
			result.append(ticker)
	return result

def price_mom(end_datetime, period, field_name, sector, tickers_map):
	mysql_mutex_acquired = False
	try:
		
		delta = pandas.DateOffset(months = period)
		start_datetime = end_datetime - delta
		end_date = ''.join(end_datetime.isoformat()[0:10].split('-'))
		start_date = ''.join(start_datetime.isoformat()[0:10].split('-'))
		mysql_mutex.acquire()
		mysql_mutex_acquired = True
		valid_tickers = pandas.read_sql("SELECT DISTINCT ticker FROM %s WHERE sector = '%s' AND date = '%s';"%(raw_table, sector, end_datetime.isoformat()[0:10]),  mysql_conn, coerce_float = False)
		mysql_mutex.release()
		mysql_mutex_acquired = False
		selected_tickers = select_tickers(valid_tickers, tickers_map)
		if len(selected_tickers) > 0:
			tmp_result = LocalTerminal.get_reference_data(selected_tickers, 'CUST_TRR_RETURN_HOLDING_PER', CUST_TRR_START_DT=start_date, CUST_TRR_END_DT=end_date).as_map()
			outfile = open(out_folder+"/%s-mom-%s.%s.csv"%(sector, end_date, period), "w")
			utilities.write_row(outfile, "date", "ticker", "field", "value")
			for ticker in tmp_result:
				if not pandas.isnull(tmp_result[ticker]['CUST_TRR_RETURN_HOLDING_PER']):
					utilities.write_row(outfile, end_date, ticker, field_name, tmp_result[ticker]['CUST_TRR_RETURN_HOLDING_PER'])
			outfile.close()
		else:
			print_status("No ticker found on %s."%end_date)
		return 0
	except Exception as e:
		if mysql_mutex_acquired:
			mysql_mutex.release()
		traceback.print_exc()
		err_mutex.acquire()
		errs.append(traceback.format_exc())
		err_mutex.release()
		return -1

def send_status_management():
	def send_status():
		global errs, sleep
		try:
			subject = "Crawler Status Update"
			if sleep:
				body = "Status: Pending\n"
			else:
				body = "Status: Crawling\n"
			body += "Sector: %s\nProgress: %d/%d\n"%(cur_sector, finished_date, total_date)
			err_mutex.acquire()
			if len(errs) > 0:
				body += "Error Message:\n"
				for errmsg in errs:
					body += "--------------------\n"
					body += '> ' + '\n> '.join(errmsg.rstrip("\n").split("\n")) + '\n'
			errs = []
			err_mutex.release()
			utilities.send_gmail(email_status_dest, subject, body)
		except Exception as e:
			traceback.print_exc()

	time.sleep(10)
	schedule.every(email_status_freq).minutes.do(send_status).run()
	while not exit_flag:
		schedule.run_pending()
		time.sleep(1)
	
def send_finish_msg():
	subject = "Crawler Status Update"
	body = "Finished crawling, crawler is going to terminate."
	utilities.send_gmail(email_status_dest, subject, body)

def retry_crawler():
	global sleep
	sleep = False

mysql_conn = utilities.mysql_connection(host, database, username)
end_dates = pandas.read_sql("SELECT DISTINCT date FROM %s WHERE date >= '%s' ORDER BY date asc"%(raw_table, first_date), mysql_conn, coerce_float = False, parse_dates = ["date"])["date"]

email_thread = threading.Thread(target = send_status_management)
email_thread.start()
print_status("Crawling data...")
for sector in sectors:
	cur_sector = sector
	total_date = len(end_dates)
	finished_date = 0
	tickers_map = {}
	for ticker in utilities.tickers_table[sector]:
		tickers_map[ticker] = True
	for end_date in end_dates:
		retry = True
		while retry:
			with ThreadPoolExecutor(max_workers = max_thread_no) as executor:
				futures = []
				for period, field_name in periods:
					futures.append(executor.submit(price_mom, end_date, period, field_name, sector, tickers_map))
				retry = False
				for future in futures:
					if future.result() != 0:
						retry = True
						sleep = True
						print_status("Exception occured when crawling date %s, retry after 3 hrs."%end_date)
			# In case of daily limit exceeded, retry after 3 hours
			if sleep:
				threading.Timer(3*60*60, retry_crawler).start()
			while sleep:
				time.sleep(1)
		print_status("Finished crawling %s on %s"%(sector, end_date))
		finished_date += 1
send_finish_msg()
exit_flag = True