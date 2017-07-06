from __future__ import print_function
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import pandas
import schedule
import time
from tia.bbg import LocalTerminal
import traceback
from threading import Thread
import utilities

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'
raw_table = 'bloomberg_raw'
out_folder = "./historical data"

email_status_dest = "clarkwkw@yahoo.com.hk"
email_status_freq = 60

periods = [(1, "price_mom_1m"), (3, "price_mom_3m"), (6, "price_mom_6m"), (12, "price_mom_12m")]
max_thread_no = 8
sectors = list(utilities.tickers_table.keys())

print_status = utilities.print_status
cur_sector = "INIT"
exit_flag = False
total_date = 0
finished_date = 0
errs = []
err_mutex = multiprocessing.Lock()

def price_mom(end_datetime, period, field_name, sector):
	try:
		tickers = utilities.tickers_table[sector]

		delta = pandas.DateOffset(months = period)
		start_datetime = end_datetime - delta
		end_date = end_datetime.isoformat()[0:10]
		start_date = start_datetime.isoformat()[0:10]
		tmp_result = LocalTerminal.get_reference_data(tickers, 'CUST_TRR_RETURN_HOLDING_PER', CUST_TRR_START_DT=start_date, CUST_TRR_END_DT=end_date).as_map()
		outfile = open(out_folder+"/%s-mom-%s.%s.csv"%(sector, end_date, period), "w")
		utilities.write_row(outfile, "date", "ticker", "field", "value")
		for ticker in tmp_result:
			if not pandas.isnull(tmp_result[ticker]['CUST_TRR_RETURN_HOLDING_PER']):
				utilities.write_row(outfile, end_date, ticker, field_name, tmp_result[ticker]['CUST_TRR_RETURN_HOLDING_PER'])
		outfile.close()
		return 0
	except Exception as e:
		traceback.print_exc()
		err_mutex.acquire()
		errs.append(traceback.format_exc())
		err_mutex.release()
		return -1

def send_status_management():
	def send_status():
		subject = "Crawler Status Update"
		body = "Crawling %s sector, progress: %d/%d."%(cur_sector, finished_date, total_date)
		utilities.send_gmail(email_status_dest, subject, body)
	time.sleep(10)
	schedule.every(email_status_freq).minutes.do(send_status).run()
	while not exit_flag:
		schedule.run_pending()
		time.sleep(1)

def send_err_status():
	subject = "Crawler Status Update"
	body = "Crawler is going to terminate, error(s) occured.\n"
	body += "BTW, was crawling %s sector, progress: %d/%d.\n"%(cur_sector, finished_date, total_date)
	body += "Error Message:\n"
	for errmsg in errs:
		body += "--------------------\n"
		body += '> ' + '\n> '.join(errmsg.rstrip("\n").split("\n"))
	utilities.send_gmail(email_status_dest, subject, body)

def send_finish_msg():
	subject = "Crawler Status Update"
	body = "Finished crawling, crawler is going to terminate."
	utilities.send_gmail(email_status_dest, subject, body)

mysql_conn = utilities.mysql_connection(host, database, username)
end_dates = pandas.read_sql("SELECT DISTINCT date FROM %s ORDER BY date asc"%raw_table, mysql_conn, coerce_float = False, parse_dates = ["date"])["date"]

email_thread = Thread(target = send_status_management)
email_thread.start()
print_status("Crawling data...")
for sector in sectors:
	cur_sector = sector
	total_date = len(end_dates)
	for end_date in end_dates:
		with ThreadPoolExecutor(max_workers = max_thread_no) as executor:
			futures = []
			for period, field_name in periods:
				futures.append(executor.submit(price_mom, end_date, period, field_name, sector))
			for future in futures:
				if future.result() != 0:
					print_status("Exception occured when crawling date %s, abort."%end_date)
					exit_flag = True
			if exit_flag:
				break
		print_status("Finished crawling %s on %s"%(sector, end_date))
		finished_date += 1
	if exit_flag:
		send_err_status()
		break
if not exit_flag:
	send_finish_msg()