from __future__ import print_function
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import multiprocessing
import numpy as np
import pandas
from tia.bbg import LocalTerminal
from tqdm import tqdm
import traceback
import utilities

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'
raw_table = 'bloomberg_raw'

#M/D/YYYY
periods = [(1, "price_mom_1m"), (3, "price_mom_3m"), (6, "price_mom_6m"), (12, "price_mom_12m")]
max_thread_no = 4

print_status = utilities.print_status

class CSV_Buffer:
	def __init__(self, csv_path, limit):
		self.file = open(csv_path, "w")
		self.buffer_lock = multiprocessing.Lock()
		self.content = ""
		self.limit = limit
		self.content_count = 0
		self.append("date", "ticker", "field", "value")

	def append(self, date, ticker, name, value):
		new_content = "%s, %s, %s, %s\n"%(str(date), str(ticker), str(name), str(value))
		self.buffer_lock.acquire()
		self.content += new_content
		self.content_count += 1
		if self.content_count >= self.limit:
			self.force_flush()
		self.buffer_lock.release()

	def force_flush(self):
		self.file.write(self.content)
		self.content = ""
		self.content_count = 0

	def close(self):
		self.force_flush()
		self.file.close()

def price_mom(ticker, end_dates, field_name, period, buffer):
	try:
		global finished
		for end_datetime in end_dates:
			delta = pandas.DateOffset(months = period)
			start_datetime = end_datetime - delta
			end_date = end_datetime.isoformat()[0:10]
			start_date = start_datetime.isoformat()[0:10]
			end_date = ''.join(end_date.split('-'))
			start_date = ''.join(start_date.split('-'))
			result = LocalTerminal.get_reference_data(ticker, 'CUST_TRR_RETURN_HOLDING_PER', CUST_TRR_START_DT=start_date, CUST_TRR_END_DT=end_date).as_map()
			value = result[ticker]['CUST_TRR_RETURN_HOLDING_PER']
			buffer.append(end_date, ticker, field_name, value)
		status_mutex.acquire()
		print_status("\t  Crawled period %s."%period)
		status_mutex.release()
		return 0
	except Exception as e:
		traceback.print_exc()
		return -1

mysql_conn = utilities.mysql_connection(host, database, username)
print_status("Crawling data...")

for sector in utilities.tickers_table.keys():
	print_status("\tCrawling %s sector..."%(sector))
	buffs = {}
	for period, _ in periods:
		buffs[period] = CSV_Buffer(sector+"."+str(period)+".csv", 1000)
	with ThreadPoolExecutor(max_workers = max_thread_no) as executor:
		futures = []
		for ticker in utilities.tickers_table[sector]:
			print_status("\t Crawling stock %s"%ticker)
			trading_dates = pandas.read_sql("SELECT DISTINCT date FROM %s WHERE ticker = '%s'"%(raw_table, ticker), mysql_conn, coerce_float = False, parse_dates = ["date"])
			trading_dates = trading_dates["date"]
			for period, field in periods:
				futures.append(executor.submit(price_mom, ticker, trading_dates, field, period, buffs[period]))
			for future in futures:
				if future.result() != 0:
					print_status("Exception occured when crawling %s, abort."%ticker)
					exit(-1)
	for sector in buffs:
		buffs[sector].close()

print_status("Done.")