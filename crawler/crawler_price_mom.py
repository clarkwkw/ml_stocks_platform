from __future__ import print_function
from concurrent.futures import ThreadPoolExecutor
from dateutil.relativedelta import relativedelta
import datetime
import json
import multiprocessing
import numpy as np
from tia.bbg import LocalTerminal
import traceback
import utilities

#M/D/YYYY
#periods = [(1, "price_mom_1m"), (3, "price_mom_3m"), (6, "price_mom_6m"), (12, "price_mom_12m")]
periods = [(12, "price_mom_12m")]
max_thread_no = 10

print_status = utilities.print_status
total_requests = 0
finished = 0
status_mutex = multiprocessing.Lock()
status_status = False

with open('trading_days.json', 'r') as f:
	trading_dates = json.load(f)['end_dates']

def price_mom(ticker, end_date, field_name, period, csv_file):
	try:
		global finished
		[year, month, day] = [int(x) for x in end_date.split('-')]
		end_datetime = datetime.datetime(year, month, day)
		delta = relativedelta(months = period)
		start_datetime = end_datetime - delta
		start_date = start_datetime.isoformat()[0:10]
		result = LocalTerminal.get_reference_data(ticker, 'CUST_TRR_RETURN_HOLDING_PER', CUST_TRR_START_DT=start_date, CUST_TRR_END_DT=end_date).as_map()
		value = result[ticker]['CUST_TRR_RETURN_HOLDING_PER']
		status_mutex.acquire()
		finished += 1
		utilities.write_row(csv_file, end_date, ticker, field_name, value)
		print_status("Crawled (%.2f%%)"%(1.0* finished / total_requests * 100))
		status_mutex.release()
	except Exception as e:
		traceback.print_exc()

print_status("Crawling data...")

for sector in utilities.tickers_table.keys():
	csv_file = open(sector+".csv", "w")
	utilities.write_row(csv_file, "date", "ticker", "field", "value")
	with ThreadPoolExecutor(max_workers = max_thread_no) as executor:
		total_requests = len(periods)*len(utilities.tickers_table[sector])*len(trading_dates)
		finished = 0
		print_status("\tCrawling %s sector... (%d requests)"%(sector, total_requests))
		for period, field in periods:
			for ticker in utilities.tickers_table[sector]:
				for trading_date in trading_dates:
					executor.submit(price_mom, ticker, trading_date, field, period, csv_file)
		print_status("Writing to csv file...")

	csv_file.close()

print_status("Done.")