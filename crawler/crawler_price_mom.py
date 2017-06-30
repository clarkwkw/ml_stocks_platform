from __future__ import print_function
from concurrent.futures import ThreadPoolExecutor
import json
import multiprocessing
import numpy as np
from tia.bbg import LocalTerminal
import traceback
import utilities

#M/D/YYYY
start_date = '12/31/1995'
end_date = '12/31/2016'
periods = [(30, "price_mom_1m"), (90, "price_mom_3m"), (180, "price_mom_6m"), (360, "price_mom_12m")]
max_thread_no = 10

print_status = utilities.print_status
total_requests = 0
finished = 0
status_mutex = multiprocessing.Lock()
status_status = False

with open('trading_days.json', 'r') as f:
	trading_dates = json.load(f)['end_dates']

def price_mom(ticker, end_date, field_name, period):
	try:
		global finished
		result = LocalTerminal.get_reference_data(ticker, 'CUST_TRR_RETURN_HOLDING_PER', CUST_TRR_START_DT=start_date, CUST_TRR_END_DT=end_date).as_map()
		value = result[ticker]['CUST_TRR_RETURN_HOLDING_PER']
		status_mutex.acquire()
		finished += 1
		print_status("Crawled (%.2f%%)"%(1.0* finished / total_requests * 100))
		status_mutex.release()		
		return (end_date, ticker, field_name, value)
	except Exception as e:
		traceback.print_exc()

print_status("Crawling data...")
with ThreadPoolExecutor(max_workers = max_thread_no) as executor:
	for sector in utilities.tickers_table.keys():
		crawlers = []
		total_requests = len(periods)*len(utilities.tickers_table[sector])*len(trading_dates)
		finished = 0

		print_status("\tCrawling %s sector... (%d requests)"%(sector, total_requests))
		for period, field in periods:
			for ticker in utilities.tickers_table[sector]:
				for trading_date in trading_dates:
					crawlers.append(executor.submit(price_mom, ticker, trading_date, field, period))
		print_status("Writing to csv file...")
		csv_file = open(sector+".csv", "w")
		utilities.write_row(csv_file, "date", "ticker", "field", "value")
		for crawler in crawlers:
			date, ticker, field, value = crawler.result()
			utilities.write_row(csv_file, date, ticker, field, value)
		csv_file.close()

print_status("Done.")