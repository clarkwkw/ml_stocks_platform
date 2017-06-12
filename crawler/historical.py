from __future__ import print_function
import json
import numpy as np
from tia.bbg import LocalTerminal
from utilities import *

tickers_json = "./test_tickers.json"
fields_json = "./historical_fields.json"
batch_size = 1000
frequency = "DAILY"
start_date = '12/31/1980'
end_date = '31/12/2010'

print_status("Loading json settings...")
with open(tickers_json) as tickers_file:
	tickers_table = json.load(tickers_file)
with open(fields_json) as fields_file:
	fields_table = json.load(fields_file)

print_status("Crawling data...")

for sector in tickers_table.keys():
	print_status("\tCrawling %s sector..."%sector)
	csv_file = open(sector+".csv", "w")
	write_row(csv_file, "date", "ticker", "field", "value")

	batches = batch_data(tickers_table[sector], batch_size)
	i = 1

	for (begin, end) in batches:
		print_status("\t Batch %d/%d"%(i, len(batches)))
		result = LocalTerminal.get_historical(tickers_table[sector][begin:end], fields_table.keys(), period = frequency, start = start_date, end = end_date)
		print_status("\t Received data from Bloomberg, parsing...")
		result = result.as_map()

		for ticker in result.keys():
			ticker_data = result[ticker]
			for field in ticker_data.keys():
				field_data = ticker_data[field]
				for date in field_data.keys():
					value = field_data[date]
					write_row(csv_file, date, ticker, field, value)
		i += 1

	csv_file.close()

print_status("Done.")