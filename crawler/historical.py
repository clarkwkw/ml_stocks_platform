from __future__ import print_function
import json
import numpy as np
from tia.bbg import LocalTerminal
import utilities

batch_size = 1000
frequency = "DAILY"
start_date = '12/31/1980'
end_date = '31/12/2010'

def write_dict(csv_file, dict):
	for ticker in dict.keys():
			ticker_data = dict[ticker]
			for field in ticker_data.keys():
				field_data = ticker_data[field]
				for date in field_data.keys():
					value = field_data[date]
					utilities.write_row(csv_file, date, ticker, field, value)

print_status = utilities.print_status

print_status("Crawling data...")

daily_fields = get_raw_fields(freq = "daily", override = false)
quarterly_fields = get_raw_fields(freq = "quarterly", override = false)

for sector in tickers_table.keys():
	print_status("\tCrawling %s sector..."%sector)
	csv_file = open(sector+".csv", "w")
	write_row(csv_file, "date", "ticker", "field", "value")

	batches = batch_data(tickers_table[sector], batch_size)
	i = 1

	for (begin, end) in batches:
		print_status("\t Batch %d/%d"%(i, len(batches)))
		for field_list in daily_fields:
			result = LocalTerminal.get_historical(tickers_table[sector][begin:end], field_list, period = "DAILY", start = start_date, end = end_date)
			result = result.as_map()
			write_dict(csv_file, result)

		for field_list in quarterly_fields:
			result = LocalTerminal.get_historical(tickers_table[sector][begin:end], field_list, period = "QUARTERLY", start = start_date, end = end_date)
			result = result.as_map()
			write_dict(csv_file, result)
		i += 1

	csv_file.close()

print_status("Done.")