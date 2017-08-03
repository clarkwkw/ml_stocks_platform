from __future__ import print_function
import json
import numpy as np
from tia.bbg import LocalTerminal
import utilities

batch_size = 1000

#M/D/YYYY
start_date = '12/31/1995'
end_date = '12/31/2016'

def write_dict(csv_file, dict, raw_fields, parsed_fields):
	for ticker in dict.keys():
			ticker_data = dict[ticker]
			for field in ticker_data.keys():
				if type(parsed_fields) is list:
					parsed_field = parsed_fields[raw_fields.index(field)]
				else:
					parsed_field = parsed_fields
				field_data = ticker_data[field]
				for date in field_data.keys():
					value = field_data[date]
					utilities.write_row(csv_file, date, ticker, parsed_field, value)

print_status = utilities.print_status

print_status("Crawling data...")

daily_fields = utilities.direct_fields(freq = "daily", override = False)
quarterly_fields = utilities.direct_fields(freq = "quarterly", override = False)
override_fields = utilities.direct_fields(override = True)

for sector in utilities.tickers_table.keys():
	print_status("\tCrawling %s sector..."%sector)
	csv_file = open(sector+".csv", "w")
	utilities.write_row(csv_file, "date", "ticker", "field", "value")

	batches = utilities.batch_data(utilities.tickers_table[sector], batch_size)
	i = 1

	for (begin, end) in batches:
		print_status("\t Batch %d/%d"%(i, len(batches)))
		for (raw_fields, parsed_fields) in daily_fields:
			result = LocalTerminal.get_historical(utilities.tickers_table[sector][begin:end], raw_fields, start = start_date, end = end_date)
			result = result.as_map()
			write_dict(csv_file, result, raw_fields, parsed_fields)

		for  (raw_fields, parsed_fields) in quarterly_fields:
			result = LocalTerminal.get_historical(utilities.tickers_table[sector][begin:end], raw_fields, period = "QUARTERLY", start = start_date, end = end_date)
			result = result.as_map()
			write_dict(csv_file, result, raw_fields, parsed_fields)

		for ((field, override, freq), parsed_field) in override_fields:
			result = LocalTerminal.get_historical(utilities.tickers_table[sector][begin:end], [field], period = freq.upper(), start = start_date, end = end_date, **override)
			result = result.as_map()
			write_dict(csv_file, result, field, parsed_field)
		i += 1

	csv_file.close()

print_status("Done.")