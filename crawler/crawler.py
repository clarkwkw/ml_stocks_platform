from __future__ import print_function
import json
import numpy as np
import pandas
from tia.bbg import LocalTerminal
from utilities import *

tickers_json = "./test_tickers.json"
fields_json = "./field table.json"
batch_size = 1000

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
		result = LocalTerminal.get_historical(tickers_table[sector][begin:end], fields_table.keys(), period='QUARTERLY', start='12/31/1980', end='31/12/2010')
		print_status("\t Received data from Bloomberg, parsing...")
		tmp_frame = result.as_frame()

		cols = list(tmp_frame)

		for index, row in tmp_frame.iterrows():
			date = index
			for col in cols:
				ticker = col[0]
				field = col[1]
				value = row[col]
				write_row(csv_file, date, ticker, field, value)
		i += 1

	csv_file.close()

print_status("Done.")