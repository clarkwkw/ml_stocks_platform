from __future__ import print_function
import pandas
import numpy as np

def print_status(msg):
	print("> "+str(msg))

print_status("Loading libraries...")

from tia.bbg import LocalTerminal
import pandas as pd
import json

tickers_json = "./test_tickers.json"
fields_json = "./field table.json"

print_status("Loading json settings...")
with open(tickers_json) as tickers_file:
	tickers_table = json.load(tickers_file)
with open(fields_json) as fields_file:
	fields_table = json.load(fields_file)

print_status("Crawling data...")

for sector in tickers_table.keys():
	print_status("  Crawling %s sector..."%sector)
	#result = LocalTerminal.get_historical(tickers_table[sector], fields_table.keys(), start='6/1/2017', end='6/5/2017', ignore_field_error=1)
	result = LocalTerminal.get_historical(tickers_table[sector], fields_table.keys(), period='MONTHLY', start='1/1/2016', end='1/1/2017')
	tmp_frame = result.as_frame()
	dates = []
	tickers = []
	fields = []
	values = []
	i = 0
	cols = list(tmp_frame)

	for index, row in tmp_frame.iterrows():
		date = index
		for col in cols:
			ticker = col[0]
			field = col[1]
			dates.append(date)
			tickers.append(ticker)
			fields.append(field)
			values.append(row[col])
			i += 1
	data = {'date': dates, 'ticker': tickers, 'field':fields, 'value':values}
	result_frame = pandas.DataFrame(data)		
	result_frame.to_csv(sector+".csv", index = False)

print_status("Done.")