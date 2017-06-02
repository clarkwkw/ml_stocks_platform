from __future__ import print_function

def print_status(msg):
	print("> "+str(msg))

print_status("Loading libraries...")

from tia.bbg import LocalTerminal
import pandas as pd
import json

tickers_json = "tickers.json"
fields_json = "field table.json"
test_stock = "T US Equity"

print_status("Loading json settings...")
with open(tickers_json) as tickers_file:
	tickers_table = json.load(tickers_file)
with open(fields_json) as fields_file:
	fields_table = json.load(fields_file)

print_status("Crawling data...")
'''
for sector in tickers_table.keys():
	print_status("  Crawling %s sector..."%sector)
	result = LocalTerminal.get_reference_data(tickers_table[sector], fields_table.keys(), ignore_field_error=1)
	result.as_frame().to_csv(sector+".csv", index = False)
'''
result = LocalTerminal.get_reference_data(test_stock, fields_table.keys(), ignore_field_error=1)
result.as_frame().to_csv("test.csv")
print_status("Done.")