import pandas

def print_status(msg):
	print("> "+str(msg))

def batch_data(tickers, batch_size):
	length = len(tickers)
	batches = length//batch_size
	if length%batch_size != 0:
		batches += 1
	arr = []
	for i in range(batches):
		start = i*batch_size
		end = (i+1)*batch_size
		if end > length:
			end = length
		arr.append((start, end))
	return arr

def write_row(f, date, ticker, field, value):
	if pandas.isnull(value):
		value = ""
	f.write("%s, %s, %s, %s\n"%(str(date), str(ticker), str(field), str(value)))