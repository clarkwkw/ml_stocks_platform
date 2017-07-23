import argparse
import filling
import numpy as np
import pandas
from tqdm import tqdm
import traceback
import utilities
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

host = "seis10.se.cuhk.edu.hk"
database = "finanai"
username = "finanai"
raw_table = 'US_bloomberg_factor'
target_table = 'US_machine_learning_factor'
max_thread_no = 2

id_fields = ['record_id', 'date', 'ticker', 'sector']

def parse_arg():
	parser = argparse.ArgumentParser()
	group1 = parser.add_mutually_exclusive_group(required=True)
	

	# Rebuild section
	group1.add_argument("-r", "--rebuild", help = "rebuild the machine learning factor table", action='store_true')

	# Fillmean section
	group1.add_argument("-m", "--fillmean", help = "fill the sector mean for missing values in the machine learning factor table", action = 'store_true')

	# Getdata section
	group1.add_argument("-g", "--getdata", help = "get data from machine learning table", action = "store_true")
	parser.add_argument("-t", "--tickers", nargs = "+", help = "[getdata] tickers to crawl, no. of tickers to crawl if only 1 integer is provided")

	# Common argument
	parser.add_argument("-s", "--sectors", nargs = "+", help = "[mandatory] sector names")
	parser.add_argument("-f", "--factors", nargs = "+", help = "[mandatory] direct fields to rebuild [-r]/ fields to fill with sector mean [-f]/ factors to download [-g]")
	args =  parser.parse_args()

	if type(args.sectors) is not list:
		raise argparse.ArgumentTypeError("argument -s/--sectors: expected at least one string")
	if type(args.factors) is not list:
		raise argparse.ArgumentTypeError("argument -f/--factors: expected at least one string")
	if args.getdata and type(args.tickers) is not list:
		raise argparse.ArgumentTypeError("argument -t/--tickers: expected at least one string")

	return args

def get_ML_factors(ticker, factors = [], mysql_conn = None, mysql_mutex = None):
	factors_str = ""
	for factor in factors:
		factors_str += ", %s"%factor
	sql_query = "SELECT record_id, date%s FROM %s WHERE ticker = '%s' ORDER BY date asc;"%(factors_str, target_table, ticker)
	mysql_mutex.acquire()
	df = pandas.read_sql(sql_query, mysql_conn, coerce_float = False, parse_dates = ['date'])
	mysql_mutex.release()
	return df

def get_raw_factors(ticker, factors = [], mysql_conn = None, mysql_mutex = None):
	factors_str = "'%s'"%factors[0]
	for i in range(1, len(factors)):
		factors_str += ", '%s'"%factors[i]
	sql_query = "SELECT date, field, value FROM %s WHERE ticker = '%s' AND field in (%s) ORDER BY date asc;"%(raw_table, ticker, factors_str)
	mysql_mutex.acquire()
	df = pandas.read_sql(sql_query, mysql_conn, coerce_float = False, parse_dates = ['date'])
	mysql_mutex.release()
	return df

def save_factors(ml_table, factors, mysql_conn, mysql_mutex):
	tmp_table_name = "tmp_"+utilities.rand_str(6)
	update_str = "ml.%s = tmp.%s"%(factors[0], factors[0])
	for i in range(1, len(factors)):
		update_str += ", ml.%s = tmp.%s"%(factors[i], factors[i])
	mysql_mutex.acquire()
	while mysql_conn.dialect.has_table(mysql_conn, tmp_table_name):
		tmp_table_name = "tmp_"+utilities.rand_str(6)
	ml_table.to_sql(tmp_table_name, mysql_conn, if_exists = 'fail', index = False, chunksize = 100)
	sql = "UPDATE %s ml INNER JOIN %s tmp ON ml.record_id = tmp.record_id SET %s;"%(target_table, tmp_table_name, update_str)
	with mysql_conn.begin() as conn:
		conn.execute(sql)
		conn.execute("DROP TABLE %s;"%tmp_table_name)
	mysql_mutex.release()

def rebuild(sectors, factors):
	tickers_to_rebuild = []
	finished_count = 0
	futures = []
	for sector in sectors:
		tickers_to_rebuild.extend(utilities.tickers_table[sector])
	utilities.print_status("Firing request for %d ticker(s)..."%len(tickers_to_rebuild))
	mysql_conn = utilities.mysql_connection(host, database, username)
	mysql_mutex = multiprocessing.Lock()
	with ThreadPoolExecutor(max_workers = max_thread_no) as executor:
		for ticker in tickers_to_rebuild:
			futures.append(executor.submit(rebuild_by_ticker, ticker, factors, mysql_conn, mysql_mutex))
		for _ in tqdm(as_completed(futures)):
				finished_count += 1

def rebuild_by_ticker(ticker, factors, mysql_conn, mysql_mutex):
	try:
		ml_table = get_ML_factors(ticker, mysql_conn = mysql_conn, mysql_mutex = mysql_mutex)
		raw_table = get_raw_factors(ticker, factors, mysql_conn, mysql_mutex)
		for factor in factors:
			ml_table[factor] = np.NAN
		for index, row in raw_table.iterrows():
				date = row['date']
				field = row['field']
				val = row['value']
				ml_table.loc[ml_table['date'] == date, field] = val
		ml_table = filling.fill_direct_prev(ml_table, factors)
		save_factors(ml_table, factors, mysql_conn, mysql_mutex)
		return 0
	except:
		traceback.print_exc()

def fillmean(sectors, factors):
	mysql_conn = utilities.mysql_connection(host, database, username)
	if len(factors) == 1 and factors[0] == 'all':
		factors = pandas.read_sql("SELECT COLUMN_NAME as factors FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';"%(database, target_table), mysql_conn, coerce_float = False)
		factors = factors.loc[~(factors['factors'].isin(id_fields)), 'factors']	
	for sector in sectors:
		for factor in factors:
			utilities.print_status("Filling %s - %s"%(sector, factor))
			sql = "UPDATE %s ml JOIN (SELECT sector, date, avg(%s) AS avg FROM %s WHERE sector = '%s' GROUP BY sector, date) val ON ml.sector = val.sector AND ml.date = val.date SET ml.%s = val.avg WHERE ml.%s IS NULL;"%(target_table, factor, target_table, sector, factor, factor)
			with mysql_conn.begin() as conn:
				conn.execute(sql)

def getdata(sectors, factors, tickers):
	mysql_conn = utilities.mysql_connection(host, database, username)
	if len(factors) == 1 and factors[0] == 'all':
		factors_sql = "*"
	else:
		factors = id_fields + factors
		factors_sql = ", ".join(factors)

	rand_tickers = False
	n_tickers = len(tickers)

	if n_tickers == 1 and tickers[0].isdigit():
		rand_tickers = True
		n_tickers = int(tickers[0])
	for sector in sectors:
		print("Crawling sector - %s"%sector)
		if rand_tickers:
			tickers = pandas.read_sql("SELECT DISTINCT ticker FROM %s WHERE sector = '%s' ORDER BY rand() LIMIT %d;"%(target_table, sector, n_tickers), mysql_conn, coerce_float = False)['ticker']
		tickers_sql = "'%s'"%tickers[0]
		for i in range(1, len(tickers)):
			tickers_sql += ", '%s'"%tickers[i]
		df = pandas.read_sql("SELECT %s FROM %s WHERE ticker in (%s);"%(factors_sql, target_table, tickers_sql), mysql_conn,  coerce_float = False, parse_dates = ["date"])
		df.to_csv("%s-%d.csv"%(sector, n_tickers), index = False, na_rep = "nan")

if __name__ == "__main__":
	args = parse_arg()
	if args.rebuild:
		rebuild(args.sectors, args.factors)
	elif args.fillmean:
		fillmean(args.sectors, args.factors)
	elif args.getdata:
		getdata(args.sectors, args.factors, args.tickers)
	else:
		raise Exception("Please select a valid action")