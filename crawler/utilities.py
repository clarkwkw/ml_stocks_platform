import pandas
import getpass
try:
	import sqlalchemy
except ImportError:
	print("> sqlachemy & mysqldb are required for mysql connection")

def print_status(msg):
	print("> "+str(msg))

def batch_data(series, batch_size):
	length = len(series)
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
		value = "nan"
	f.write("%s, %s ,%s ,%s\n"%(str(date), str(ticker), str(field), str(value)))

def mysql_connection(host, database, username):
	print_status('Connecting to %s@%s'%(username, host))
	password = getpass.getpass('MYSQL Password:')
	try:
		engine = sqlalchemy.create_engine('mysql+mysqldb://%s:%s@%s'%(username, password, host))
		engine.execute('USE %s'%database)
	except sqlalchemy.exc.OperationalError as e:
		print_status('Wrong credentials, abort')
		exit(-1)
	print_status("Connected")
	return engine

