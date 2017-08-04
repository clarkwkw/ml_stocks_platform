import pandas
from utilities import print_status, mysql_connection

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'

target_table = 'US_bloomberg_factor_2'
filelist = [('./historical data/Materials.csv', 'Materials'), ('./historical data/Energy.csv', 'Energy')]

mysql_conn = mysql_connection(host, database, username)
for (filepath, sector) in filelist:
	print_status('Reading %s...'%filepath)
	df = pandas.read_csv(filepath, index_col = False, na_values = ['nan'], skipinitialspace = True)
	df['sector'] = sector
	print_status('Uploading...')
	df.to_sql(target_table, mysql_conn, if_exists = 'append', index = False, chunksize = 10000)
