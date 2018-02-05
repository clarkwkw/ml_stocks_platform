import pandas
from utilities import print_status, mysql_connection, tickers_table
import os

host = 'seis10.se.cuhk.edu.hk'
database = 'finanai'
username = 'finanai'

target_table = 'HK2_bloomberg_factor'
file_dir = "./historical data"
filelist = []
err_files = {
	"format": [],
	"sector": []
}

tmplist = os.listdir(file_dir)
for path in tmplist:
	basename = os.path.basename(path)
	name, ext = os.path.splitext(basename)
	if ext.lower() == ".csv":
		try:
			sector, _ = name.split("-", 1)
			if sector not in tickers_table:
				err_files["sector"].append(basename)
			else:
				filelist.append((file_dir.rstrip("/") + "/" + path, sector))
		except ValueError:
			err_files["format"].append(basename)

for reason, files in err_files.items():
	if len(files) > 0:
		print("Files including:")
		print("\t" + "\n\t".join(files))
		print("   are not included due to %s error."%reason)
		response = ""
		while response.lower() not in ["y", "n"]:
			response = raw_input("Continue? [y/n] ")
		if response.lower() == "n":
			exit(-1)

mysql_conn = mysql_connection(host, database, username)
for (filepath, sector) in filelist:
	print_status('Reading %s...'%filepath)
	df = pandas.read_csv(filepath, index_col = False, na_values = ['nan'], skipinitialspace = True)
	df['sector'] = sector
	print_status('Uploading...')
	df.to_sql(target_table, mysql_conn, if_exists = 'append', index = False, chunksize = 10000)
