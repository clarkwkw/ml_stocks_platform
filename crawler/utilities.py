import json
import pandas
import platform
import smtplib
from email.mime.text import MIMEText
import getpass
try:
	import sqlalchemy
except ImportError:
	print("> sqlachemy & mysql connector (2.1.4)/ mysqldb are required for mysql connection")

tickers_json = "./test_tickers.json"
fields_json = "./fields.json"
email_json = "./email.json"

# mysqldb/mysqlconnector/...
# For more options, 
# see http://docs.sqlalchemy.org/en/latest/dialects/mysql.html#module-sqlalchemy.dialects.mysql.mysqlconnector
db_connector = "mysqlconnector"

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
	f.write("%s, %s, %s, %s\n"%(str(date), str(ticker), str(field), str(value)))

def mysql_connection(host, database, username):
	print_status('Connecting to %s@%s'%(username, host))
	password = getpass.getpass('MYSQL Password:')
	try:
		engine = sqlalchemy.create_engine('mysql+%s://%s:%s@%s/%s'%(db_connector, username, password, host, database))
	except sqlalchemy.exc.OperationalError as e:
		print_status('Wrong credentials, abort')
		exit(-1)
	print_status("Connected")
	return engine

def send_gmail(recepient, subject, body):
	msg = MIMEText(body)
	msg['Subject'] = subject+" (%s)"%(platform.node())
	msg['From'] = "ML Stocks Platform <%s>"%gmail_address
	msg['To'] = recepient
	mailserver = smtplib.SMTP_SSL(host = "smtp.gmail.com", port = 465)
	mailserver.ehlo()
	mailserver.login(gmail_address, gmail_password)
	mailserver.sendmail(gmail_address, recepient, msg.as_string())
	mailserver.quit()

def direct_fields(freq = "", limit = 25, override = False):
	if limit <= 0:
		raise Exception("'limit' must be positive")
	raw_fields = [field for field in direct_fields_table.values() if field["enabled"]]
	parsed_fields = [field for field in direct_fields_table.keys() if direct_fields_table[field]["enabled"]]
	filtered_raw_fields = []
	filtered_parsed_fields = []
	for i in range(len(raw_fields)):
		field = raw_fields[i]
		if freq.upper() != "" and freq.upper() != field["freq"].upper():
			continue
		if override and "override" not in field:
			continue
		if not override and "override" in field:
			continue
		if override:
			filtered_raw_fields.append((field["raw_field"], field["override"], field["freq"]))
		else:
			filtered_raw_fields.append(field["raw_field"])
		filtered_parsed_fields.append(parsed_fields[i])
	if not override:
		batches = batch_data(filtered_raw_fields, limit)
		result = []
		for (start, end) in batches:
			result.append((filtered_raw_fields[start:end], filtered_parsed_fields[start:end]))
		return result
	else:
		result = []
		for i in range(len(filtered_raw_fields)):
			result.append((filtered_raw_fields[i], filtered_parsed_fields[i]))
		return result

def parsed_field(raw_field, override = None):
	if override is None:
		return raw_to_parsed_table[raw_field]
	else:
		return raw_to_parsed_table[(raw_field, override)]

def direct_parsed_fields():
	return [field for field in direct_fields_table if direct_fields_table[field]['enabled']]

def indirect_parsed_fields():
	return list(indirect_fields_table.keys())

def ml_fields():
	direct_ml_fields = [field for field in direct_fields_table if direct_fields_table[field]['enabled'] and direct_fields_table[field]['ml_db_keep']]
	return direct_ml_fields + indirect_parsed_fields() + complex_fields

with open(tickers_json) as tickers_file:
	tickers_table = json.load(tickers_file)
with open(fields_json) as fields_file:
	fields_table = json.load(fields_file)
	direct_fields_table = fields_table["direct_fields"]
	indirect_fields_table = fields_table["indirect_fields"]
	complex_fields = fields_table["complex_fields"]
with open(email_json) as email_file:
	email_table = json.load(email_file)
	gmail_address = email_table["gmail_address"]
	gmail_password = email_table["gmail_password"]