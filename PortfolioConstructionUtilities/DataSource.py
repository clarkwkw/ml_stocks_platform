import utils
import pandas
import debug

# factors: list of field names, or "all" -> all fields will be downloaded
def DownloadTableFileFromMySQL(market_id, sectors = [], factors = [], market_cap = None, start_date = None, end_date = None, output_dir = None):
	ml_factor_table = "%s_machine_learning_factor"%market_id
	factors_sql,condition_sql = None, ""
	condition_sqls = []
	if type(factors) is list:
		factors.extend(utils._id_fields)
		factors_sql = ",".join(factors)
	elif factors == 'all':
		factors_sql = "*"
	else:
		raise Exception("Unexpected value for factors '%s'"%str(factors))

	if type(sectors) is list:
		condition_sqls.append("sector IN ('%s')"%("', '".join(sectors)))
	else:
		raise Exception("Unexpected value for sectors '%s'"%str(sectors))

	if start_date is not None:
		condition_sqls.append("date >= '%s'"%start_date)

	if end_date is not None:
		condition_sqls.append("date <= '%s'"%end_date)

	if market_cap is not None:
		condition_sqls.append("market_cap >= %s"%str(market_cap))

	if len(condition_sqls) > 0:
		condition_sql = "WHERE %s"%(" AND ".join(condition_sqls))

	debug.log("DataSource: Firing MYSQL request..")
	sql = "SELECT %s FROM %s %s;"%(factors_sql, ml_factor_table, condition_sql)
	debug.log("DataSource: query: %s"%sql)
	mysql_engine = utils.get_mysql_engine()
	ml_factors = pandas.read_sql(sql, mysql_engine, parse_dates = ['date'])

	debug.log("DataSource: Building index on raw data..")
	ml_factors.sort_values(by = ['sector'], inplace = True)
	ml_factors.set_index(keys = ['sector'], drop = False, inplace = True)

	debug.log("DataSource: Getting price info..")
	prices_df = None
	if "last_price" in factors:
		prices_df = ml_factors[['ticker', 'date', 'last_price']].copy()
	else:
		prices_df = pandas.read_sql("SELECT date, ticker, last_price FROM %s %s;"%(ml_factor_table, condition_sql), mysql_engine, parse_dates = ['date'])
	prices_df.rename(columns = {'last_price': 'price'})
	prices_df.sort_values(by = ['date'], inplace = True)
	prices_df.set_index(keys = ['date'], drop = False, inplace = True)
	if type(output_dir) is str:
		prices_df.to_csv("%s/prices.csv"%output_dir, na_rep = "nan", index = False)

	debug.log("DataSource: Splitting MLfactors into individual sectors..")
	ML_sector_factors = {}
	for sector in sectors:
		ML_sector_factors[sector] = ml_factors.loc[ml_factors['sector'] == sector]
		ML_sector_factors[sector].sort_values(by = ['date'], inplace = True)
		ML_sector_factors[sector].set_index(keys = ['date'], drop = False, inplace = True)
		if type(output_dir) is str:
			ML_sector_factors[sector].to_csv("%s/%s_ML_factor.csv"%(output_dir, sector), na_rep = "nan", index = False)
	return ML_sector_factors, prices_df

def LoadTableFromFile(sectors, input_dir):
	prices_df = pandas.read_csv("%s/prices.csv"%input_dir, na_values = ["nan"], parse_dates = ["date"])
	prices_df.set_index(keys = ['date'], drop = False, inplace = True)
	
	ML_sector_factors = {}
	for sector in sectors:
		ML_sector_factors[sector] = pandas.read_csv("%s/%s_ML_factor.csv"%(input_dir, sector), na_values = ["nan"], parse_dates = ["date"])
		ML_sector_factors[sector].set_index(keys = ['date'], drop = False, inplace = True)

	return ML_sector_factors, prices_df