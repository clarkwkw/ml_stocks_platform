import utils
import pandas
import config
import debug

# factors: list of field names, or "all" -> all fields will be downloaded
def DownloadTableFileFromMySQL(market_id, sectors = [], factors = [], market_cap = None, include_null_cap = False, start_date = None, end_date = None, output_dir = None):
	ml_factor_table = "%s_machine_learning_factor"%market_id
	factors_sql,condition_sql = None, ""
	condition_sqls = []
	if type(factors) is list:
		for factor in utils._necessary_factors:
			if factor not in factors:
				factors.append(factor)
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
		if include_null_cap:
			condition_sqls.append("(market_cap >= %s OR market_cap is NULL)"%str(market_cap))
		else:
			condition_sqls.append("market_cap >= %s"%str(market_cap))

	if len(condition_sqls) > 0:
		condition_sql = "WHERE %s"%(" AND ".join(condition_sqls))

	debug.log("DataSource: Firing MYSQL request..")
	sql = "SELECT %s FROM %s %s;"%(factors_sql, ml_factor_table, condition_sql)
	debug.log("DataSource: query: %s"%sql)
	mysql_engine = utils.get_mysql_engine()
	ml_factors = pandas.read_sql(sql, mysql_engine, parse_dates = ['date'])
	ml_factors.dropna(subset=['last_price'],inplace=True)

	if config.datasource_force_fill_zero:
		ml_factors.fillna(value = 0, inplace = True)

	debug.log("DataSource: Building index on raw data..")
	ml_factors.sort_values(by = ['sector'], inplace = True)
	ml_factors.set_index(keys = ['sector'], drop = False, inplace = True)

	debug.log("DataSource: Getting price info..")
	prices_df = ml_factors[['ticker', 'date', 'last_price']].copy()
	prices_df.is_copy = False
	prices_df.rename(columns = {'last_price': 'price'}, inplace = True)
	prices_df.sort_values(by = ['date'], inplace = True)
	prices_df.set_index(keys = ['date'], drop = False, inplace = True)

	if type(output_dir) is str:
		prices_df.to_csv("%s/prices.csv"%output_dir, na_rep = "nan", index = False)

	if type(output_dir) is str:
		ml_factors.to_csv("%s/ML_factor_table_file.csv"%output_dir, na_rep = "nan", index = False)

	ML_sector_factors = split_to_sectors(ml_factors, sectors)

	debug.log("DataSource: Data is ready")
	return ML_sector_factors, prices_df

def LoadTableFromFile(sectors, input_dir):
	debug.log("DataSource: Loading data from disk..")

	ml_factors = pandas.read_csv("%s/ML_factor_table_file.csv"%input_dir, na_values = ["nan"], parse_dates = ["date"])
	prices_df = pandas.read_csv("%s/prices.csv"%input_dir, na_values = ["nan"], parse_dates = ["date"])
	prices_df.set_index(keys = ['date'], drop = False, inplace = True)

	ML_sector_factors = split_to_sectors(ml_factors, sectors)

	debug.log("DataSource: Data is ready")
	return ML_sector_factors, prices_df

def split_to_sectors(ml_factors, sectors):
	debug.log("DataSource: Splitting MLfactors into individual sectors..")
	ML_sector_factors = {}
	for sector in sectors:
		ML_sector_factors[sector] = ml_factors.loc[ml_factors['sector'] == sector]
		ML_sector_factors[sector].is_copy = False
		ML_sector_factors[sector].sort_values(by = ['date'], inplace = True)
		ML_sector_factors[sector].set_index(keys = ['date'], drop = False, inplace = True)
	return ML_sector_factors