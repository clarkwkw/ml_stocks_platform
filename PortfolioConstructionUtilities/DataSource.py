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

	if type(start_date) == str:
		condition_sqls.append("date >= '%s'"%start_date)

	if type(end_date) == str:
		condition_sqls.append("date <= '%s'"%end_date)

	if market_cap is not None:
		condition_sqls.append("market_cap >= %s"%str(market_cap))

	if len(condition_sqls) > 0:
		condition_sql = "WHERE %s"%(" AND ".join(condition_sqls))

	debug.log("DataSource: Firing MYSQL request..")
	sql = "SELECT %s FROM %s %s ORDER BY sector asc;"%(factors_sql, ml_factor_table, condition_sql)
	mysql_engine = utils.get_mysql_engine()
	ml_factors = pandas.read_sql(sql, mysql_engine, parse_dates = ['date'])
	ml_factors.set_index(keys = ['sector'], drop = False, inplace = True)

	debug.log("DataSource: Splitting dataframe into individual sectors..")
	ML_sector_factors = {}
	for sector in sectors:
		ML_sector_factors[sector] = ml_factors.loc[ml_factors['sector'] == sector]
		ML_sector_factors[sector].sort(columns = ['date'], inplace = True)
		ML_sector_factors[sector].set_index(keys = ['date'], drop = False, inplace = True)
		if type(output_dir) is str:
			ML_sector_factors[sector].to_csv("%s/%s_ML_factor.csv"%(output_dir, sector), na_rep = "nan", index = False)
	return ML_sector_factors