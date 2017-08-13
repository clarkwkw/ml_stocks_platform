# --- Global settings ---
date_format = "%Y-%m-%d"

debug_log = True

ignore_warnings = True

max_thread = 4

# --- Module specific settings ---

# Fill missing values with zero when downloading data from MYSQL
datasource_force_fill_zero = True

svm_max_iter = 1000

# In SimulateTradingProcess, if no metaparameter selection is required, 
# whether the training data should be formed in a rolling way
simulate_rolling_data_when_not_metapara = False

# In SimulateTradingProcess, for metaparameter selection, 
# reserve at least the following no. of year data at the beginning
# that means the first model would be delayed by this no. of year
simulate_reserved_data_duration = 3

# --- MYSQL Config ---
sql_host = "seis10.se.cuhk.edu.hk"

sql_username = "finanai"

sql_password = ""

sql_database = "finanai"

sql_db_connector = "mysqldb"