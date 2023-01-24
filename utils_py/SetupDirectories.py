import os, sys

home_dir = os.path.abspath(sys.argv[1])
script_dir = os.path.join(home_dir, "sh")
log_dir = os.path.join(home_dir, "logs")
data_dir = os.path.join(home_dir, "data")
sql_dir = os.path.join(home_dir, "sql")
backup_dir = os.path.join(data_dir, "backup")
param_dir = os.path.join(home_dir, "parms")
