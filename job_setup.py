import configparser


config = configparser.ConfigParser()
config.read('cp_billing_config.ini')

job_name = config['DEFAULT']['job_name']
job_log = config['LOGGING']['job_log']

home_dir = config['DIRECTORIES']['home_dir']
util_dir = config['DIRECTORIES']['util_dir']
script_dir = config['DIRECTORIES']['script_dir']
log_dir = config['DIRECTORIES']['log_dir']
data_dir = config['DIRECTORIES']['data_dir']
sql_dir = config['DIRECTORIES']['sql_dir']
backup_dir = config['DIRECTORIES']['backup_dir']
param_dir = config['DIRECTORIES']['param_dir']

database = config['DATABASE']['database']
userid = config['DATABASE']['userid']
password = config['DATABASE']['password']
hostname = config['DATABASE']['hostname']
port = config['DATABASE']['port']


WhlslFile = config['FILES']['WhlslFile']
DlrFile = config['FILES']['DlrFile']

file_server = config['DEFAULT']['file_server']
axway_server = config['DEFAULT']['axway_server']

email_addresses = [config["MAILING LIST"]["email_addresses"].split(",")]