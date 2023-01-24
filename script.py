import os
import sys
import datetime

job_log = os.path.join(os.path.dirname(__file__), 'job.log')  ## This is the log file
def write_startup_message(message):
    with open(job_log, 'a') as f:
        f.write("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\n")
        f.write(message + "\n")

def initialize():
    curr_date = datetime.datetime.now().strftime("%y%m%d")
    util_dir = os.path.join(os.environ['HOME'], 'utils')
    os.chdir(os.environ['HOME'])
    os.system(f"{util_dir}/SetupDirectories {os.path.join(os.environ['HOME'], 'CVP/Scripts')}")
    os.system(f"{util_dir}/SetupEnvironment")
    os.system(f"{util_dir}/SetupJob cp_billing")
    if not os.path.isfile(job_log):
        os.remove(job_log)
    with open(job_log, 'a') as f:
        f.write(f"{os.path.basename(__file__)} - Started\n")
        f.write(f"{job_name} Started - Process id = {os.getpid()}\n")
        f.write(f"{job_name} runs following steps:\n")
        f.write("   1) LoadWholesaleFile\n")
        f.write("   2) LoadDealersInfo\n")
        f.write("   3) ProcessWholesaleFile\n")
        f.write("   4) BackupTransactions\n")
        f.write("   5) UpdateTransactions\n")
        f.write("   6) CreateBillingFile\n")
        f.write("   7) CreateAccountingReport\n")
        f.write("   8) EncryptBillingFile\n")
        f.write("   9) SendBillingFile\n")
        f.write("  10) SendReports\n")
        f.write("  11) BackupFiles\n")
    file_exists = 1
    return_code = os.system(f"{util_dir}/SetupUDBEnvironment")
    if return_code > 0:
        process_messages(1)
    global whlsl_file, dlr_file
    whlsl_file = "WSMONTHLYUNIT.TXT"
    dlr_file = "dealers_info.txt"
    open(os.path.join(data_dir, 'dummy'), 'w').close()

def validate_files():
    pass

import os

def process_messages(error_code, database=None):
    util_dir = os.path.join(os.environ['HOME'], 'utils')
    if error_code == 0:
        os.system(f"{util_dir}/SendEmail {__file__} 'Completed Successfully' {job_log}")
    elif error_code == 1:
        with open(job_log, 'a') as f:
            f.write("ERROR: Setting up UDB environment\n")
        os.system(f"{util_dir}/SendEmail {__file__} 'ERROR: Setting up UDB environment' {job_log}")
        exit(1)
    elif error_code == 2:
        with open(job_log, 'a') as f:
            f.write("ERROR: Missing Wholesale file\n")
            f.write("Aborting ...\n")
        os.system(f"{util_dir}/SendEmail {__file__} 'ERROR: Missing Wholesale file' {job_log}")
        exit(1)
    elif error_code == 3:
        with open(job_log, 'a') as f:
            f.write(f"ERROR: Connecting to database: {database}\n")
            f.write("Aborting ...\n")
        os.system(f"{util_dir}/SendEmail {__file__} 'ERROR: Connecting to database: {database}' {job_log}")
        exit(1)
    elif error_code == 4:
        with open(job_log, 'a') as f:
            f.write(f"ERROR: Running {sys.argv[2]}\n")
            f.write("Aborting ...\n")
        os.system(f"{util_dir}/SendEmail {__file__} 'ERROR: Running {sys.argv[2]}' {job_log}")
        exit(1)
    elif error_code == 5:
        with open(job_log, 'a') as f:
            f.write("ERROR: Running sql script\n")
            f.write("Aborting ...\n")
        os.system(f"{util_dir}/SendEmail {__file__} 'ERROR: Running Process_Wholesale.sql' {job_log}")
        exit(1)
    elif error_code == 6:
        with open(job_log, 'a') as f:
            f.write("ERROR: Encrypting the file\n")
            f.write("Aborting ...\n")
        os.system(f"{util_dir}/SendEmail {__file__} 'ERROR: Encrypting the file' {job_log}")
        exit(1)
    elif error_code == 7:
        with open(job_log, 'a') as f:
            f.write("ERROR: FTP the file to Axway\n")
            f.write("Aborting ...\n")
        os.system(f"{util_dir}/SendEmail {__file__} 'ERROR: FTP the file to Axway' {job_log}")
        exit(1)
    elif error_code == 8:
        with open(job_log, 'a') as f:
            f.write("ERROR: FTP the file to W drive\n")
            f.write("Aborting ...\n")
        os.system(f"{util_dir}/SendEmail {__file__} 'ERROR: FTP the file to W drive' {job_log}")
        exit(1)


