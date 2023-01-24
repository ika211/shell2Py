import os
import sys
import datetime


def write_log(message, logpath):
    with open(logpath, 'a') as f:
        f.write(message)
def write_startup_message(message, logpath):
    write_log("============================================================")
    write_log(message)

def send_email(script_name, message, job_log):
    pass

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

def wait_for_file():
    pass

def load_wholesale_file():
    pass

def load_dealers_info():
    pass

def process_wholesale_file():
    pass

def backup_transactions():
    pass

def update_transactions():
    pass

def create_billing_file():
    pass

def create_accounting_report():
    pass

def encrypt_billing_file():
    pass

def send_billing_file():
    pass

def send_reports():
    pass

def backup_files():
    pass

def finalize():
    pass

def process_messages(error_code, sql_script=None,database=None):
    if error_code == 0:
        send_email(sys.argv[0], "Completed Successfully", job_log)
    elif error_code == 1:
        write_log("ERROR: Setting up UDB environment", job_log)
        send_email(sys.argv[0], "ERROR: Setting up UDB environment", job_log)
        exit(1)
    elif error_code == 2:
        write_log("ERROR: Loading wholesale file", job_log)
        send_email(sys.argv[0], "ERROR: Loading wholesale file", job_log)
        exit(1)
    elif error_code == 3:
        write_log(f"ERROR: Connecting to database {database}", job_log)
        write_log("Aborting ...", job_log)
        send_email(sys.argv[0], f"ERROR: Connecting to database {database}", job_log)
        exit(1)
    elif error_code == 4:
        write_log(f"ERROR: Running {sql_script}", job_log)
        write_log("Aborting ...", job_log)
        send_email(sys.argv[0], f"ERROR: Running {sql_script}", job_log)
        exit(1)
    elif error_code == 5:
        write_log("ERROR: Running Process_Wholesale.sql", job_log)
        write_log("Aborting ...", job_log)
        send_email(sys.argv[0], "ERROR: Running Process_Wholesale.sql", job_log)
        exit(1)
    elif error_code == 6:
        write_log("ERROR: Encrypting the file", job_log)
        write_log("Aborting ...", job_log)
        send_email(sys.argv[0], "ERROR: Encrypting the file", job_log)
        exit(1)
    elif error_code == 7:
        write_log("ERROR: FTP the file to Axway", job_log)
        write_log("Aborting ...", job_log)
        send_email(sys.argv[0], "ERROR: FTP the file to Axway", job_log)
        exit(1)
    elif error_code == 8:
        write_log("ERROR: FTP the file to W drive", job_log)
        write_log("Aborting ...", job_log)
        send_email(sys.argv[0], "ERROR: FTP the file to W drive", job_log)
        exit(1)


if __name__ == '__main__':
    job_log = "joblog"  # remove
    job_name = "temp"  # remove
    data_dir = os.path.join(os.environ['HOME'], 'data')  # remove

    skipstep = 0
    print(sys.argv)
    if len(sys.argv) > 1:
        skipstep = int(sys.argv[1]) - 1

    # initialize(sys.argv[1]) ## doubt

    steps = ["load_wholesale_file", "load_dealers_info", "process_wholesale_file", "backup_transactions",
             "update_transactions", "create_billing_file", "create_accounting_report", "encrypt_billing_file",
             "send_billing_file", "send_reports", "backup_files"]

    for i in range(0, len(steps)):
        if skipstep <= i:
            func = globals().get(steps[i])
            if i == 0:
                wait_for_file()
            func()
        else:
            write_log("Skipping Step: {}\n".format(steps[i]), job_log)

    finalize()
    sys.exit(0)