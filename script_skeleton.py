import os
import sys
import datetime


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

def process_messages(error_code, database=None):
    pass

def append_to_log(message):
    with open(job_log, 'a') as f:
        f.write(message)

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
            append_to_log("Skipping Step: {}\n".format(steps[i]))

    finalize()
    sys.exit(0)