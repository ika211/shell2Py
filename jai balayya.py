import sys

def wait_for_file():
    print("wait_for_file")

def load_wholesale_file():
    print("load_wholesale_file")

def load_dealers_info():
    print("load_dealers_info")

def process_wholesale_file():
    print("process_wholesale_file")

def backup_transactions():
    print("backup_transactions")

def update_transactions():
    print("update_transactions")

def create_billing_file():
    print("create_billing_file")

def create_accounting_report():
    print("create_accounting_report")

def encrypt_billing_file():
    print("encrypt_billing_file")

def send_billing_file():
    print("send_billing_file")

def send_reports():
    print("send_reports")

def backup_files():
    print("backup_files")

def append_to_log(message):
    with open(job_log, 'a') as f:
        f.write(message)

skipstep = 0
job_log = "job.log"
print(sys.argv)
if len(sys.argv) > 1:
    skipstep = int(sys.argv[1])

if skipstep <= 1:
    wait_for_file()
    load_wholesale_file()
else:
    append_to_log("Skipping Step: LoadWholesaleFile\n")

if skipstep <= 2:
    load_dealers_info()
else:
    append_to_log("Skipping Step: LoadDealersInfo\n")

if skipstep <= 3:
    process_wholesale_file()
else:
    append_to_log("Skipping Step: ProcessWholesaleFile\n")

if skipstep <= 4:
    backup_transactions()
else:
    append_to_log("Skipping Step: BackupTransactions\n")

if skipstep <= 5:
    update_transactions()
else:
    append_to_log("Skipping Step: UpdateTransactions\n")

if skipstep <= 6:
    create_billing_file()
else:
    append_to_log("Skipping Step: CreateBillingFile\n")

if skipstep <= 7:
    create_accounting_report()
else:
    append_to_log("Skipping Step: CreateAccountingReport\n")

if skipstep <= 8:
    encrypt_billing_file()
else:
    append_to_log("Skipping Step: EncryptBillingFile\n")

if skipstep <= 9:
    send_billing_file()
else:
    append_to_log("Skipping Step: SendBillingFile\n")

if skipstep <= 10:
    send_reports()
else:
    append_to_log("Skipping Step: SendReports\n")

if skipstep <= 11:
    backup_files()
else:
    append_to_log("Skipping Step: BackupFiles\n")