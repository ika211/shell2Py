import os
import sys
import shutil
import datetime
import subprocess

def write_log(message, logpath):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(logpath, 'a') as f:
        f.write(f'{timestamp} - {message}')

def write_startup_message(message, logpath):
    write_log("============================================================", logpath)
    write_log(message, logpath)

def send_email(script_name, message, job_log):
    subprocess.run([f"{util_dir}/SendEmail", f"{script_name}", f"{message}", f"{job_log}"])

def initialize():
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
    return_code = os.system(f"{util_dir}/SetupUDBEnvironment")
    if return_code > 0:
        process_messages(1)
    global whlsl_file, dlr_file
    whlsl_file = "WSMONTHLYUNIT.TXT"
    dlr_file = "dealers_info.txt"
    open(os.path.join(data_dir, 'dummy'), 'w').close()

def validate_files():
    src = '/home/SVC/dcicftp/Wholesale/Inbound/WSMONTHLYUNIT.TXT'
    dst = '/apps/ims/df5731b/CVP/WSMONTHLYUNIT.TXT'
    shutil.move(src, dst)
    if not os.path.isfile(os.path.join(data_dir, whlsl_file)):
        write_log(f"Missing {whlsl_file} file", job_log)
        return False
    return False


def wait_for_file():
    file_exists = validate_files()
    if not file_exists:
        process_messages(2)
    src = f'{os.environ["HOME"]}/CVP/{whlsl_file}'   ## check again
    dst = f'{data_dir}'
    shutil.move(src, dst)


def load_wholesale_file():
    write_startup_message("load_wholesale_file Started", job_log)
    cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase",f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0:
        process_messages(3, database=database)

    subprocess.run(["db2", "-stvf", "f{sql_dir}/Load_Wholesale_Monthly.sql", ">", "$$.log"])
    subprocess.run(["cat", "$$.log", f"{job_log}"])
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|020000", "$$.log"])
    if cmd4.returncode >= 1:
        process_messages(4, sql_script="Load_Wholesale_Monthly.sql")
    cmd5 = subprocess.run([f"{util_dir}/CheckSqlError", "^SQL", "SQL3107W", "$$.log"])
    if cmd5.returncode >= 1:
        process_messages(4, sql_script="Load_Wholesale_Monthly.sql")
    write_startup_message("load_wholesale_file Ended", job_log)

def process_wholesale_file():
    write_startup_message("process_wholesale_file Started", job_log)
    cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0:
        process_messages(3, database=database)

    subprocess.run(["db2", "-stvf", "f{sql_dir}/Process_Wholesale_Monthly.sql", ">", "$$.log"])
    subprocess.run(["cat", "$$.log", f"{job_log}"])
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|020000", "$$.log"])
    if cmd4.returncode >= 1:
        process_messages(5)
    # write_startup_message("process_wholesale_file Ended", job_log)
    write_log("process_wholesale_file Ended", job_log)

def load_dealers_info():
    write_startup_message("load_dealers_info Started", job_log)
    cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0:
        process_messages(3, database=database)

    subprocess.run(["db2", "-stvf", "f{sql_dir}/Extract_Dealers.sql", "-x", "-r", f"{data_dir}/dealers_infos.txt",
                    ">", "$$.log"])
    subprocess.run(["cat", "$$.log", f"{job_log}"])
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|020000", "$$.log"])
    if cmd4.returncode >= 1:
        process_messages(4, sql_script="Extract_Dealers.sql")
    cmd5 = subprocess.run([f"{util_dir}/CheckSqlError", "^SQL", "SQL3107W", "$$.log"])
    if cmd5.returncode >= 1:
        process_messages(4, sql_script="Extract_Dealers.sql")
    write_startup_message("load_dealers_info Ended", job_log)

    cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0:
        process_messages(3, database=database)

    # load dealer details into dealer-temp
    subprocess.run(["db2", "-stvf", "f{sql_dir}/Backup_Dealers.sql", ">", "/apps/ims/df5731b/CVP/Backup_Dealers.log"])
    subprocess.run(["db2", "-stvf", "f{sql_dir}/Load_Dealers.sql", ">", "$$.log"])
    subprocess.run(["cat", "$$.log", f"{job_log}"])
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|020000", "$$.log"])
    if cmd4.returncode >= 1:
        process_messages(4, sql_script="Load_Dealers.sql")
    cmd5 = subprocess.run([f"{util_dir}/CheckSqlError", "^SQL", "SQL3107W", "$$.log"])
    if cmd5.returncode >= 1:
        process_messages(4, sql_script="Load_Dealers.sql")

    # Call update CVP dealer stored procedure
    subprocess.run(["db2", "-stvf", "f{sql_dir}/Update_CVP_Dealer.sql", ">",
                    "/apps/ims/df5731b/CVP/Update_CVP_Dealer.log"])
    # write_startup_message("update_dealers_info Ended", job_log)
    write_log("update_dealers_info Ended", job_log)

def backup_transactions():
    write_startup_message("backup_transactions Started", job_log)
    cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0:
        process_messages(3, database=database)

    subprocess.run(["db2", "-stvf", "f{sql_dir}/Backup_Transactions.sql", ">", "$$.log"])
    subprocess.run(["cat", "$$.log", f"{job_log}"])
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|020000", "$$.log"])
    if cmd4.returncode >= 1:
        process_messages(4, sql_script="Backup_Transactions.sql")

    cmd5 = subprocess.run([f"{util_dir}/CheckSqlError", "^SQL", "SQL3107W", "$$.log"])
    if cmd5.returncode >= 1:
        process_messages(4, sql_script="Backup_Transactions.sql")
    write_log("backup_transactions Ended", job_log)

def update_transactions():
    write_startup_message("update_transactions Started", job_log)
    cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0:
        process_messages(3, database=database)

    subprocess.run(["db2", "-stvf", f"{sql_dir}/Update_Transactions.sql", ">", "$$.log"])
    subprocess.run(["cat", "$$.log", f"{job_log}"])
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|020000", "$$.log"])
    if cmd4.returncode >= 1:
        process_messages(4, sql_script="Update_Transactions.sql")
    cmd5 = subprocess.run([f"{util_dir}/CheckSqlError", "^SQL", "SQL3107W", "$$.log"])
    if cmd5.returncode >= 1:
        process_messages(4, sql_script="Update_Transactions.sql")
    write_log("update_transactions Ended", job_log)

def create_billing_file():
    write_startup_message("create_billing_file Started", job_log)
    cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0:
        process_messages(3, database=database)

    subprocess.run(["db2", "-stvf", f"{sql_dir}/Create_Billing_File.sql", ">", "$$.log"])
    subprocess.run(["cat", "$$.log", f"{job_log}"])
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|020000", "$$.log"])
    if cmd4.returncode >= 1:
        process_messages(4, sql_script="Create_Billing_File.sql")
    cmd5 = subprocess.run([f"{util_dir}/CheckSqlError", "^SQL", "SQL3107W", "$$.log"])
    # if cmd5.returncode >= 1:
    #     process_messages(4, sql_script="Create_Billing_File.sql")

    #
    # code missing
    #

    write_log("create_billing_file Ended", job_log)

def create_accounting_report():
    write_startup_message("create_accounting_report Started", job_log)
    cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0:
        process_messages(3, database=database)

    subprocess.run(["db2", "-stvf", f"{sql_dir}/Create_Accounting_Report.sql", ">", "$$.log"])
    subprocess.run(["cat", "$$.log", f"{job_log}"])
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|020000", "$$.log"])
    if cmd4.returncode >= 1:
        process_messages(4, sql_script="Create_Accounting_Report.sql")
    cmd5 = subprocess.run([f"{util_dir}/CheckSqlError", "^SQL", "SQL3107W", "$$.log"])
    # if cmd5.returncode >= 1:
    #     process_messages(4, sql_script="Create_Accounting_Report.sql")

    # code missing

    write_log("create_accounting_report Ended", job_log)

def encrypt_billing_file():
    write_startup_message("encrypt_billing_file Started", job_log)
    # cmd = subprocess.run([".", f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    # if cmd.returncode > 0:
    #     process_messages(3, database=database)

    cmd = subprocess.run([".", f"{util_dir}/Encrypt", f"{data_dir}/CVP_monthly.csv"])
    if cmd.returncode >= 1:
        process_messages(6)

    write_log("encrypt_billing_file Ended", job_log)


def send_billing_file():
    write_startup_message("send_billing_file Started", job_log)
    cmd = subprocess.run([".", f"{util_dir}/Ftp", f"{AxwayServer}", f"{parm_dir}/send_to_Axway.ftp",
                          "ftp_$$.msg", "ftp_$$.err"])
    ftp_error = cmd.returncode

    subprocess.run(["cat", "ftp_$$.msg"], stdout=open(f"{job_log}", "a"))
    subprocess.run(["cat", "ftp_$$.err"], stdout=open(f"{job_log}", "a"))

    subprocess.run(["rm", "ftp_$$.msg"])
    subprocess.run(["rm", "ftp_$$.err"])

    if ftp_error == 1:
        process_messages(7)

    write_log("send_billing_file Ended", job_log)

def send_reports():
    write_startup_message("send_reports Started", job_log)

    shutil.move(f"{data_dir}/CVP_Exception", f"{data_dir}/CVP_Exception.{curr_date}.csv")
    shutil.move(f"{data_dir}/CVP_Accounting", f"{data_dir}/CVP_Accounting.{curr_date}.csv")

    cmd = subprocess.run([f"{util_dir}/Ftp", f"{file_server}", f"{parm_dir}/send_to_Wdrive.ftp",
                            "ftp_$$.msg", "ftp_$$.err"])
    ftp_error = cmd.returncode

    subprocess.run(["cat", "ftp_$$.msg"], stdout=open(f"{job_log}", "a"))
    subprocess.run(["cat", "ftp_$$.err"], stdout=open(f"{job_log}", "a"))

    subprocess.run(["rm", "ftp_$$.msg"])
    subprocess.run(["rm", "ftp_$$.err"])

    if ftp_error == 1:
        process_messages(7)

    write_log("send_reports Ended", job_log)

def backup_files():
    write_startup_message("backup_files Started", job_log)

    # Wholesale file backup
    os.rename(f'{backup_dir}/{WhlslFile}.2.gz', f'{backup_dir}/{WhlslFile}.3.gz')
    os.rename(f'{backup_dir}/{WhlslFile}.1.gz', f'{backup_dir}/{WhlslFile}.2.gz')
    os.rename(f'{backup_dir}/{WhlslFile}', f'{backup_dir}/{WhlslFile}.1')
    shutil.make_archive(f'{backup_dir}/{WhlslFile}.1', 'gz', f'{backup_dir}/{WhlslFile}.1')

    # Dealer File backup
    os.rename(f'{backup_dir}/{DlrFile}.2.gz', f'{backup_dir}/{DlrFile}.3.gz')
    os.rename(f'{backup_dir}/{DlrFile}.1.gz', f'{backup_dir}/{DlrFile}.2.gz')
    os.rename(f'{backup_dir}/{DlrFile}', f'{backup_dir}/{DlrFile}.1')
    shutil.make_archive(f'{backup_dir}/{DlrFile}.1', 'gz', f'{backup_dir}/{DlrFile}.1')

    # CVP Transactions backup
    os.rename(f'{backup_dir}/ins_whsl_cvp_transaction.txt.1.gz', f'{backup_dir}/ins_whsl_cvp_transaction.txt.2.gz')
    os.rename(f'{backup_dir}/ins_whsl_cvp_transaction.txt', f'{backup_dir}/ins_whsl_cvp_transaction.txt.1')
    shutil.make_archive(f'{backup_dir}/ins_whsl_cvp_transaction.txt.1', 'gz', f'{backup_dir}/ins_whsl_cvp_transaction.txt.1')

    # Delete temporary files
    subprocess.run(["rm", f"{data_dir}/CVP_Monthly_Data.txt"])
    subprocess.run(["rm", f"{data_dir}/CVP_Exception_Data"])
    subprocess.run(["rm", f"{data_dir}/CVP_Accounting_Data"])
    subprocess.run(["rm", f"{data_dir}/CVP_monthly.txt.pgp"])
    subprocess.run(["rm", "null.log"])

    # Billing file backup
    shutil.move(f"{data_dir}/CVP_Monthly.txt", f"{backup_dir}/CVP_Monthly.txt.{curr_date}")
    shutil.move(f"{data_dir}/CVP_Exception.{curr_date}.csv", f"{backup_dir}/CVP_Exception.{curr_date}.csv")
    shutil.move(f"{data_dir}/CVP_Accounting.{curr_date}.csv", f"{backup_dir}/CVP_Accounting.{curr_date}.csv")
    shutil.make_archive(f"{backup_dir}/CVP_Monthly.txt.{curr_date}", "gz", f"{backup_dir}/CVP_Monthly.txt.{curr_date}")
    shutil.make_archive(f"{backup_dir}/CVP_Exception.{curr_date}.csv", "gz", f"{backup_dir}/CVP_Exception.{curr_date}.csv")
    shutil.make_archive(f"{backup_dir}/CVP_Accounting.{curr_date}.csv", "gz", f"{backup_dir}/CVP_Accounting.{curr_date}.csv")

    write_log("backup_files Ended", job_log)

def update_statistics():
    write_startup_message("update_statistics Started", job_log)

    cmd = subprocess.run([f"{util_dir}/ConnectToDatabase", f"{database}", f"{userid}", f"{password}"])
    if cmd.returncode > 0 :
        process_messages(3, database=database)
    pid = os.getpid()
    subprocess.run(["db2","-stvf", f"{sql_dir}/Update_Statistics.sql"], stdout=open(f"{pid}.log", "w"))
    subprocess.run(["cat", f"{pid}.log"], stdout=open("job.log", "a"))
    cmd4 = subprocess.run([f"{util_dir}/CheckSqlError", "SQLSTATE", "00000|20000", f"{pid}.log"])
    if cmd4.returncode >= 1:
        process_messages(4, sql_script="Update_Statistics.sql")
    cmd5 = subprocess.run([f"{util_dir}/CheckSqlError", "^SQL", "SQL3107W", f"{pid}.log"])
    if cmd5.returncode >= 1:
        process_messages(4, sql_script="Update_Statistics.sql")

    subprocess.run(["rm", f"{pid}.log"])
    write_log("update_statistics Ended", job_log)

def finalize():
    update_statistics()
    subprocess.run(["rm", f"{data_dir}/dummy"])
    write_log(f"{job_name} Ended", job_log)
    write_log(f"{os.path.basename(__file__)} Ended", job_log)
    process_messages(0)

    # Log backup


def process_messages(error_code, sql_script=None, database=None):
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
    # add variable values to os.environ to share variables across python and shell
    global whlsl_file, dlr_file, util_dir, sql_dir, database, userid, password
    global AxwayServer, parm_dir, file_server, backup_dir, WhlslFile, DlrFile

    curr_date = datetime.datetime.now().strftime("%Y%m%d")
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