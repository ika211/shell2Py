########################################################################################
#
#  Proprietary of -----------------
#***************************************************************************************
# cp_billing.sh
#***************************************************************************************
# Description:           creates CVP billing file
#
# System:                IMS
# Frequency:             Monthly
# Arguments for script:  None
#
# Steps:                 (1) Import Wholesale file into temp table (LoadWholesaleFile)
#                        (2) Process Wholesale file                (ProcessWholesaleFile)
#                        (3) Backup Wholesale file                 (BackupWholesaleFile)
#
# Note:                  Change parameters passed to SetupUDBEnvironment in
#                        Initialize function
#                        Change parameters passed to SetupDirectories in
#                        Initialize function
#                        Change parameters passed to SetupJob in
#                        Initialize function
#                        Change database name in Initialize
#***************************************************************************************

WriteStartupMessage()
{
  $utilDir/WriteLog "= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="
  $utilDir/WriteLog "$1" $/$jobLog
}
echo "$ is :: $"
echo "$jobLog is:: $jobLog"
Initialize()
{
  dateTime=$(date '+%m/%d/%Y %H:%M:%S')
  currDate=$(date '+%y%m%d')
  utilDir=$HOME/utils
  . $utilDir/SetupDirectories $HOME/CVP/Scripts
  . $utilDir/SetupEnvironment
  . $utilDir/SetupJob "cp_billing"
  $utilDir/CheckFileExistence $/$jobLog
  if [ $? -eq 1 ]
    then rm $/$jobLog
  fi
  $utilDir/WriteLog "$0 - Started" $/$jobs_log.log
  $utilDir/WriteLog "$jobName Started - Process id = $$" $/$jobLog
  $utilDir/WriteLog "$jobName runs following steps:" $/$jobLog
  $utilDir/WriteLog "   1) LoadWholesaleFile" $/$jobLog
  $utilDir/WriteLog "   2) LoadDealersInfo" $/$jobLog
  $utilDir/WriteLog "   3) ProcessWholesaleFile" $/$jobLog
  $utilDir/WriteLog "   4) BackupTransactions" $/$jobLog
  $utilDir/WriteLog "   5) UpdateTransactions" $/$jobLog
  $utilDir/WriteLog "   6) CreateBillingFile" $/$jobLog
  $utilDir/WriteLog "   7) CreateAccountingReport" $/$jobLog
  $utilDir/WriteLog "   8) EncryptBillingFile" $/$jobLog
  $utilDir/WriteLog "   9) SendBillingFile" $/$jobLog
  $utilDir/WriteLog "  10) SendReports" $/$jobLog
  $utilDir/WriteLog "  11) BackupFiles" $/$jobLog
  fileExists=1
  . $utilDir/SetupUDBEnvironment
  if [ $? -gt 0 ]
    then ProcessMessages 1
  fi
  whlslFile=WSMONTHLYUNIT.TXT
  DlrFile=dealers_info.txt
  echo '' > $dataDir/dummy
  cd $homeDir
}

ValidateFiles()
{
  mv /home/SVC/dcicftp/Wholesale/Inbound/WSMONTHLYUNIT.TXT /apps/ims/df5731b/CVP/WSMONTHLYUNIT.TXT
  $utilDir/CheckFileExistence $dataDir/$whlslFile
  if [ $? -ne 1 ]
    then
      # shellcheck disable=SC2086
      $utilDir/WriteLog "Missing $whlslFile file" $/$jobLog
      fileExists=0
  fi
}

ProcessMessages()
{
  if [ $1 -eq 0 ]
    then
      $utilDir/SendEmail  $0 "Completed Successfully" $/$jobLog
  fi

  if [ $1 -eq 1 ]
    then
      $utilDir/WriteLog "ERROR: Setting up UDB environment" $/$jobLog
      $utilDir/SendEmail  $0 "ERROR: Setting up UDB environment" $/$jobLog
      exit 1
  fi

  if [ $1 -eq 2 ]
    then
      $utilDir/WriteLog "ERROR: Missing Wholesale file" $/$jobLog
      $utilDir/WriteLog "Aborting ..." $/$jobLog
      $utilDir/SendEmail  $0 "ERROR: Missing Wholesale file" $/$jobLog
      exit 1
  fi

  if [ $1 -eq 3 ]
    then
      $utilDir/WriteLog "ERROR: Connecting to database: $database" $/$jobLog
      $utilDir/WriteLog "Aborting ..." $/$jobLog
      $utilDir/SendEmail  $0 "ERROR: Connecting to database: $database" $/$jobLog
      exit 1
  fi

  if [ $1 -eq 4 ]
    then
      $utilDir/WriteLog "ERROR: Running $2" $/$jobLog
      $utilDir/WriteLog "Aborting ..." $/$jobLog
      $utilDir/SendEmail  $0 "ERROR: Running $2" $/$jobLog
      exit 1
  fi

  if [ $1 -eq 5 ]
    then
      $utilDir/WriteLog "ERROR: Running sql script" $/$jobLog
      $utilDir/WriteLog "Aborting ..." $/$jobLog
      $utilDir/SendEmail  $0 "ERROR: Running Process_Wholesale.sql" $/$jobLog
      exit 1
  fi

  if [ $1 -eq 6 ]
    then
      $utilDir/WriteLog "ERROR: Encrypting the file" $/$jobLog
      $utilDir/WriteLog "Aborting ..." $/$jobLog
      $utilDir/SendEmail  $0 "ERROR: Encrypting the file" $/$jobLog
      exit 1
  fi

  if [ $1 -eq 7 ]
    then
      $utilDir/WriteLog "ERROR: FTP the file to Axway" $/$jobLog
      $utilDir/WriteLog "Aborting ..." $/$jobLog
      $utilDir/SendEmail  $0 "ERROR: FTP the file to Axway" $/$jobLog
      exit 1
  fi

  if [ $1 -eq 8 ]
    then
      $utilDir/WriteLog "ERROR: FTP the file to W drive" $/$jobLog
      $utilDir/WriteLog "Aborting ..." $/$jobLog
      $utilDir/SendEmail  $0 "ERROR: FTP the file to W drive" $/$jobLog
      exit 1
  fi

}

WaitForFile()
{
  ValidateFiles
  if [ $fileExists -eq 0 ]
    then ProcessMessages 2
  fi
  mv $HOME/CVP/$whlslFile $dataDir
}

LoadWholesaleFile()
{
  $utilDir/WriteLog "LoadWholesaleFile Started" $/$jobLog
  . $utilDir/ConnectToDatabase $database $userid $password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  db2 -stvf $sqlDir/Load_Wholesale_Monthly.sql > $/$$.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 "Load_Wholesale_Monthly.sql"
  fi
  $utilDir/CheckSqlError "^SQL" "SQL3107W" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 "Load_Wholesale_Monthly.sql"
  fi

  $utilDir/WriteLog "LoadWholesaleFile Ended" $/$jobLog
}

ProcessWholesaleFile()
{
  WriteStartupMessage "ProcessWholesaleFile Started" $/$jobLog
  . $utilDir/ConnectToDatabase $database $userid $password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  db2 -stvf $sqlDir/Process_Wholesale_Monthly.sql > $/$$.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 5
  fi

  WriteStartupMessage "ProcessWholesaleFile Ended" $/$jobLog
}

LoadDealersInfo()
{
  WriteStartupMessage "LoadDealersInfo Started" $/$jobLog
  . $utilDir/ConnectToDatabase $mf_database $mf_userid $mf_password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  db2 -stf $sqlDir/Extract_Dealers.sql -x -r $dataDir/dealers_infos.txt > $/null.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 "Extract_Dealers.sql"
  fi
  $utilDir/CheckSqlError "^SQL" "SQL3107W" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 "Extract_Dealers.sql"
  fi

  . $utilDir/ConnectToDatabase $database $userid $password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  ####load dealerdetail into dealertemp
  db2 -stvf $sqlDir/Backup_Dealers.sql > /apps/ims/df5731b/CVP/Backup_Dealers.log
  db2 -stvf $sqlDir/Load_Dealers.sql > $/$$.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 "Load_Dealers.sql"
  fi
  $utilDir/CheckSqlError "^SQL" "SQL3107W" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 "Load_Dealers.sql"
  fi
  #### Call update CVP dealer stored procedure
  db2 -stvf $sqlDir/Update_CVP_Dealer.sql > /apps/ims/df5731b/CVP/Update_CVP_Dealer.log

  $utilDir/Writelog "LoadDealersInfo Ended" $/$jobLog
}

BackupTransactions()
{
  WriteStartupMessage "BackupTransactions Started"
  . $utilDir/ConnectToDatabase $database $userid $password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  db2 -stvf $sqlDir/Backup_Transactions.sql > $/$$.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 Backup_Transactions.sql
  fi

  SutilDir/ChecksqlError "^SQL" "SQL3107W" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 Backup_Transactions.sql
  fi

  $utilDir/WriteLog "BackupTransactions Ended" $/$jobLog
}

UpdateTransactions ()
{
  WriteStartupMessage "UpdateTransactions Started"
  . $utilDir/ConnectToDatabase $database $userid $password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  db2 -stvf $sq1Dir/Update_Transactions.sq1 > $/$$.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 Update_Transactions.sq1
  fi
  SutilDir/ChecksqlError "^SQL" "SQL3107W" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 Update_Transactions.sql
  fi

  $utilDir/WriteLog "UpdateTransactions Ended" $/$jobLog
}

CreatingBillingFile()
{
  WriteStartupMessage " CreateBillingFile Started"
  . $utilDir/ConnectToDatabase $database $userid $password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  db2 -stvf $sq1Dir/Create_Billing_File.sq1 > $/$$.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -gt 1 ]
    then ProcessMessages 4 Create_Billing_File.sq1
  fi
  $utilDir/CheckSqlError "^SQL" "SQL3107W" $/$$.log
#  if [ $? -gt 1 ]
#    then ProcessMessages 4 Create_Billing_File.sq1
#  fi
  cat $dataDir/headers/CVP_Monthly_Header.txt $dataDir/CVP_Monthly_Data.txt > $dataDir #line cut off
  cat $dataDir/headers/CVP_Monthly_Header.txt $dataDir/CVP_Monthly_Data.txt > $dataDir #line cut off
  cat $dataDir/headers/CVP_Exception_Header.txt $dataDir/CVP_Exception_Data > $dataDi #line cut off
  $utilDir/WriteLog "CreateBillingFile Ended" $/$jobLog
}

CreateAccountingReport()
{
  WriteStartupMessage "CreateAccountingReport Started"
  . $utilDir/ConnectToDatabase $database $userid $password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  db2 -stvf $sq1Dir/Create_Accounting_Report.sq1 > $/$$.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 Create_Accounting_Report.sq1
  fi
  $utilDir/CheckSqlError "^SQL" "SQL3107W" $/$$.log
#  if [ $? -ge 1 ]
#    then ProcessMessages 4 Create_Accounting_Report.sq1
#  fi

  cat $dataDir/headers/CVP_Accounting_Header.txt $dataDir/CVP_Accounting_Data > $dataDir #line cut off

  $utilDir/WriteLog "CreateAccountingReport Ended" $/$jobLog
}

EncryptBillingFile()
{
  WriteStartupMessage "EncryptBillingFile Started"
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  $utilDir/Encrypt $dataDir/CVP_monthly.csv

  if [ $? -ge 1 ]
    then ProcessMessages 6
  fi

  $utilDir/WriteLog "EncryptBillingFile Ended" $/$jobLog
####$utilDir/ftp.monthly.sh
}

SendBillingFile()
{
  WriteStartupMessage "SendBillingFile Started"
  $utilDir/Ftp $AxwayServer $parmDir/send_to_Axway.ftp $/ftp_$$.msg $/ftp_$$.err
  ftpError=$?

  cat $/ftp_$$.msg >> $/$jobLog
  cat $/ftp_$$.err >> $/$jobLog

  rm $/ftp_$$.msg
  rm $/ftp_$$.err

  if [ $ftpError -eq 1 ]
    then
    ProcessMessages 7
  fi

  $utilDir/WriteLog "SendBillingFile Ended" $/$jobLog
}

SendReports()
{
  WriteStartupMessage "SendReports Started"

    mv $dataDir/CVP_Exception $dataDir/CVP_Exception.${currDate}.csv
    mv $dataDir/CVP_Accounting $dataDir/CVP_Accounting.${currDate}.csv

    $utilDir/Ftp $FileServer $parmDir/send_to_Wdrive.ftp $/ftp_$$.msg $/ftp_$$.err
    ftpError=$?

    cat $/ftp_$$.msg >> $/$jobLog
    cat $/ftp_$$.err >> $/$jobLog

    rm $/ftp_$$.msg
    rm $/ftp_$$.err

    if [ $ftpError -eq 1 ]
      then
      ProcessMessages 7
    fi

  $utilDir/WriteLog "SendReports Ended" $/$jobLog
}

BackupFiles()
{
  # Wholesale file backup
  mv $backDir/${WhlslFile}.2.gz $backDir/${WhlslFile}.3.gz
  mv $backDir/${WhlslFile}.1.gz $backDir/${WhlslFile}.2.gz
  mv $backDir/${WhlslFile} $backDir/${WhlslFile}.1
  gzip $backDir/${WhlslFile}.1

  # Dealer Info file backup
  mv $backDir/${DlrFile}.2.gz $backDir/${DlrFile}.3.gz
  mv $backDir/${DlrFile}.1.gz $backDir/${DlrFile}.2.gz
  mv $backDir/${DlrFile} $backDir/${DlrFile}.1
  gzip $backDir/${DlrFile}.1

  # CVP Transactions file backup
  mv $backDir/ins_whsl_cvp_transaction.txt.1.gz $backDir/ins_whsl_cvp_transaction.txt.2.gz
  mv $backDir/ins_whsl_cvp_transaction.txt $backDir/ins_whsl_cvp_transaction.txt.1
  gzip $backDir/ins_whsl_cvp_transaction.txt.1

  # Delete temporary files
  rm $dataDir/CVP_Monthly_Data.txt
  rm $dataDir/CVP_Exception_Data
  rm $dataDir/CVP_Accounting_Data
  rm $dataDir/CVP_monthly.txt.pgp
  rm $/null.log


  # Billing file backup
  mv $dataDir/CVP_Monthly.txt $backDir/CVP_Monthly.txt.${currDate}
  mv $dataDir/CVP_Exception.${currDate}.csv $backDir/CVP_Exception.${currDate}.csv
  mv $dataDir/CVP_Accounting.${currDate}.csv $backDir/CVP_Accounting.${currDate}.csv
  gzip $backDir/CVP_Monthly.txt.${currDate}
  gzip $backDir/CVP_Exception.${currDate}.csv
  gzip $backDir/CVP_Accounting.${currDate}.csv
}

UpdateStatistics()
{
  WriteStartupMessage "UpdateStatistics Started"
  . $utilDir/ConnectToDatabase $database $userid $password
  if [ $? -gt 0 ]
    then ProcessMessages 3
  fi

  db2 -stvf $sq1Dir/Update_Statistics.sq1 > $/$$.log
  cat $/$$.log >> $/$jobLog
  $utilDir/CheckSqlError "SQLSTATE" "00000|02000" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 Update_Statistics.sq1
  fi
  $utilDir/CheckSqlError "^SQL" "SQL3107W" $/$$.log
  if [ $? -ge 1 ]
    then ProcessMessages 4 Update_Transactions.sq]
  fi

  $utilDir/WriteLog "UpdateStatistics Ended" $/$jobLog
}

Finalize()
{
  UpdateStatistics
  rm $dataDir/dummy
  rm $/$$.log
  $utilDir/WriteLog "$jobName Ended" $/$jobLog
  $utilDir/WriteLog "$0 - Ended" $/jobs_log.log
  ProcessMessages 0
  # Log backup
  mv $/cvp_log.2.gz $/cvp_log.3.gz
  mv $/cvp_log.1.gz $/cvp_log.2.gz
  tar -zcf cvp_log.1.gz $/*.log
  tar -zcf cvp_log.1.gz $/*.msg
  mv cvp_log.1.gz $/backup
  rm $/*.log
  rm $/*.msg
}

# ---- Script starts here ----
#if [ $# -eq 0 ]
#  then skipstep=0
#  else skipstep=$1
#fi

Initialize $1


#if [ $skipstep -le 1 ]
#  then WaitForFile
#      LoadWholesaleFile
#  else $utilDir/WriteLog "Skipping Step: LoadWholesaleFile" $/$jobLog
#fi
#
#if [ $skipstep -le 2 ]
#  then LoadDealersInfo
#  else $utilDir/WriteLog "Skipping Step: LoadDealersInfo" $/$jobLog
#fi
#
#if [ $skipstep -le 3 ]
#  then ProcessWholesaleFile
#  else $utilDir/WriteLog "Skipping Step: ProcessWholesaleFile" $/$jobLog
#fi
#
#if [ $skipstep -le 4 ]
#  then BackupTransactions
#  else $utilDir/WriteLog "Skipping Step: BackupTransactions" $/$jobLog
#fi
#
#if [ $skipstep -le 5 ]
#  then UpdateTransactions
#  else $utilDir/WriteLog "Skipping Step: UpdateTransactions" $/$jobLog
#fi
#
#if [ $skipstep -le 6 ]
#  then CreateBillingFile
#  else $utilDir/WriteLog "Skipping Step: CreateBillingFile" $/$jobLog
#fi
#
#if [ $skipstep -le 7 ]
#  then CreateAccountingReport
#  else $utilDir/WriteLog "Skipping Step: CreateAccountingReport" $/$jobLog
#fi
#
#if [ $skipstep -le 8 ]
#  then EncryptBillingFile
#  else $utilDir/WriteLog "Skipping Step: EncryptBillingFile" $/$jobLog
#fi
#
#if [ $skipstep -le 9 ]
#  then SendBillingFile
#  else $utilDir/WriteLog "Skipping Step: SendBillingFile" $/$jobLog
#fi
#
#if [ $skipstep -le 10 ]
#  then SendReports
#  else $utilDir/WriteLog "Skipping Step: SendAccountingReport" $/$jobLog
#fi
#
#if [ $skipstep -le 11 ]
#  then BackupFiles
#  else $utilDir/WriteLog "Skipping Step: BackupFiles" $/$jobLog
#fi
#
#Finalize
#exit 0