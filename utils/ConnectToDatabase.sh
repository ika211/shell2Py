if [ $# -eq 1 ]
  then connectString="CONNECT TO $1 USER df5731b USING 'D2D?+HAkU?efrO4&'"
  else connectString="CONNECT TO $1 USER $2 USING $3"
fi
db2 $connectString > $logDir/$$.log
$utilDir/CheckSqlError "SQLSTATE" "00000|02000" $logDir/$$.log
rc=$?
cat $logDir/$$.log >> $logDir/$jobLog
return $rc