errorCnt=`grep $1 $3 | grep -v -E $2 | wc -l`
if [ $errorCnt -eq 0 ]
  then errorCnt=`grep "^DB2" $3 | grep -v -E "DB2000" | wc -l`
fi

if [ $errorCnt -eq 0 ]
  then exit 0
else
  exit 1
fi