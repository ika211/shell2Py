utilDir=$HOME/utils
lineFound=1
line=`grep "^$1" SutilDir/jobsemail.list`
if [ $? -gt 0 ]
  then
    line=`grep "^scripts" SutilDir/jobsemail.list`
  if [ $? -gt 0 ]
  then
    emailAddresses="fei.m.li@mercedes-benz.com"
    lineFound=0
  fi
fi

if [ $lineFound -eq 1 ]
  then
    emailAddresses=`expr "$line" : ".*;\(.*\)"`
fi
script=`expr //$1 : '.*/\(.*\)'`
dateTime=`date '+%m/%d/%Y %H:%M:%S'`
for address in $emailAddresses
do
  if [ $# -gt 3 ]
    then mailx -s "$2 - $dateTime" $address < $3
    else mailx -s "$script - $2 - $dateTime" $address < $3
  fi
done