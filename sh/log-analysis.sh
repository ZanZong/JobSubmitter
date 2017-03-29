#!/bin/bash
# parse logs and statistic jobs running information
# show the results by paramaters

if [ ! $# == 2 ]; then
	echo "Usage: ResultStatistic.sh <show-number> <log-file>"
	exit
fi
echo "start parsing..."
num=$1
logfile=$2
dir=${JOBSUBMITTER_HOME}
rm -rf ${dir}/${logfile}
for i in $(ls ${dir}/logs)
do
	echo $i >> ${dir}/${logfile}
	java -jar ${dir}/bin/LogAnalysis.jar ${dir}/logs/$i >> ${dir}/${logfile}
	echo ${i}" is done"
done
#java -jar ${dir}/bin/ResultStatistic.jar ${dir}/${logfile} ${num}
