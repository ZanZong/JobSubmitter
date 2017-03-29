#!/bin/bash
# Now just have 2 queues
# run jobs and gather logs to ./logs
if [ ! $# == 3 ]; then
	echo "Usage: gen_best.sh <capacity/fair> <start-tag> <end-tag>"
	exit
fi
scheduler=$1
start=$2
end=$3
dir=${JOBSUBMITTER_HOME}
hadoopdir=$HADOOP_HOME
totalnum=0
rm -rf ${dir}/logs/*
echo "clean YARN AM logs before"
for j in $(seq ${start} ${end})
do	
	cp ${dir}/schedulerxmls/${scheduler}-scheduler-${j}.xml ${hadoopdir}/etc/hadoop/${scheduler}-scheduler.xml
	/usr/local/hadoop/bin/yarn rmadmin -refreshQueues
	$dir/sh/runyarnappA.sh & $dir/sh/runyarnappB.sh
	cout=0
	for k in $(ls -t ${hadoopdir}/logs/userlogs/)
	do
		if [ $cout == 2 ]; then 
			break
		fi
		let cout+=1
		echo $k
		OLD_IFS=$IFS
		IFS='_'
		arr=($k)
		IFS=$OLD_IFS
		tag=${arr[1]}_${arr[2]}
		cat ${hadoopdir}/logs/userlogs/${k}/container_${tag}_01_000001/AppMaster.stderr >> ${dir}/logs/submit-${j}.log
	done
	let totalnum+=1
done
echo "==========log analysis=========="
echo ${totalnum}" jobs is done"
echo "Use "${scheduler}" scheduler"
echo "Use configuration from "${start}" to "${end}", logs to submit-"${start}".log -> submit-"${end}".log"

#parse logs and statistic
${dir}/sh/log-analysis.sh 1 ./${scheduler}-res
echo "If you want to see details of statistic information, please refer the file ./"${scheduler}"-res"
