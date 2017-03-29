#!/bin/bash
# This script run workloads in one queue in default setting.

# Jobsubmitter is executed by `./bin/hadoop`. Before run this 
# script, you must run `build.sh` to build Jobsubmitter.
# parameters as follow:
# --queue    Which queue you want to submitted. You can set it to 
#            your queue name. 
# --master_memory    Memory you want to allocated to Jobsubmitter.
#                    It also use resource of YARN. Besides, it's not
#                    the resources of workloads you want to run, 
#                    which are setting in `./conf/`.
# --master_vcores    vcores you want to allocated to Jobsubmitter.
# --job_xml          The path of workload setting. This XML files are
#                    put in `./conf/` in default. Please view 
#                    `example-job.xml` in the directory `./conf/` to 
#                    learn more detail.
# --cut_time         The multiple of cpu resources you want to reduce 
#                    which workload consumed. Usually, less cpu resources 
#                    means less run time.
#                    If you want run workloads in normal, just set this to "1".
#                    And if you want to want to run more faster, you can set 
#                    it bigger.
# --task_type        There are four types can be set. "assembly" is runnig
#                    the workloads of CloudMix. In addition, you can run 
#                    c-executables, shellscript, java with the setting follows:
#                    "c-program", "shellscript", "jar". This part's scalability 
#                    is not very well, if you want to use your special workload 
#                    executables, please view the code in 
#                    `ict.yarndeploy.ApplicationMaster.java`


HADOOP_HOME=${HADOOP_HOME}
JOBSUBMITTER_HOME=${JOBSUBMITTER_HOME}
# If you don't want to restart YARN, just remove three lines follows
cp $JOBSUBMITTER_HOME/schedulerxmls/capacity-scheduler.xml $HADOOP_HOME/etc/hadoop/
$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/start-yarn.sh
# run workload
$HADOOP_HOME/bin/hadoop jar $JOBSUBMITTER_HOME/bin/Jobsubmitter-1.0-SNAPSHOT.jar ict.master.Master  \
--jar $JOBSUBMITTER_HOME/bin/Jobsubmitter-1.0-SNAPSHOT.jar \
--queue queueA \
--master_memory 2048 \
--master_vcores 2 \
--job_xml $JOBSUBMITTER_HOME/conf/priorityjob.xml \
--cut_time 100 \
--task_type assembly
#--task_type jar
#--task_type assembly
#--task_type c-program
#--task_type shellscript
#--task_type assembly

# log parsing
cout=0
for k in $(ls -t $HADOOP_HOME/logs/userlogs/)
do  
	if [ $cout == 1 ]; then
		break
	fi  
	let cout+=1
	echo $k
	OLD_IFS=$IFS
	IFS='_'
	arr=($k)
	IFS=$OLD_IFS
	tag=${arr[1]}_${arr[2]}
	cat $HADOOP_HOME/logs/userlogs/${k}/container_${tag}_01_000001/AppMaster.stderr >> $JOBSUBMITTER_HOME/logs/singlequeue.log
done
java -jar $JOBSUBMITTER_HOME/bin/LogAnalysis.jar $JOBSUBMITTER_HOME/logs/singlequeue.log




















