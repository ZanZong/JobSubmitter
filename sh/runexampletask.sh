#!/bin/bash
# This is a simple example.
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
#                    "c-program", "shellscript", "jar". And here is a example
#                    of run the bash script. This part's scalability 
#                    is not very well, if you want to use your special workload 
#                    executables, please view the code in 
#                    `ict.yarndeploy.ApplicationMaster.java`

HADOOP_HOME=${HADOOP_HOME}
JOBSUBMITTER_HOME=${JOBSUBMITTER_HOME}

$HADOOP_HOME/bin/hadoop jar $JOBSUBMITTER_HOME/bin/Jobsubmitter-1.0-SNAPSHOT.jar ict.master.Master  \
--jar $JOBSUBMITTER_HOME/bin/Jobsubmitter-1.0-SNAPSHOT.jar \
--queue queueA \
--master_memory 2048 \
--master_vcores 2 \
--job_xml $JOBSUBMITTER_HOME/conf/example-job.xml \
--cut_time 1 \
--task_type shellscript
#--task_type assembly
#--task_type c-program
#--task_type shellscript
#--task_type assembly

