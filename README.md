#JobSubmitter

An application on YARN.<br>
一个yarn应用。通过读取xml中的task信息，将其提交到yarn container执行。<br>
主要用来模拟spark task的调度、执行。<br>
hadoop版本：`hadoop-2.7.3`
[参考](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html)
<br>
该分支`sparkworkload`将结合 [SWIM](https://github.com/SWIMProjectUCB/SWIM),提交spark作业，使用facebook的日志。
