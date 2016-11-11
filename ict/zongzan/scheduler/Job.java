package ict.zongzan.scheduler;

import java.util.List;

/**
 * Created by Zongzan on 2016/11/9.
 */
public class Job {
    // jobid
    private long jobId;
    // 时间戳
    private long timestamp;
    // jobname
    private String jobName;
    // tasks
    List<Task> tasks = null;

    private int tasksNum = 0;

    public Job(List<Task> tasks) {
        this.tasks = tasks;
        timestamp = System.currentTimeMillis();
        tasksNum = tasks.size();
    }

    public Job() {

    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
        tasksNum = tasks.size();
    }

    public int getTasksNum() {
        return tasksNum;
    }

    public void setTasksNum(int tasksNum) {
        this.tasksNum = tasksNum;
    }
}
