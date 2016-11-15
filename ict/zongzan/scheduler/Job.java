package ict.zongzan.scheduler;

import java.util.List;

/**
 * Created by Zongzan on 2016/11/9.
 */
public class Job {
    // jobid
    private long jobId;
    // jobname
    private String jobName;
    // tasks
    List<Task> tasks = null;

    private String description = "";
    private int tasksNum = 0;

    public Job(List<Task> tasks) {
        this.tasks = tasks;
        tasksNum = tasks.size();
    }

    public Job() {

    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
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

    @Override
    public String toString() {
        return "Job{" +
                "jobId=" + jobId +
                ", jobName='" + jobName + '\'' +
                ", tasks=" + tasks +
                ", description='" + description + '\'' +
                ", tasksNum=" + tasksNum +
                '}';
    }
}
