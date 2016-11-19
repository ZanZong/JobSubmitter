package ict.zongzan.scheduler;

/**
 * Created by Zongzan on 2016/11/9.
 */
public class Task {
    // task主键
    private String taskId;
    // 所属的jobId
    private String jobId;
    // task请求的资源
    private Resource resourceRequests = null;
    // 执行的jar包,在加载资源的时候要用
    private String jarPath = "";
    // 传给AM解析，加载hdfs资源
    private long taskJarLen = 0;
    private long taskJarTimestamp = 0;
    private String taskJarLocation = "";
    // 执行顺序schedule
    private String nextTask = "";
    // 这个可以由nextTask变量通过DAG得到
    // 有该变量会比较简单，待讨论
    private int execSequence = 0;
    // coontainer申请的优先级
    private int priority = 0;

    public Task(Resource resourceRequest, String jarPath) {
        this.resourceRequests = resourceRequest;
        this.jarPath = jarPath;
    }

    public Task() {
    }

    public int getExecSequence() {
        return execSequence;
    }

    public void setExecSequence(int execSequence) {
        this.execSequence = execSequence;
    }

    public String getTaskJarLocation() {
        return taskJarLocation;
    }

    public void setTaskJarLocation(String taskJarLocation) {
        this.taskJarLocation = taskJarLocation;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public Resource getResourceRequests() {
        return resourceRequests;
    }

    public void setResourceRequests(Resource resourceRequests) {
        this.resourceRequests = resourceRequests;
    }

    public long getTaskJarLen() {
        return taskJarLen;
    }

    public void setTaskJarLen(long taskJarLen) {
        this.taskJarLen = taskJarLen;
    }

    public long getTaskJarTimestamp() {
        return taskJarTimestamp;
    }

    public void setTaskJarTimestamp(long taskJarTimestamp) {
        this.taskJarTimestamp = taskJarTimestamp;
    }

    public String getNextTask() {
        return nextTask;
    }

    public void setNextTask(String nextTask) {
        this.nextTask = nextTask;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public String toString() {
        return "Task{" +
                "taskId='" + taskId + '\'' +
                ", jobId='" + jobId + '\'' +
                ", resourceRequests=" + resourceRequests +
                ", jarPath='" + jarPath + '\'' +
                ", taskJarLen=" + taskJarLen +
                ", taskJarTimestamp=" + taskJarTimestamp +
                ", taskJarLocation='" + taskJarLocation + '\'' +
                ", nextTask='" + nextTask + '\'' +
                ", execSequence=" + execSequence +
                ", priority=" + priority +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Task task = (Task) o;

        if (taskJarLen != task.taskJarLen) return false;
        if (taskJarTimestamp != task.taskJarTimestamp) return false;
        if (execSequence != task.execSequence) return false;
        if (priority != task.priority) return false;
        if (!taskId.equals(task.taskId)) return false;
        if (!jobId.equals(task.jobId)) return false;
        if (resourceRequests != null ? !resourceRequests.equals(task.resourceRequests) : task.resourceRequests != null)
            return false;
        if (jarPath != null ? !jarPath.equals(task.jarPath) : task.jarPath != null) return false;
        if (taskJarLocation != null ? !taskJarLocation.equals(task.taskJarLocation) : task.taskJarLocation != null)
            return false;
        return nextTask != null ? nextTask.equals(task.nextTask) : task.nextTask == null;

    }

    @Override
    public int hashCode() {
        int result = taskId.hashCode();
        result = 31 * result + jobId.hashCode();
        result = 31 * result + (resourceRequests != null ? resourceRequests.hashCode() : 0);
        result = 31 * result + (jarPath != null ? jarPath.hashCode() : 0);
        result = 31 * result + (int) (taskJarLen ^ (taskJarLen >>> 32));
        result = 31 * result + (int) (taskJarTimestamp ^ (taskJarTimestamp >>> 32));
        result = 31 * result + (taskJarLocation != null ? taskJarLocation.hashCode() : 0);
        result = 31 * result + (nextTask != null ? nextTask.hashCode() : 0);
        result = 31 * result + execSequence;
        result = 31 * result + priority;
        return result;
    }
}
