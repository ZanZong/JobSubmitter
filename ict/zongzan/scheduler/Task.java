package ict.zongzan.scheduler;

/**
 * Created by Zongzan on 2016/11/9.
 */
public class Task {
    // task主键
    private long taskId;
    // 所属的jobId
    private long jobId;
    // task请求的资源
    private Resource resourceRequests = null;
    // 执行的jar包,在加载资源的时候要用
    private String jarPath = "";
    // 传给AM解析，加载资源
    private long taskJarLen = 0;
    private long taskJarTimestamp = 0;
    private String taskJarLocation = "";

    public Task(Resource resourceRequest, String jarPath) {
        this.resourceRequests = resourceRequest;
        this.jarPath = jarPath;
    }

    public String getTaskJarLocation() {
        return taskJarLocation;
    }

    public void setTaskJarLocation(String taskJarLocation) {
        this.taskJarLocation = taskJarLocation;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
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

    @Override
    public String toString() {
        return "Task{" +
                "taskId=" + taskId +
                ", jobId=" + jobId +
                ", resourceRequests=" + resourceRequests +
                ", jarPath='" + jarPath + '\'' +
                ", taskJarLen=" + taskJarLen +
                ", taskJarTimestamp=" + taskJarTimestamp +
                ", taskJarLocation='" + taskJarLocation + '\'' +
                '}';
    }
}
