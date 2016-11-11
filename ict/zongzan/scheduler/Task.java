package ict.zongzan.scheduler;

/**
 * Created by Zongzan on 2016/11/9.
 */
public class Task {
    // task主键
    private long taskId;
    // task生成时间
    private long timestamp = 0;
    // 所属的jobId
    private long jobId;
    // task请求的资源
    private Resource resourceRequest = null;
    // 执行的jar包
    private String jarPath = "";

    public Task(Resource resourceRequest, String jarPath) {
        this.resourceRequest = resourceRequest;
        this.jarPath = jarPath;
        timestamp = System.currentTimeMillis();
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
}
