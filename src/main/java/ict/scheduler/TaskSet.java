package ict.scheduler;

import java.util.HashSet;

/**
 * 定义setId的规则
 * {jobid-execSequence}
 * Created by Zongzan on 2016/11/19.
 */
public class TaskSet {
    private HashSet<Task> taskSet = new HashSet<Task>();

    private String setId = "";

    public TaskSet(HashSet<Task> taskSet, String setId) {
        this.taskSet = taskSet;
        if(taskSet != null){
            this.setId = setId;
        }
        else
            System.out.println("TaskSet contrustor error");
    }

    public HashSet<Task> getSet() {
        return taskSet;
    }

    public String getSetId() {
        return setId;
    }
}
