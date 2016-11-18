package ict.zongzan.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;

import java.util.*;

/**
 * 组织成DAG图，通过拓扑排序得到启动顺序
 * 按此循序，如果该task入度变为0，则启动该task
 * Created by Zongzan on 2016/11/16.
 */
public class Schedule {

    private static final Log LOG = LogFactory.getLog(Schedule.class);
    private List<Task> tasks = null;

    // 待执行的task集合
    public  Set<String> toBeExecutedTasks = new HashSet<String>();
    // task in running
    private Set<String> runningTasks = new HashSet<String>();
    // 执行完的Container
    private Set<String> completedTasks = new HashSet<String>();

    // container池，申请到的未使用的container
    public List<Container> containersPool = new LinkedList<Container>();
    // task使用container的对应关系,<containerId, taskId>
    public Map<String, String> containerUsage = new HashMap<String, String>();

    //当前执行到的execSequence值
    public int currentSeq = 0;
    //记录yarn分配的container总数
    private int containerCounter = 0;

    // Update
    public void updateExecTasks(){
        Set<String> tmp = new HashSet();
        // 开始执行下一个seq
        // 满足待执行的为零个，正在执行的也是零个，
        if(toBeExecutedTasks.isEmpty() && runningTasks.isEmpty()) {
            currentSeq++;
            for(Task task : tasks) {
                if(task.getExecSequence() == currentSeq) {
                    tmp.add(task.getTaskId());
                }
            }
            toBeExecutedTasks.addAll(tmp);
            //唤醒线程
            this.toBeExecutedTasks.notify();
            LOG.info("Current stage is done. execSequence is added to = " + currentSeq);
        }
    }

    public void moveFromReady2Run(String id){
        toBeExecutedTasks.remove(id);
        runningTasks.add(id);
    }

    public void moveFromRun2Complete(String id){
        runningTasks.remove(id);
        completedTasks.add(id);
    }

    private int getIndex(String id, List<String> ids){
        int i = 0;
        int len = ids.size();
        for(i = 0; i < len; i++){
            if(ids.get(i).equals(id)){
                return i;
            }
        }
        return -1;
    }
    /**
     * 得到当前可以执行的task
     * 每次get时更新toBeExecutedTasks
     * @return
     */
    public Set<String> getToBeExecutedTasks() {
        updateExecTasks();
        return toBeExecutedTasks;
    }

    /**
     * 在onCompleted方法中调用，将执行完的container添加至集合
     * @param conids
     */
    public void addCompletedContainers(List<String> conids){
        if(conids != null && !conids.isEmpty()){
            for(String id : conids){
                String taskid = containerUsage.get(id);
                //更改状态
                moveFromRun2Complete(taskid);
            }
        }
        else{
            LOG.error("List is empty or null pointer," +
                    " Paramater List<String> containerids is null.");
        }
    }

    /**
     * ContainerPool的操作
     * @param allocatedContainers
     */
    public void addNewContainerToPool(List<Container> allocatedContainers){
        containerCounter += allocatedContainers.size();
        containersPool.addAll(allocatedContainers);
    }

    public int getContainersPoolSize(){
        return containersPool.size();
    }

    /**
     * 该方法直接取Container，还应该重载根据资源选择container的方法，待实现
     * @return
     */
    public Container getContainerFromPool(){
        if(containersPool.size() > 0){
            Container container = containersPool.get(0);
            // 从pool删除
            removeContainerFromPool(container.getId().toString());
            return container;
        }
        else {
            LOG.error("No container in pool.");
            return null;
        }
    }
    public void removeContainerFromPool(String containerId){
        int len = containersPool.size();
        int i = 0;
        for(i = 0; i < len; i++){
            if(containersPool.get(i).getId().toString().equals(containerId)){
                containersPool.remove(i);   break;
            }
        }
        if(i == len){
            LOG.error("This container is not in the Container pool. Remove failed.");
        }
    }


     /*public DAG transToDAG(List<Task> tasks){

     }*/

    public Schedule(List<Task> tasks) {
        this.tasks = tasks;
    }

    /**
     * task状态静态类
     * 区分task两种状态，运行、结束
     */
    /*public static class TaskStatus{
        public static final String RUNNING = "RUNNING";
        public static final String COMPLETE = "COMPLETE";
    }*/
}
