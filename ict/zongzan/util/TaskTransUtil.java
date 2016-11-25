package ict.zongzan.util;


import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import ict.zongzan.scheduler.Job;
import ict.zongzan.scheduler.Resource;
import ict.zongzan.scheduler.Task;
import ict.zongzan.yarndeploy.DSConstants;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * 在Client将task提交到ApplicationMaster，转成json字符串，在Master取出
 * Created by Zongzan on 2016/11/15.
 */
public class TaskTransUtil {


    public static List<String> getIdList(String ids){
        String[] tmp = ids.split(DSConstants.SPLIT);
        List<String> res = new ArrayList<String>();
        for(String s : tmp){
            res.add(s);
        }
        return res;
    }

    /**
     * 参数是通过Client以json格式传过来的
     * 该函数解析String获得task对象
     * @param taskString
     * @return Task
     */
    public static Task getTask(String taskString) throws IllegalArgumentException{
        Gson gson = new Gson();
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonTask = jsonParser.parse(taskString);

        JsonObject jsonObject = null;
        Task task = new Task();
        if(jsonTask.isJsonObject()){
            //获得Task对象和Resource对象
            jsonObject = jsonTask.getAsJsonObject();
            JsonObject resourceObject = jsonObject.getAsJsonObject(DSConstants.RESOURCEREQUESTS);
            if(!resourceObject.isJsonObject()){
                //resource解析错误
                throw new IllegalArgumentException();
            }
            else{
                task.setTaskId(jsonObject.get(DSConstants.TASKID).getAsString());
                task.setJobId(jsonObject.get(DSConstants.JOBID).getAsString());
                task.setJarPath(jsonObject.get(DSConstants.JARPATH).getAsString());
                task.setTaskJarLen(jsonObject.get(DSConstants.TASKJARLEN).getAsLong());
                task.setTaskJarLocation(jsonObject.get(DSConstants.TASKJARLOCATIOIN).getAsString());
                task.setTaskJarTimestamp(jsonObject.get(DSConstants.TASKJARTIMESTAMP).getAsLong());
                task.setPriority(jsonObject.get(DSConstants.PRIORITY).getAsInt());
                task.setNextTask(jsonObject.get(DSConstants.NEXTTASK).getAsString());
                task.setExecSequence(jsonObject.get(DSConstants.EXECSEQUENCE).getAsInt());

                Resource r = new Resource(resourceObject.get(DSConstants.CORES).getAsInt(),
                        resourceObject.get(DSConstants.RAM).getAsInt(),
                        resourceObject.get(DSConstants.LOCALDISKSPACE).getAsInt(),
                        resourceObject.get(DSConstants.SCPS).getAsDouble());
                task.setResourceRequests(r);
            }
        }
        else{
            //task解析错误
            throw new IllegalArgumentException();
        }
        return task;
    }
    public static Job getJob(String jobString) throws IllegalArgumentException {
        Gson gson = new Gson();
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonJob = jsonParser.parse(jobString);
        Job job = new Job();
        JsonObject jobObject = null;

        if(jsonJob.isJsonObject()){
            // 获得job对象
            jobObject = jsonJob.getAsJsonObject();
            JsonArray jsonArray = jobObject.getAsJsonArray(DSConstants.JOBTASKS);
            System.out.println(jsonArray);
            List<Task> taskList = null;
            Type type = new TypeToken<ArrayList<Task>>(){}.getType();
            taskList = gson.fromJson(jsonArray, type);
            //System.out.println(taskList.size());
            job.setJobId(jobObject.get(DSConstants.JOBID).getAsString());
            job.setJobName(jobObject.get(DSConstants.JOBNAME).getAsString());
            job.setDescription(jobObject.get("description").getAsString());
            job.setTasks(taskList);

        }
        return job;
    }

    public static Job jobFactory(){
        List<Task> tasks = new ArrayList<Task>();
        Task t = null;
        Resource r = null;
        for(int i = 0; i < 4; i++){
            r = new Resource(1, 1500);
            t = new Task(r, "/home/zongzan/taskjar/task" + (i+1) +".jar");
            t.setJobId("000001");
            t.setTaskId("8600" + i);
            t.setNextTask("86004");
            if(i == 3)
                t.setNextTask("null");
            t.setPriority(1);
            tasks.add(t);
        }
        tasks.get(0).setExecSequence(0);
        tasks.get(1).setExecSequence(0);
        tasks.get(2).setExecSequence(0);
        tasks.get(3).setExecSequence(1);

        Job job = new Job(tasks);
        job.setJobId("000001");
        job.setJobName("TestJob");
        return job;
    }

    public static Task getTaskById(String id, List<Task> tasks) {
        for(Task task : tasks){
            if(task.getTaskId().equals(id)){
                return task;
            }
        }
        return null;
    }

    public static Job getJobById(String id, List<Job> jobs) {
        for(Job job : jobs) {
            if(job.getJobId().equals(id)){
                return job;
            }
        }
        return null;
    }
    public static String getFileNameByPath(String URI){
        final String SPLIT = "/";
        String[] res = URI.split(SPLIT);
        if(res.length > 0){
            return res[res.length - 1];
        }
        else
            return "ERROR";
    }
    //test
    public static void main(String[] args){
        // 对解析字符串测试
        Job job = jobFactory();
        Gson gson = new Gson();
        String str = gson.toJson(job);
        //System.out.println(str);

        System.out.println(getJob(str).getTasks().get(0));

       /* Gson gson = new Gson();
        String taskString = gson.toJson(job.getTasks().get(0));
        System.out.println(taskString);
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(taskString);

        JsonObject jsonObject = null;
        if(element.isJsonObject()){
            jsonObject = element.getAsJsonObject();
        }
        System.out.println("id:"+jsonObject.get("taskId")+" taskJarLen:"+jsonObject.get("taskJarLen")
                            +" Location:" + jsonObject.get("taskJarLocation"));
        JsonObject resObject = jsonObject.getAsJsonObject("resourceRequests");

        System.out.println("resource:" + resObject.toString());

        System.out.println("ram:" + resObject.get("RAM").toString());
*/
        //测试getFileNameByPath
       /* String s = getFileNameByPath("hdfs://hw115:9000//user/root/JobSubmitter/application_001/task1.jar");
        System.out.println(s);
*/

        //测试getTask()
        /*String s = "{\"taskId\":\"86000\",\"jobId\":\"000001\",\"resourceRequests\"" +
                ":{\"cores\":1,\"RAM\":1500,\"localDiskSpace\":0,\"scps\":0.0},\"jarPath\":\"/home/zongzan/taskjar/task1.jar\",\"taskJarLen\":53152605,\"taskJarTimestamp\":1479308270442,\"taskJarLocation\":\"hdfs://hw115:9000/user/root/JobSubmitter/application_1479024382672_0027/task1.jar\",\"nextTask\":\"86003\",\"priority\":1}";
        Task t = getTask(s);
        System.out.println(t.toString());*/
        /*System.out.println(t.getJarPath());
        System.out.println(t.getTaskJarTimestamp());
        System.out.println(t.getResourceRequests().toString());
*/


    }

}
