package ict.zongzan.util;


import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import ict.zongzan.scheduler.Job;
import ict.zongzan.scheduler.Resource;
import ict.zongzan.scheduler.Task;
import ict.zongzan.yarndeploy.DSConstants;
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
                task.setTaskId(jsonObject.get(DSConstants.TASKID).toString());
                task.setJobId(jsonObject.get(DSConstants.JOBID).toString());
                task.setJarPath(jsonObject.get(DSConstants.JARPATH).toString());
                task.setTaskJarLen(Long.parseLong(jsonObject.get(DSConstants.TASKJARLEN).toString()));
                task.setTaskJarLocation(jsonObject.get(DSConstants.TASKJARLOCATIOIN).toString());
                task.setTaskJarTimestamp(Long.parseLong(jsonObject.get(DSConstants.TASKJARTIMESTAMP).toString()));

                Resource r = new Resource(Integer.parseInt(resourceObject.get(DSConstants.CORES).toString()),
                        Integer.parseInt(resourceObject.get(DSConstants.RAM).toString()),
                        Integer.parseInt(resourceObject.get(DSConstants.LOCALDISKSPACE).toString()),
                        Integer.parseInt(resourceObject.get(DSConstants.SCPS).toString()));
                task.setResourceRequests(r);
            }
        }
        else{
            //task解析错误
            throw new IllegalArgumentException();
        }
        return task;
    }


    public static Job jobFactory(){
        List<Task> tasks = new ArrayList<Task>();
        Task t = null;
        Resource r = null;
        for(int i = 0; i < 3; i++){
            r = new Resource(1, 1500);
            t = new Task(r, "/home/zongzan/taskjar/task" + (i+1) +".jar");
            t.setJobId("000001");
            t.setTaskId("9486000" + i);
            tasks.add(t);
        }
        Job job = new Job(tasks);
        job.setJobId("000001");
        job.setJobName("TestJob");
        return job;
    }


    //test
    public static void main(String[] args){
        Job job = jobFactory();
        Gson gson = new Gson();
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


    }

}
