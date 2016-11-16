package ict.zongzan.master;

import ict.zongzan.scheduler.Job;
import ict.zongzan.scheduler.Resource;
import ict.zongzan.scheduler.Task;
import ict.zongzan.yarndeploy.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Start YarnClient by this class
 * Created by Zongzan on 2016/11/9.
 */
public class Master {

    private static final Log LOG = LogFactory.getLog(Master.class);
    //set job for Client
    Job job = null;

    public static void main(String[] args){
        //client result;
        boolean result = false;
        try{
            Client yarnClient = new Client();
            yarnClient.setJob(jobFactory());
            // 初始化client
            try {
                boolean doRun = yarnClient.init(args);
                LOG.info("Initializing Client");
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                yarnClient.printUsage();
                System.exit(-1);
            }
            //启动client
            result = yarnClient.run();
            LOG.info("result:" + result);
            if(result)
                LOG.info("Application complete successfully!");
            else
                LOG.error("Application failed!");
            System.exit(2);
        }catch (Exception e){
            e.printStackTrace();
            LOG.fatal("Error running Client", e);
            LOG.error("Application failed!");
            System.exit(1);
        }

        System.out.println(jobFactory().toString());
        LOG.debug(jobFactory().toString());
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

}
