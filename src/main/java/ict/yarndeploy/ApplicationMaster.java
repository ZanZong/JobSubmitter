

package ict.yarndeploy;
/**
 * Created by zongzan on 2016/11/4.
 */

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import ict.calculate.CloudArchOriginal;
import ict.calculate.IbsCProgram;
import ict.scheduler.Job;
import ict.scheduler.Schedule;
import ict.scheduler.Task;
import ict.util.TaskTransUtil;
import ict.scheduler.TaskSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import com.google.common.annotations.VisibleForTesting;


@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    @VisibleForTesting
    @Private
    public static enum DSEvent {
        DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
    }

    @VisibleForTesting
    @Private
    public static enum DSEntity {
        DS_APP_ATTEMPT, DS_CONTAINER
    }

    // Configuration
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync amRMClient;

    // In both secure and non-secure modes, this points to the job-submitter.
    @VisibleForTesting
    UserGroupInformation appSubmitterUgi;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;

    // Application Attempt Id ( combination of attemptId and fail count )
    @VisibleForTesting
    protected ApplicationAttemptId appAttemptID;

    // TODO
    // For status update for clients - yet to be implemented
    // Hostname of the container
    private String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";

    // App Master configuration
    // No. of containers to run shell command on
    @VisibleForTesting
    protected int numTotalContainers = 0;
    // Memory to request for the container on which the shell command will run
    private int containerMemory = 10;
    // VirtualCores to request for the container on which the shell command will run
    private int containerVirtualCores = 1;
    // Priority of the request
    private int requestPriority;

    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    @VisibleForTesting
    protected AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    @VisibleForTesting
    protected AtomicInteger numRequestedContainers = new AtomicInteger();

    // Shell command to be executed
    private String shellCommand = "";
    // Args to be passed to the shell command
    private String shellArgs = "";
    // Env variables to be setup for the shell command
    private Map<String, String> shellEnv = new HashMap<String, String>();
    // cuttime
    private int cutTime = 1;

    // Location of shell script ( obtained from info set in env )
    // Shell script path in fs
    private String scriptPath = "";

    //jobs
    private List<Job> jobs = new ArrayList();
    // tasks
    private Map<String, List<Task>> tasksMap = new HashMap<>();
    // task类型，
    private String taskType = "";
    private Task memConsume = new Task();
    // Timeline domain ID
    private String domainId = null;

    // Hardcoded path to custom log_properties
    private static final String log4jPath = "log4j.properties";

 /* private static final String shellCommandPath = "shellCommands";
  private static final String shellArgsPath = "shellArgs";*/

    private volatile boolean done;

    private ByteBuffer allTokens;
    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();
    private List<Thread> schedulerThreads = new ArrayList<>();

    //scheduler
    Map<String, Schedule> scheduleMap = new HashMap<>();


    // Queue<Task> taskQueue = new LinkedList<Task>();
    Map<Integer, LinkedList<Task>> taskQueuePool = new HashMap<>();
    //Map: alloc containerid --> taskid
    Map<String, String> ctMap = new HashMap<>();
    //Map: completed task number in each set --> num
    Map<String, Integer> cmpltTaskNumOfSet = new HashMap<>();
    //Map: task in each set
    Map<String, Integer> totalTaskNumOfSet = new HashMap<>();
    //Map: wakeup 
    Map<String, TaskSet> wakeUp = new HashMap<>();
    // total submit job number
    int totalSubmittedTaskNum = 0;
    Set badContaier = new HashSet<String>();

    // Timeline Client
    @VisibleForTesting
    TimelineClient timelineClient;

    private final String linux_bash_command = "bash";

    /**
     * @param args Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            ApplicationMaster appMaster = new ApplicationMaster();
            LOG.info("Initialing ApplicationMaster");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }


    public ApplicationMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
    }

    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {
        Options opts = new Options();
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("shell_env", true,
                "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true,
                "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");

        opts.addOption("help", false, "Print usage");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        //Check whether customer log4j.properties file exists
        if (fileExist(log4jPath)) {
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class,
                        log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        Map<String, String> envs = System.getenv();

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs
                    .get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        if (cliParser.hasOption("shell_env")) {
            String shellEnvs[] = cliParser.getOptionValues("shell_env");
            for (String env : shellEnvs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                shellEnv.put(key, val);
            }
        }
        // get memorycore, have some error with Gson,so use more bad method bellow
        String memInfo = envs.get(DSConstants.MEMCONSUME);
        memConsume.setTaskJarLocation(memInfo.split(";")[0]);
        memConsume.setTaskJarLen(Long.parseLong(memInfo.split(";")[1]));
        memConsume.setTaskJarTimestamp(Long.parseLong(memInfo.split(";")[2]));

        String jobids = envs.get(DSConstants.JOBIDSTRING);

        List<String> ids = TaskTransUtil.getIdList(jobids);
        LOG.info("test------jobids:" + ids.toString());
        for (String id : ids) {
            String jobJson = envs.get(id);
            LOG.info("jobid=" + id + " taskJson:" + jobJson);
            jobs.add(TaskTransUtil.getJob(jobJson));
        }
        LOG.info("Get job list from client. job number = " + jobs.size());

        for(Job job : jobs){
            Schedule schedule = new Schedule(job.getTasks());
            schedule.initTotalTaskNumOfSet(totalTaskNumOfSet);
            schedule.initWakeUp(wakeUp);
            scheduleMap.put(job.getJobId(), schedule);
            numTotalContainers += job.getTasks().size();
            tasksMap.put(job.getJobId(), job.getTasks());
        }

        taskType = envs.get(DSConstants.TASKTYPE);
        if(taskType.equals("assembly")) {
            cutTime = Integer.parseInt(envs.get(DSConstants.CUTTIME));
        }
        LOG.info("TaskType = " + taskType + ",numTotalContainers:" + numTotalContainers);
        if (envs.containsKey(DSConstants.JOBSUBMITTERDOMAIN)) {
            domainId = envs.get(DSConstants.JOBSUBMITTERDOMAIN);
        }

        if (numTotalContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run task with no containers. Total containers number is 0");
        }
        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    public void runFast() throws YarnException, IOException, InterruptedException {
        LOG.info("Start Application in fast mode");

    }
    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({"unchecked"})
    public void run() throws YarnException, IOException, InterruptedException {
        LOG.info("Starting ApplicationMaster");

        // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
        // are marked as LimitedPrivate
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.info("Executing with tokens:");
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            LOG.info(token);
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // Create appSubmitterUgi and add original tokens to it
        String appSubmitterUserName =
                System.getenv(Environment.USER.name());
        appSubmitterUgi =
                UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(500, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        startTimelineClient(conf);
        if (timelineClient != null) {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                    DSEvent.DS_APP_ATTEMPT_START, domainId, appSubmitterUgi);
        }

        // Setup local RPC Server to accept status requests directly from clients
        // TODO need to setup a protocol for client to be able to communicate to
        // the RPC server
        // TODO use the rpc port info to register with the RM for the client to
        // send requests to this app master

        // Register self with ResourceManager
        // This will start heartbeating to the RM

        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                        appMasterTrackingUrl);
        // Dump out information about cluster capability as seen by the
        // resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        //numAllocatedContainers.addAndGet(previousAMRunningContainers.size());//总共分配了多少

       /* int numTotalContainersToRequest =
                numTotalContainers - previousAMRunningContainers.size();*/

        LOG.info("\n[WORKSTART]," + System.currentTimeMillis());
        for(Job job : jobs){
            LOG.info("\n\n----test--Launch job scheduler thread:jobid=" + job.getJobId()
                    + ", JobList Size=" + jobs.size()+",job:"+job);

            SchedulerThread schedulerThread = new SchedulerThread(job);
            Thread st = new Thread(schedulerThread);
            st.start();
            schedulerThreads.add(st);
        }

        //numRequestedContainers.set(numTotalContainers);

    }

    @VisibleForTesting
    void startTimelineClient(final Configuration conf)
            throws YarnException, IOException, InterruptedException {
        try {
            appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
                            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
                        // Creating the Timeline Client
                        timelineClient = TimelineClient.createTimelineClient();
                        timelineClient.init(conf);
                        timelineClient.start();
                    } else {
                        timelineClient = null;
                        LOG.warn("Timeline service is not enabled");
                    }
                    return null;
                }
            });
        } catch (UndeclaredThrowableException e) {
            throw new YarnException(e.getCause());
        }
    }

    @VisibleForTesting
    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    @VisibleForTesting
    protected boolean finish() {
        // wait for completion.
        LOG.info("Come to finish(). Wait all thread done.");

        while ((numCompletedContainers.get() != numTotalContainers)) {

            if(!taskQueuePool.isEmpty()) {
                Iterator<Integer> keys =  taskQueuePool.keySet().iterator();
                int maxSet = -1;
                int tmp = 0;
                while(keys.hasNext()){
                    int i = keys.next();
                    if(taskQueuePool.get(i).size() > tmp){
                        tmp = taskQueuePool.get(i).size();
                        maxSet = i;
                    }
                }
                if (maxSet == -1) {
                    LOG.info("All task set is empty");
                }
                else {
                    LOG.info("Max setnum is:" + maxSet);
                    if(numAllocatedContainers.get() != 0 &&
                            numAllocatedContainers.get() == numCompletedContainers.get()){
                        ContainerRequest containerAsk = setupContainerAskForRM(taskQueuePool.get(maxSet).peek());
                        amRMClient.addContainerRequest(containerAsk);
                        LOG.info("Request container again for remain tasks.");
                    }
                    LOG.info("\n---numRequestedContainers---" + numRequestedContainers.get() +
                            "--numRealAllocContainer--" + numAllocatedContainers.get() +
                            "--numCompletedContainers--" + numCompletedContainers.get() +
                            "--task remain in maxset--" + taskQueuePool.get(maxSet).size() +
                            "--totalSubmittedTasks--" + totalSubmittedTaskNum +
                            "--badContainerNum--" + badContaier.size() + "\n");
                }
            }

            try {
            Thread.sleep(500);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        LOG.info("\n[WORKEND]," + System.currentTimeMillis());
        if (timelineClient != null) {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                    DSEvent.DS_APP_ATTEMPT_END, domainId, appSubmitterUgi);
        }


        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in launchthread join: " + e.getMessage());
                e.printStackTrace();
            }
        }
      /*  for(Thread st : schedulerThreads) {
            try {
                st.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in schedulerthread join: " + e.getMessage());
                e.printStackTrace();
            }
        }
*/
        LOG.info("Application completed. Stopping running containers");

        nmClientAsync.stop();

        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (numFailedContainers.get() == 0) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get() + ", allocated="
                    + numAllocatedContainers.get() + ", failed="
                    + numFailedContainers.get();
            LOG.info(appMessage);
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }

        amRMClient.stop();

        // Stop Timeline Client
        if (timelineClient != null) {
            timelineClient.stop();
        }

        return success;
    }

    
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("onContainersCompleted:Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());
                if(badContaier.contains(containerStatus.getContainerId())){
                    LOG.info("A bad container complete.");
                    if (timelineClient != null) {
                        publishContainerEndEvent(
                                timelineClient, containerStatus, domainId, appSubmitterUgi);
                    }
                    return;
                }
                // 增加计数
                String tag = ctMap.get(containerStatus.getContainerId().toString());
                String taskid = tag.split("_")[1];
                String jobid = tag.split("_")[0];
                Task task = TaskTransUtil.getTaskById(taskid, tasksMap.get(jobid));

                // 供日志解析
                LOG.info("\n" + DSConstants.TASKEND + ",TASKTAG=" + jobid + "_" + taskid
                        + ",TIMESTAMP=" + System.currentTimeMillis() + "\n");

                if(task != null){
                    int seq = task.getExecSequence();
                    String setid = scheduleMap.get(jobid).getSetIdByTask(seq);
                    int preNum = 0;
                    if(cmpltTaskNumOfSet.get(setid) == null){
                        cmpltTaskNumOfSet.put(setid, 0);
                    }
                    preNum = cmpltTaskNumOfSet.get(setid);
                    cmpltTaskNumOfSet.put(setid, ++preNum);

                    //唤醒
                    synchronized (wakeUp.get(setid)) {
                        if (preNum == totalTaskNumOfSet.get(setid)){
                            wakeUp.get(setid).notify();
                            LOG.info("Wake up the scheduler thread");
                        }
                    }
                    /*LOG.info("\n\n----test---" +
                            " taskid = " + taskid +
                            ",sequence = " + seq +
                            ",has complete num = " + preNum +
                            ",total num = " + totalTaskNumOfSet.get(setid) + "\n");*/
                }
                else{
                    LOG.info("\n\n----test---" +
                            "error.TaskTransUtil.getTaskById Null Pointer.");
                }
                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

               /* // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    // container failed
                    if (ContainerExitStatus.ABORTED != exitStatus) {

                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        // container被kill，RM会把资源释放掉
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                    }
                } else {*/
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId="
                            + containerStatus.getContainerId());
               // }

                if (timelineClient != null) {
                    publishContainerEndEvent(
                            timelineClient, containerStatus, domainId, appSubmitterUgi);
                }

            }
        }

        @Override
            public void onContainersAllocated(List<Container> allocatedContainers) {
 

                LOG.info("Got response from RM for container ask, allocatedCnt="
                        + allocatedContainers.size());
                for (Container allocatedContainer : allocatedContainers) {
                    LOG.info("Allocated a new container and run a new task in queue"
                            + ", containerId=" + allocatedContainer.getId()
                            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                            + ":" + allocatedContainer.getNodeId().getPort()
                            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                            + ", containerResourceMemory="
                            + allocatedContainer.getResource().getMemory()
                    + ", containerResourceVirtualCores="
                            + allocatedContainer.getResource().getVirtualCores()
                            + ", containerToken="
                            + allocatedContainer.getContainerToken().getIdentifier().toString()
                    + ", contaierPriority=" + allocatedContainer.getPriority().toString());
               
                    Task t = null;
                    synchronized (taskQueuePool) {
                    LinkedList<Task> taskQueue = taskQueuePool.get(Integer.parseInt(allocatedContainer.getPriority().toString()));
                    t = taskQueue.poll();
                }
                if(t == null){
           
                    LOG.info("Task queue is empty, abandon this container.");
                    badContaier.add(allocatedContainer.getId());
                    amRMClient.releaseAssignedContainer(allocatedContainer.getId());
                    return;
                }
                numAllocatedContainers.addAndGet(1);
                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener, t);
                Thread launchThread = new Thread(runnableLaunchContainer);
                launchThreads.add(launchThread);
                launchThread.start();
                // containerid -- taskid
                String val = t.getJobId() + "_" + t.getTaskId();
                ctMap.put(allocatedContainer.getId().toString(), val);

                LOG.info("\n" + DSConstants.TASKSTART + ",TASKTAG=" + val
                        + ",TIMESTAMP=" + System.currentTimeMillis()
                        + ",CONTAINERID=" + allocatedContainer.getId()
                        + ",PRIORITY=" + allocatedContainer.getPriority() + "\n");

            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            amRMClient.stop();
        }
    }



    private class SchedulerThread implements Runnable {
            
            Schedule schedule = null;
            List<Task> tasks = null;
            String jobId = "";
            double starttime = 0;

            public SchedulerThread(Job job) {
                if (job.getTasks().isEmpty()){
                    LOG.error("New scheduler thread error, tasks list is empty.");
                    return;
                }
                this.tasks = job.getTasks();
                schedule = new Schedule(tasks);
                this.jobId = job.getJobId();
                this.starttime = Double.parseDouble(job.getStarttime());
            }

        @Override
        public void run() {
           
            try {
                if(cutTime == 1) {
                  
                }
                else {
                    starttime /= (double) cutTime;
                }
                LOG.info("Job=" + jobId + " is loaded, wait " + starttime + " seconds to be executed.");
                Thread.sleep((long)(starttime * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
           
            int i = 0;
            int taskSetsSize = schedule.taskSetsSize();
            for( ; i < taskSetsSize; i++){
                TaskSet taskSet = schedule.getTaskSet(i);
                Iterator<Task> iterator = taskSet.getSet().iterator();
                while(iterator.hasNext()) {
                    Task task = iterator.next();
                    LinkedList<Task> taskQueue = null;
                    synchronized (taskQueuePool){
                        ContainerRequest containerAsk = setupContainerAskForRM(task);
                        amRMClient.addContainerRequest(containerAsk);
                        taskQueue = taskQueuePool.get(task.getPriority());
                        totalSubmittedTaskNum++;
                        if(taskQueue == null) {
                            taskQueue = new LinkedList();
                            taskQueue.offer(task);
                            taskQueuePool.put(task.getPriority(), taskQueue);
                        }
                        else {
                          
                            taskQueue.offer(task);
                        }
                    }
                    // 供日志解析
                    LOG.info("\n" + DSConstants.TASKWAIT + ",TASKTAG=" + jobId + "_" + task.getTaskId()
                            + ",TIMESTAMP=" + System.currentTimeMillis()
                    );
                }

                LOG.info("Run Scheduler Thread, execute tasks in taskset, tasksetid = " + taskSet.getSetId());
                if (cmpltTaskNumOfSet.get(taskSet.getSetId()) !=
                        totalTaskNumOfSet.get(taskSet.getSetId())){
                    synchronized (wakeUp.get(taskSet.getSetId())) {
                        try {
                            LOG.info("\n\n------test---" +
                                    "Wait to be completed, TaskSet id=" + taskSet.getSetId() + "\n");
                            wakeUp.get(taskSet.getSetId()).wait();
                            if(i == taskSetsSize - 1) {
                                LOG.info("\n\n------test---" +
                                        "All taskset is completed, job "+jobId+" is done.\n");
                            }
                            else {
                                LOG.info("\n\n------test---" +
                                        "Wake Up! Come to next seq.\n");
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            LOG.info("Scheduler thread done. JobId=" + jobId);
        }


    }


    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        Container container;
        //
        NMCallbackHandler containerListener;
        //task info
        Task task = null;


        /**
         * @param lcontainer Allocated container
         * @param containerListener Callback handler of the container
         */
        public LaunchContainerRunnable(
                Container lcontainer, NMCallbackHandler containerListener, Task task) {
            this.container = lcontainer;
            this.containerListener = containerListener;
            this.task = task;
        }

        @Override
        public void run() {
            LOG.info("Setting up container launch container for containerid="
                    + container.getId());

            // Set the local resources
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            URL taskUrl = null;
            URL memUrl = null;
            try {
                taskUrl = ConverterUtils.getYarnUrlFromURI(
                        new URI(task.getTaskJarLocation()));
                memUrl = ConverterUtils.getYarnUrlFromURI(
                        new URI(memConsume.getTaskJarLocation()));
            } catch (URISyntaxException e) {
                LOG.error("Error when trying to use task jar path specified"
                        + " in env, path=" + task.getJarPath(), e);
                numCompletedContainers.incrementAndGet();
                numFailedContainers.incrementAndGet();
                return;
            }
            LocalResource taskJarRsrc = LocalResource.newInstance(taskUrl,
                    LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                    task.getTaskJarLen(), task.getTaskJarTimestamp());
            if (taskType.equals("assembly")){
                LocalResource memJarRsrc = LocalResource.newInstance(memUrl,
                    LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                    memConsume.getTaskJarLen(), memConsume.getTaskJarTimestamp());
                localResources.put(TaskTransUtil.getFileNameByPath(memConsume.getTaskJarLocation()), memJarRsrc);
            }
            localResources.put(TaskTransUtil.getFileNameByPath(task.getTaskJarLocation()), taskJarRsrc);
            
            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);

   
            if (taskType.equals("jar")){
                shellCommand = "java";
                shellArgs = "-jar " + TaskTransUtil.getFileNameByPath(task.getTaskJarLocation())
                        + " " + task.getResourceRequests().getScps()
                        + " " + task.getResourceRequests().getCores();
            }
            else if (taskType.equals("shellscript")){
                shellCommand = "./" + TaskTransUtil.getFileNameByPath(task.getTaskJarLocation());
                shellArgs = String.valueOf(task.getResourceRequests().getScps());
            }
            else if (taskType.equals("c-program")){
                shellCommand = "./" + TaskTransUtil.getFileNameByPath(task.getTaskJarLocation());
                shellArgs = IbsCProgram.getLoops(task.getResourceRequests().getScps(),
                                    task.getResourceRequests().getCores());
            }
            else if (taskType.equals("assembly")) {

                double time = (double)task.getResourceRequests().getScps() / task.getResourceRequests().getCores();
                if(cutTime == 1) {
                    // do nothing, 1 is original
                }
                else {
                    time /= (double)cutTime;
                }
                long loops = CloudArchOriginal.getloops(time, task.getResourceRequests().getCores());
                time *= 1000;//参数的单位是ms
                shellCommand = "./memorycore " + task.getResourceRequests().getRAM() + " " + (long)time + " &";
                shellArgs = "for((a=0;a<" + loops + ";a++));do ./" +
                       TaskTransUtil.getFileNameByPath(task.getTaskJarLocation()) + "; done;";
            }
            //shellCommand = linux_bash_command;
            //vargs.add(shellCommand);
            // Set args for the shell command
            //vargs.add(shellArgs);
            vargs.add(shellCommand);
            vargs.add(shellArgs);
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<String>();
            commands.add(command.toString());
            LOG.info(task.getJobId()+"_"+task.getTaskId()
                    +" "+"Execute command:" + command.toString());
            // Set up ContainerLaunchContext, setting local resource, environment,
            // command and token for constructor.
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, shellEnv, commands, null, allTokens.duplicate(), null);
            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    private void renameScriptFile(final Path renamedScriptPath)
            throws IOException, InterruptedException {
        appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws IOException {
                FileSystem fs = renamedScriptPath.getFileSystem(conf);
                fs.rename(new Path(scriptPath), renamedScriptPath);
                return null;
            }
        });
        LOG.info("User " + appSubmitterUgi.getUserName()
                + " added suffix(.sh/.bat) to script file as " + renamedScriptPath);
    }


    @VisibleForTesting
    static class NMCallbackHandler
            implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<ContainerId, Container>();
        private final ApplicationMaster applicationMaster;

        public NMCallbackHandler(ApplicationMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            LOG.info("Succeeded to stop Container " + containerId);
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            LOG.info("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
        }

        @Override
        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {

            LOG.info("Succeeded to start Container " + containerId);

            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
            if (applicationMaster.timelineClient != null) {
                ApplicationMaster.publishContainerStartEvent(
                        applicationMaster.timelineClient, container,
                        applicationMaster.domainId, applicationMaster.appSubmitterUgi);
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId + " Exception:" + t.getMessage());
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }
    }


    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */

    private ContainerRequest setupContainerAskForRM(Task task) {

        Priority pri = Priority.newInstance(task.getPriority());

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(task.getResourceRequests().getRAM(),
                task.getResourceRequests().getCores());

        ContainerRequest request = new ContainerRequest(capability, null, null,
                pri);
        numRequestedContainers.addAndGet(1);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }

    private boolean fileExist(String filePath) {
        return new File(filePath).exists();
    }

    private String readContent(String filePath) throws IOException {
        DataInputStream ds = null;
        try {
            ds = new DataInputStream(new FileInputStream(filePath));
            return ds.readUTF();
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(ds);
        }
    }

    private static void publishContainerStartEvent(
            final TimelineClient timelineClient, Container container, String domainId,
            UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_START.toString());
        event.addEventInfo("Node", container.getNodeId().toString());
        event.addEventInfo("Resources", container.getResource().toString());
        entity.addEvent(event);

        try {
            ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
                @Override
                public TimelinePutResponse run() throws Exception {
                    return timelineClient.putEntities(entity);
                }
            });
        } catch (Exception e) {
            LOG.error("Container start event could not be published for "
                            + container.getId().toString(),
                    e instanceof UndeclaredThrowableException ? e.getCause() : e);
        }
    }

    private static void publishContainerEndEvent(
            final TimelineClient timelineClient, ContainerStatus container,
            String domainId, UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getContainerId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_END.toString());
        event.addEventInfo("State", container.getState().name());
        event.addEventInfo("Exit Status", container.getExitStatus());
        entity.addEvent(event);
        try {
            timelineClient.putEntities(entity);
        } catch (YarnException | IOException e) {
            LOG.error("Container end event could not be published for "
                    + container.getContainerId().toString(), e);
        }
    }

    private static void publishApplicationAttemptEvent(
            final TimelineClient timelineClient, String appAttemptId,
            DSEvent appEvent, String domainId, UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(appAttemptId);
        entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setEventType(appEvent.toString());
        event.setTimestamp(System.currentTimeMillis());
        entity.addEvent(event);
        try {
            timelineClient.putEntities(entity);
        } catch (YarnException | IOException e) {
            LOG.error("App Attempt "
                    + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
                    + " event could not be published for "
                    + appAttemptId.toString(), e);
        }
    }
}
