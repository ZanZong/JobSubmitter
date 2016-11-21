/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ict.zongzan.yarndeploy;
/**
 * Created by Zongzan on 2016/11/4.
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

import ict.zongzan.scheduler.Schedule;
import ict.zongzan.scheduler.Task;
import ict.zongzan.scheduler.TaskSet;
import ict.zongzan.util.TaskTransUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.map.HashedMap;
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
/**
 *下面两个协议是AM和RM和NM通信的，现在统一组合到ApplicationMaster类中
 */
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
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
import org.apache.hadoop.yarn.applications.distributedshell.Log4jPropertyHelper;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
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
    protected int numTotalContainers = 1;
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

    // Location of shell script ( obtained from info set in env )
    // Shell script path in fs
    private String scriptPath = "";
    // Timestamp needed for creating a local resource
    private long shellScriptPathTimestamp = 0;
    // File length needed for local resource
    private long shellScriptPathLen = 0;


    private List<Task> tasks = new ArrayList<Task>();

    // Timeline domain ID
    private String domainId = null;

    // Hardcoded path to shell script in launch container's local env
    private static final String ExecShellStringPath = Client.SCRIPT_PATH;

    // Hardcoded path to custom log_properties
    private static final String log4jPath = "log4j.properties";

 /* private static final String shellCommandPath = "shellCommands";
  private static final String shellArgsPath = "shellArgs";*/

    private volatile boolean done;

    private ByteBuffer allTokens;
    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();

    //scheduler
    Schedule schedule;
    // container等待队列
    Queue<Task> taskQueue = new LinkedList<Task>();
    //Map: alloc containerid --> taskid
    Map<String, String> ctMap = new HashMap<>();
    //Map: 每个set里的task完成数量setid --> num
    Map<String, Integer> cmpltTaskNumOfSet = new HashMap<>();
    //Map: 每个set里task的数量
    Map<String, Integer> totalTaskNumOfSet = new HashMap<>();
    //Map: 用来唤醒等待该set完成的Scheduler线程
    Map<String, TaskSet> wakeUp = new HashMap<>();


    // Timeline Client
    @VisibleForTesting
    TimelineClient timelineClient;

    // 运行的命令，如果是脚本，则为"bash"
    // 命令跟目录在/bin，会补全为/bin/bash
    // 这里直接运行java -jar，故为java
    private final String linux_bash_command = "java";

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

        String taskids = envs.get(DSConstants.TASKIDSTRING);
        //根据id得到Json字符串，解析得到Task对象
        //Gson gson = new Gson();
        //JsonParser parser = new JsonParser();
        List<String> ids = TaskTransUtil.getIdList(taskids);
        // 创建task对象
        LOG.info("zongzan------taskids:" + ids.toString());
        for (String id : ids) {
            String taskJson = envs.get(id);
            LOG.info("taskid=" + id + " taskJson:" + taskJson);
            tasks.add(TaskTransUtil.getTask(taskJson));
        }
        LOG.info("Get task list from client. task number = " + tasks.size());

        // 初始化Scheduler对象,设置set相关信息
        schedule = new Schedule(tasks);
        schedule.initTotalTaskNumOfSet(totalTaskNumOfSet);
        schedule.initWakeUp(wakeUp);


        if (envs.containsKey(DSConstants.JOBSUBMITTERDOMAIN)) {
            domainId = envs.get(DSConstants.JOBSUBMITTERDOMAIN);
        }

        numTotalContainers = tasks.size();
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run task with no containers. Total containers number is 0");
        }
        requestPriority = Integer.parseInt(cliParser
                .getOptionValue("priority", "0"));
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

        //创建AMRMClientAsync对象，负责与RM交互，第一个参数是时间间隔,time/ms,心跳包间隔？
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);//将回调方法告诉了RM
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);//将回调方法告诉了NM
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
        // AM向RM注册自己，并作为一个Client，和RM之间通过心跳包交互。
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

        // A resource ask cannot exceed the max.
        // 先不做检验
        /*if (containerMemory > maxMem) {
          LOG.info("Container memory specified above max threshold of cluster."
              + " Using max value." + ", specified=" + containerMemory + ", max="
              + maxMem);
          containerMemory = maxMem;
        }

        if (containerVirtualCores > maxVCores) {
          LOG.info("Container virtual cores specified above max threshold of cluster."
              + " Using max value." + ", specified=" + containerVirtualCores + ", max="
              + maxVCores);
          containerVirtualCores = maxVCores;
        }*/

        // 向RM请求Container
        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        numAllocatedContainers.addAndGet(previousAMRunningContainers.size());//总共分配了多少

        int numTotalContainersToRequest =
                numTotalContainers - previousAMRunningContainers.size();

        // 循环向RM请求Container，直到所有所需的资源全部被请求到
        // 并循环启动所有的Container并执行程序，执行结果（success/failure）并不关心

        /*for (int i = 0; i < tasks.size(); ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM(tasks.get(i));
            amRMClient.addContainerRequest(containerAsk);
        }*/

        SchedulerThread schedulerThread = new SchedulerThread(tasks);
        schedulerThread.run();

        numRequestedContainers.set(numTotalContainers);
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
        while (!done
                && (numCompletedContainers.get() != numTotalContainers)) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
            }
        }


        if (timelineClient != null) {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                    DSEvent.DS_APP_ATTEMPT_END, domainId, appSubmitterUgi);
        }

        //阻塞调用launchThread.join()方法的线程，直到launchTread完成，此线程再继续
        //由并行执行变成了顺序执行
        //通常用在主线程中，等待其他线程完成再结束主线程
        //这里是等待10s
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // 结束container，向RM发送通知
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

    //container中的处理都是在这里编写的，由RM回调
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

                // 增加计数
                String taskid = ctMap.get(containerStatus.getContainerId().toString());
                Task task = TaskTransUtil.getTaskById(taskid, tasks);
                LOG.info("Completed task id = " + taskid);
                if(task != null){
                    int seq = task.getExecSequence();
                    String setid = schedule.getSetIdByTask(seq);
                    int preNum = 0;
                    if(cmpltTaskNumOfSet.get(setid) == null){
                        cmpltTaskNumOfSet.put(setid, 0);
                    }
                    preNum = cmpltTaskNumOfSet.get(setid);
                    cmpltTaskNumOfSet.put(setid, ++preNum);


                    LOG.info("\n\n----zongzan----\n" +
                            "taskid=" + taskid +
                            "sequence=" + seq +
                            "has complete num=" + preNum +
                            "total num=" + totalTaskNumOfSet.get(setid));
                    //唤醒
                    synchronized (wakeUp.get(setid)) {
                        if (preNum == totalTaskNumOfSet.get(setid)){
                            wakeUp.get(setid).notify();
                            LOG.info("Wake up the scheduler thread");
                        }
                    }

                }
                else{
                    LOG.info("\n\n----zongzan\n" +
                            "error.TaskTransUtil.getTaskById Null Pointer.");
                }

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                // increment counters for completed/failed containers
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
                } else {
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId="
                            + containerStatus.getContainerId());
                }
                if (timelineClient != null) {
                    publishContainerEndEvent(
                            timelineClient, containerStatus, domainId, appSubmitterUgi);
                }

            }

                // Container执行失败，重新申请执行该task
                 /* int askCount = numTotalContainers - numRequestedContainers.get();
                  numRequestedContainers.addAndGet(askCount);

                  if (askCount > 0) {
                    for (int i = 0; i < askCount; ++i) {
                      ContainerRequest containerAsk = setupContainerAskForRM();
                      amRMClient.addContainerRequest(containerAsk);
                    }
                  }

                  if (numCompletedContainers.get() == numTotalContainers) {
                    done = true;
                  }*/
                    }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            //当请求得到RM的回复时，RM回调该方法，启动Container

            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Allocated a new container and run a new task in queue"
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory()
                        + ", containerResourceVirtualCores"
                        + allocatedContainer.getResource().getVirtualCores()
                        + ", containerToken"
                        + allocatedContainer.getContainerToken().getIdentifier().toString());

                // 从队列里取一个task,并从中删除
                Task t = taskQueue.poll();
                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener, t);
                Thread launchThread = new Thread(runnableLaunchContainer);
                launchThreads.add(launchThread);
                launchThread.start();
                // containerid -- taskid
                ctMap.put(allocatedContainer.getId().toString(), t.getTaskId());
                LOG.info("Get a task from TaskQueue, launch Running thread. contaienrid = "
                        + allocatedContainer.getId().toString() + "<-->taskid = "
                        + t.getTaskId());
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



    /**
     *  该线程为scheduler线程，在AM run()时启动，监控当前task执行状态和
     *  RM分配container的状态，完成调度
     *
     *  scheduler应该在此处，每次分配得到container，就执行某些task
     *  应该按照一定的规则分配container，待实现
     *  现在的启动时无序的、平等的
     */
    private class SchedulerThread implements Runnable {
        // 每个job都有一个单独的scheduler线程
        Schedule schedule = null;
        List<Task> tasks = null;

        public SchedulerThread(List<Task> tasks) {
            this.tasks = tasks;
            schedule = new Schedule(tasks);

        }
        @Override
        public void run() {
            // 提交container申请
            int i = 0;
            int taskSetsSize = schedule.taskSetsSize();
            for( ; i < taskSetsSize; i++){
                // 每个taskSet执行完后，再申请下一个seq
                TaskSet taskSet = schedule.getTaskSet(i);
                Iterator<Task> iterator = taskSet.getSet().iterator();
                while(iterator.hasNext()) {
                    Task task = iterator.next();
                    // 添加到container等待队列，下面三句是否要加锁？
                    ContainerRequest containerAsk = setupContainerAskForRM(task);
                    amRMClient.addContainerRequest(containerAsk);
                    taskQueue.offer(task);
                }
                LOG.info("Run Scheduler Thread, execute tasks in taskset, tasksetid = " + taskSet.getSetId()) ;
                // 等待这个set运行完，在complete方法中唤醒
                if (cmpltTaskNumOfSet.get(taskSet.getSetId()) !=
                        totalTaskNumOfSet.get(taskSet.getSetId())){
                    synchronized (wakeUp.get(taskSet.getSetId())) {
                        try {
                            LOG.info("\n\n------zongzan---\n" +
                                    "Wait to be completed, TaskSet id=" + taskSet.getSetId());
                            wakeUp.get(taskSet.getSetId()).wait();

                            LOG.info("\n\n------zongzan---\n" +
                                    "Wake Up! Come to next seq.");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }


    }


    /**
     *
     * RM分配好Container资源后之后，在RM的handler中使用回调方法启动该线程运行Container
     * 该线程是Container的具体的工作
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
     */
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
            //要执行的task
            this.task = task;
        }

        @Override
        public void run() {
            LOG.info("Setting up container launch container for containerid="
                    + container.getId());

            // Set the local resources
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            /*URL taskUrl = null;
            try{
            taskUrl = ConverterUtils.getYarnUrlFromURI(
                    new URI(taskJarPath));
            } catch (URISyntaxException e){
            LOG.error("Error when trying to use task jar path specified"
                    + " in env, path=" + taskJarPath, e);
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
            return;
            }
            LocalResource taskJarRsrc = LocalResource.newInstance(taskUrl,
                  LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                  taskJarLen, taskJarTimestamp);
            localResources.put("YarnApp.jar", taskJarRsrc);*/
            URL taskUrl = null;
            try {
                taskUrl = ConverterUtils.getYarnUrlFromURI(
                        new URI(task.getTaskJarLocation()));
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
            localResources.put(TaskTransUtil.getFileNameByPath(task.getTaskJarLocation()), taskJarRsrc);
            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);
            // 设置运行命令，不用脚本，改为直接运行命令
            //vargs.add(shellCommand);
            // Set shell script path
            /*if (!scriptPath.isEmpty()) {
            vargs.add(ExecShellStringPath);
            }*/
            shellCommand = linux_bash_command;
            shellArgs = "-jar " + TaskTransUtil.getFileNameByPath(task.getTaskJarLocation());
            vargs.add(shellCommand);
            // Set args for the shell command if any
            vargs.add(shellArgs);
            //Container日志路径，包含Container的输出和错误日志，位于/hadoop/userlogs/
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<String>();
            commands.add(command.toString());

            // Set up ContainerLaunchContext, setting local resource, environment,
            // command and token for constructor.
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, shellEnv, commands, null, allTokens.duplicate(), null);
            containerListener.addContainer(container.getId(), container);

            //启动容器，container是执行环境，ctx是需要执行的任务
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
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
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }
    }


    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    //设置需要请求的Container，
    private ContainerRequest setupContainerAskForRM(Task task) {

        Priority pri = Priority.newInstance(task.getPriority());

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(task.getResourceRequests().getRAM(),
                task.getResourceRequests().getCores());

        ContainerRequest request = new ContainerRequest(capability, null, null,
                pri);
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
