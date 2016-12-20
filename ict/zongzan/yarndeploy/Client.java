package ict.zongzan.yarndeploy;
/**
 * Created by Zongzan on 2016/11/4.
 */
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.gson.Gson;
import ict.zongzan.scheduler.Job;
import ict.zongzan.scheduler.Task;
import ict.zongzan.util.JobLoader;
import ict.zongzan.util.TaskTransUtil;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.LdapGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;


@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

    private static final Log LOG = LogFactory.getLog(Client.class);

    // Configuration
    private Configuration conf;
    private YarnClient yarnClient;
    // Application master specific info to register a new Application with RM/ASM
    private String appName = "";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "";
    // Amt. of memory resource to request for to run the App Master
    private int amMemory = 10;
    // Amt. of virtual core resource to request for to run the App Master
    private int amVCores = 1;

    // Application master jar file
    private String appMasterJar = "";
    // Main class to invoke application master
    private final String appMasterMainClass;

    /*// Shell command to be executed
    private String shellCommand = "";
    // Location of shell script
    private String shellScriptPath = "";
    // Args to be passed to the shell command
    private String[] shellArgs = new String[] {};*/
    // Env variables to be setup for the shell command
    private Map<String, String> shellEnv = new HashMap<String, String>();
    // Shell Command Container priority
    private int shellCmdPriority = 0;

    // Amt of memory to request for container in which shell script will be executed
    private int containerMemory = 10;
    // Amt. of virtual cores to request for container in which shell script will be executed
    private int containerVirtualCores = 1;
    // No. of containers in which the shell script needs to be executed
    private int numContainers = 1;
    private String nodeLabelExpression = null;


    // Start time for client
    private final long clientStartTime = System.currentTimeMillis();
    // Timeout threshold for client. Kill app after time interval expires.
    // Set 5 mins.
    private long clientTimeout = 300000;

    // flag to indicate whether to keep containers across application attempts.
    private boolean keepContainers = false;

    private long attemptFailuresValidityInterval = -1;

    // Debug flag
    boolean debugFlag = false;

    // Timeline domain ID
    private String domainId = null;

    // Flag to indicate whether to create the domain of the given ID
    private boolean toCreateDomain = false;

    // Timeline domain reader access control
    private String viewACLs = null;

    // Timeline domain writer access control
    private String modifyACLs = null;

    // Command line options
    private Options opts;

    // 提交job
    private List<Job> jobs = null;

    private String jobXml = "";
    // task的类型，设计支持jar、shellscript、汇编、python脚本
    private String taskType = "";

    // hadoop会将运行的jar包解压，按照一定的目录重新打包成包名如下的jar包
    private static final String appMasterJarPath = "AppMaster.jar";

    public static final String SCRIPT_PATH = "ExecScript.sh";

    public Client(Configuration conf) throws Exception  {
        this(
                "ict.zongzan.yarndeploy.ApplicationMaster",
                conf);
    }

    Client(String appMasterMainClass, Configuration conf) {
        this.conf = conf;
        this.appMasterMainClass = appMasterMainClass;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - JobSubmitter");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("timeout", true, "Application timeout in milliseconds");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
        opts.addOption("jar", true, "Jar file containing the application master");
        opts.addOption("shell_command", true, "Shell command to be executed by " +
                "the Application Master. Can only specify either --shell_command " +
                "or --shell_script");
        opts.addOption("shell_script", true, "Location of the shell script to be " +
                "executed. Can only specify either --shell_command or --shell_script");
        opts.addOption("shell_args", true, "Command line args for the shell script." +
                "Multiple args can be separated by empty space.");
        opts.getOption("shell_args").setArgs(Option.UNLIMITED_VALUES);
        opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("shell_cmd_priority", true, "Priority for the shell command containers");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("keep_containers_across_application_attempts", false,
                "Flag to indicate whether to keep containers across application attempts." +
                        " If the flag is true, running containers will not be killed when" +
                        " application attempt fails and these containers will be retrieved by" +
                        " the new application attempt ");
        opts.addOption("attempt_failures_validity_interval", true,
                "when attempt_failures_validity_interval in milliseconds is set to > 0," +
                        "the failure number will not take failures which happen out of " +
                        "the validityInterval into failure count. " +
                        "If failure count reaches to maxAppAttempts, " +
                        "the application will be failed.");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("domain", true, "ID of the timeline domain where the "
                + "timeline entities will be put");
        opts.addOption("view_acls", true, "Users and groups that allowed to "
                + "view the timeline entities in the given domain");
        opts.addOption("modify_acls", true, "Users and groups that allowed to "
                + "modify the timeline entities in the given domain");
        opts.addOption("create", false, "Flag to indicate whether to create the "
                + "domain specified with -domain.");
        opts.addOption("help", false, "Print usage");
        opts.addOption("node_label_expression", true,
                "Node label expression to determine the nodes"
                        + " where all the containers of this application"
                        + " will be allocated, \"\" means containers"
                        + " can be allocated anywhere, if you don't specify the option,"
                        + " default node_label_expression of queue will be used.");
        opts.addOption("job_xml", true, "The location of job XML.");
        opts.addOption("task_type", true, "Set what kind of Job you want to run." +
                        " Java jar(jar), shell script(shellscript)," +
                " c program(c-program), or assembly program(assembly).");
    }

    /**
     */
    public Client() throws Exception  {
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     */
    public void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    /**
     * Parse command line options
     * @param args Parsed command line options
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        /*if (cliParser.hasOption("log_properties")) {
            String log4jPath = cliParser.getOptionValue("log_properties");
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(Client.class, log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }*/

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;
        }

        if (cliParser.hasOption("keep_containers_across_application_attempts")) {
            LOG.info("keep_containers_across_application_attempts");
            keepContainers = true;
        }

        appName = cliParser.getOptionValue("appname", "JobSubmitter");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "1024"));
        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));

        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }
        if (amVCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + amVCores);
        }

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }
        //master的jar包本地路径是通过参数传进来的
        appMasterJar = cliParser.getOptionValue("jar");

        /*if (!cliParser.hasOption("shell_command") && !cliParser.hasOption("shell_script")) {
            throw new IllegalArgumentException(
                    "No shell command or shell script specified to be executed by application master");
        } else if (cliParser.hasOption("shell_command") && cliParser.hasOption("shell_script")) {
            throw new IllegalArgumentException("Can not specify shell_command option " +
                    "and shell_script option at the same time");
        } else if (cliParser.hasOption("shell_command")) {
            shellCommand = cliParser.getOptionValue("shell_command");
        } else {
            shellScriptPath = cliParser.getOptionValue("shell_script");
        }
        if (cliParser.hasOption("shell_args")) {
            shellArgs = cliParser.getOptionValues("shell_args");
        }*/

        if (cliParser.hasOption("shell_env")) {
            String envs[] = cliParser.getOptionValues("shell_env");
            for (String env : envs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length()-1)) {
                    val = env.substring(index+1);
                }
                shellEnv.put(key, val);
            }
        }
        shellCmdPriority = Integer.parseInt(cliParser.getOptionValue("shell_cmd_priority", "0"));
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

        taskType = cliParser.getOptionValue("task_type");
        // 从配置文件加载job
        jobXml = cliParser.getOptionValue("job_xml");

        if(jobXml.equals("null")){
            LOG.error("Can't find job XML.");
        }
        LOG.info("Load job from path:" + jobXml);
        jobs = new JobLoader(jobXml).getJobFromXML();

        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", containerVirtualCores=" + containerVirtualCores
        + ", numContainer=" + numContainers);
    }

    nodeLabelExpression = cliParser.getOptionValue("node_label_expression", null);

    clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));

    attemptFailuresValidityInterval =
            Long.parseLong(cliParser.getOptionValue(
            "attempt_failures_validity_interval", "-1"));

    //log4jPropFile = cliParser.getOptionValue("log_properties", "");

    // Get timeline domain options
        if (cliParser.hasOption("domain")) {
            domainId = cliParser.getOptionValue("domain");
            toCreateDomain = cliParser.hasOption("create");
            if (cliParser.hasOption("view_acls")) {
                viewACLs = cliParser.getOptionValue("view_acls");
            }
            if (cliParser.hasOption("modify_acls")) {
                modifyACLs = cliParser.getOptionValue("modify_acls");
            }
        }

        return true;
    }

    /**
     * Main run function for the client
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public boolean run() throws IOException, YarnException {

        LOG.info("Running Client");
        yarnClient.start();

        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM"
                + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
                NodeState.RUNNING);
        LOG.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for"
                    + ", nodeId=" + node.getNodeId()
                    + ", nodeAddress" + node.getHttpAddress()
                    + ", nodeRackName" + node.getRackName()
                    + ", nodeNumContainers" + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        LOG.info("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue"
                        + ", queueName=" + aclInfo.getQueueName()
                        + ", userAcl=" + userAcl.name());
            }
        }

        if (domainId != null && domainId.length() > 0 && toCreateDomain) {
            prepareTimelineDomain();
        }

        // ----------------Get a new application id---------------------
        // 与ResourceManager通信，创建Application
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

        if (amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + amVCores
                    + ", max=" + maxVCores);
            amVCores = maxVCores;
        }
        /*
          Client最重要的任务是设置对象ApplicationSubmissionContext，它定义了ResourceManager启动
          ApplicationMaster所需的全部信息。需要在context中设置以下信息：
          1.队列，优先级。该app提交到哪个队列，优先级是多少。
          2.用户。哪个用户提交的app，用于设定权限管理。
          3.设置ContainerLaunchContext。启动并运行ApplicationMaster的那个container的相关信息
          （本地资源、tokens）、环境变量和运行命令。
         */
        // 请求application id，设置App名字
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);

        if (attemptFailuresValidityInterval >= 0) {
            appContext
                    .setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
        }

        // 加载本地资源
        // 在container的环境中，如果需要运行某些脚本或者jar包
        // 就需要将他们加载的container中。首先将它们复制到
        // 分布式文件系统中，container可以从中读取资源并在启动
        // 时，由LaunchContainerRunnable线程加载。

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        //加载本地资源,存到了hdfs里

        FileSystem fs = FileSystem.get(conf);
        addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(),
                localResources, null);
        //改为不上传脚本，传jar包，使用命令直接运行
       /* String hdfsShellScriptLocation = "";
        long hdfsShellScriptLen = 0;
        long hdfsShellScriptTimestamp = 0;
        if (!shellScriptPath.isEmpty()) {
            Path shellSrc = new Path(shellScriptPath);
            String shellPathSuffix =
                    appName + "/" + appId.toString() + "/" + SCRIPT_PATH;
            Path shellDst =
                    new Path(fs.getHomeDirectory(), shellPathSuffix);
            fs.copyFromLocalFile(false, true, shellSrc, shellDst);
            hdfsShellScriptLocation = shellDst.toUri().toString();
            FileStatus shellFileStatus = fs.getFileStatus(shellDst);
            hdfsShellScriptLen = shellFileStatus.getLen();
            hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
        }*/

        // jar包(或其他可执行文件)添加到container
        // 如果不同task的jar相同，则将这些task的jar信息指向同一个hdfs文件
        Map<String, String> jarPathMap = new HashMap<>();
        for(Job job : jobs){
            for(Task task : job.getTasks()){
                if(jarPathMap.get(task.getJarPath()) == null) {
                    // 该taskjar需要存到hdfs
                    addRescToContainer(fs, task, appId.toString());
                    jarPathMap.put(task.getJarPath(), task.getJobId()+ "_" + task.getTaskId());
                }
                else {
                    // 已经存过了，直接从hdfs取
                    String tmps = jarPathMap.get(task.getJarPath());
                    String jobId = tmps.split("_")[0];
                    String taskId = tmps.split("_")[1];
                    Task proTask = TaskTransUtil.
                            getTaskById(taskId, TaskTransUtil.
                                    getJobById(jobId, jobs).getTasks());
                    if(proTask == null){
                        LOG.error("Set task file location error.");
                    }
                    else{
                        try {
                            task.setTaskJarLen(proTask.getTaskJarLen());
                            task.setTaskJarLocation(proTask.getTaskJarLocation());
                            task.setTaskJarTimestamp(proTask.getTaskJarTimestamp());
                            LOG.info("Already has jar in contaier. Task id = " + task.getTaskId());
                        } catch (NullPointerException e) {
                            LOG.error("Don't have task which taskid=" + taskId
                                    + "in tasks.");
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        // 把消耗内存程序加载到hdfs
        Task memConsume = new Task();
        ict.zongzan.scheduler.Resource memres = new ict.zongzan.scheduler.Resource(1,1);
        memConsume.setTaskId("-1");
        memConsume.setResourceRequests(memres);
        memConsume.setJarPath("/home/zongzan/Jobsubmitter/tasks/assembly/memorycore");
        addRescToContainer(fs, memConsume, appId.toString());
        /*// old
        for(Task task : job.getTasks()){
            addRescToContainer(fs, task, appId.toString());
        }*/

        // Set the necessary security tokens as needed
        //amContainer.setContainerTokens(containerToken);

        // 设置AM运行所需的环境变量env
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();
        //set jobs
       // env.put(DSConstants.JOBNUM, String.valueOf(jobs.size()));
        StringBuilder sb = new StringBuilder();
        for(Job job : jobs){
            sb.append(job.getJobId() + DSConstants.SPLIT);
        }
        env.put(DSConstants.JOBIDSTRING, sb.toString());
        Gson gson = new Gson();
        for(Job job : jobs){
            env.put(job.getJobId(), gson.toJson(job));
            LOG.info("Transtion Job, jobId=" + job.getJobId());
        }
        // memorycore文件的信息传到AM,传过去不带双引号，无法解析
        //env.put(DSConstants.MEMCONSUME, gson.toJson(memConsume));
        String memInfo = memConsume.getTaskJarLocation()+";"+memConsume.getTaskJarLen()+";"+memConsume.getTaskJarTimestamp();
        env.put(DSConstants.MEMCONSUME, memInfo);
        //set task type
        env.put(DSConstants.TASKTYPE, taskType);

        if (domainId != null && domainId.length() > 0) {
            env.put(DSConstants.JOBSUBMITTERDOMAIN, domainId);
        }

       //设置ClassPath，将ApplicationMaster类加入其中
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        /*classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
                "./log4j.properties");*/

        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        env.put("CLASSPATH", classPathEnv.toString());

        // 设置运行参数
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + amMemory + "m");
        // Set class name
        vargs.add(appMasterMainClass);
        // Set params for Application Master
        /*vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--container_vcores " + String.valueOf(containerVirtualCores));
        vargs.add("--num_containers " + String.valueOf(numContainers));*/
            if (null != nodeLabelExpression) {
            appContext.setNodeLabelExpression(nodeLabelExpression);
        }
        vargs.add("--priority " + String.valueOf(shellCmdPriority));

        for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
            vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
        }
        if (debugFlag) {
            vargs.add("--debug");
        }
        //该参数用来重定向错误日志
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());

        //将需要执行的jar包加到ContainerCtx中
       /* addToLocalResources(fs, "/home/zongzan/taskjar/YarnApp.jar", "YarnApp.jar",
                appId.toString(), localResources, null);*/

        // 构造用于运行Application的Container
        // Container信息被封装到Context中
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and
        // vcores requirements

        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);

        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.
            try{
                final Token<?> tokens[] =
                        fs.addDelegationTokens(tokenRenewer, credentials);
                if (tokens != null) {
                    for (Token<?> token : tokens) {
                        LOG.info("Got dt for " + fs.getUri() + "; " + token);
                    }
                }

            } catch (Exception e){
                LOG.warn("Set tokens.For now, only getting tokens for the default file-system.");
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        Priority pri = Priority.newInstance(amPriority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);

        LOG.info("Submitting application to ASM");

        //提交application到RM的applications manager
        //之后会在分配的container中运行AM
        yarnClient.submitApplication(appContext);

        // TODO
        // Try submitting the same request again
        // app submission failure?

        // Monitor the application
        return monitorApplication(appId);

    }

    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    //client对app执行的监控
    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {

            // Check app status every 2 second.
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                }
                else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            }
            else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }

            /*if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
                LOG.info("Reached client specified timeout for application. Killing application");
                forceKillApplication(appId);
                return false;
            }*/
        }

    }

    /**
     * Kill a submitted application by sending a call to the ASM
     * @param appId Application Id to be killed.
     * @throws YarnException
     * @throws IOException
     */
    private void forceKillApplication(ApplicationId appId)
            throws YarnException, IOException {
        // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
        // the same time.
        // If yes, can we kill a particular attempt only?

        // Response can be ignored as it is non-null on success or
        // throws an exception in case of failures
        yarnClient.killApplication(appId);
    }

    private void addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix =
                appName + "/" + appId + "/" + fileDstPath;
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    private void prepareTimelineDomain() {
        TimelineClient timelineClient = null;
        if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
            timelineClient = TimelineClient.createTimelineClient();
            timelineClient.init(conf);
            timelineClient.start();
        } else {
            LOG.warn("Cannot put the domain " + domainId +
                    " because the timeline service is not enabled");
            return;
        }
        try {
            //TODO: we need to check and combine the existing timeline domain ACLs,
            //but let's do it once we have client java library to query domains.
            TimelineDomain domain = new TimelineDomain();
            domain.setId(domainId);
            domain.setReaders(
                    viewACLs != null && viewACLs.length() > 0 ? viewACLs : " ");
            domain.setWriters(
                    modifyACLs != null && modifyACLs.length() > 0 ? modifyACLs : " ");
            timelineClient.putDomain(domain);
            LOG.info("Put the timeline domain: " +
                    TimelineUtils.dumpTimelineRecordtoJSON(domain));
        } catch (Exception e) {
            LOG.error("Error when putting the timeline domain", e);
        } finally {
            timelineClient.stop();
        }
    }

    private void addRescToContainer(FileSystem fs, Task task, String appId){
        String srcp = task.getJarPath();
        String[] tmp = srcp.split("/");
        if(tmp.length > 0){
            Path taskJarSrc = new Path(task.getJarPath());
            String suffix = appName + "/" + appId + "/" + tmp[tmp.length - 1];//解析文件名
            Path taskJarDst = new Path(fs.getHomeDirectory(), suffix);
            try {
                fs.copyFromLocalFile(false, true, taskJarSrc, taskJarDst);
                task.setTaskJarLocation(taskJarDst.toUri().toString());
                FileStatus taskFileStatus = fs.getFileStatus(taskJarDst);
                task.setTaskJarLen(taskFileStatus.getLen());
                task.setTaskJarTimestamp(taskFileStatus.getModificationTime());
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error("Add local task jar to container failed----\n" +
                        "task:\n" + task.toString());
            }
        }
        else{
            LOG.error("Add local task jar to container failed----\n" +
                    "task:\n" + task.toString());
        }
        LOG.info("Add task jar to Container. Task id = " + task.getTaskId());

    }
}
