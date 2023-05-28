/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.v2.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.KeyGenerator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalContainerLauncher;
import org.apache.hadoop.mapred.TaskAttemptListenerImpl;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.AMStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.EventReader;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryCopyService;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventHandler;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobStartEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherImpl;
import org.apache.hadoop.mapreduce.v2.app.local.LocalContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.NoopAMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.Speculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Map-Reduce Application Master.
 * The state machine is encapsulated in the implementation of Job interface.
 * All state changes happens via Job interface. Each event
 * results in a Finite State Transition in Job.
 * <p>
 * MR AppMaster is the composition of loosely coupled services. The services
 * interact with each other via events. The components resembles the
 * Actors model. The component acts on received event and send out the
 * events to other components.
 * This keeps it highly concurrent with no or minimal synchronization needs.
 * <p>
 * The events are dispatched by a central Dispatch mechanism. All components
 * register to the Dispatcher.
 * <p>
 * The information is shared across different components using AppContext.
 */

@SuppressWarnings("rawtypes")
public class MRAppMaster extends CompositeService {

    private static final Logger LOG = LoggerFactory.getLogger(MRAppMaster.class);

    /**
     * Priority of the MRAppMaster shutdown hook.
     */
    public static final int SHUTDOWN_HOOK_PRIORITY = 30;
    public static final String INTERMEDIATE_DATA_ENCRYPTION_ALGO = "HmacSHA1";

    private Clock clock;
    private final long startTime;
    private final long appSubmitTime;
    private String appName;
    private final ApplicationAttemptId appAttemptID;
    private final ContainerId containerID;
    private final String nmHost;
    private final int nmPort;
    private final int nmHttpPort;
    protected final MRAppMetrics metrics;
    private Map<TaskId, TaskInfo> completedTasksFromPreviousRun;
    private List<AMInfo> amInfos;
    private AppContext context;
    private Dispatcher dispatcher;
    private ClientService clientService;
    private ContainerAllocator containerAllocator;
    private ContainerLauncher containerLauncher;
    private EventHandler<CommitterEvent> committerEventHandler;
    private Speculator speculator;
    protected TaskAttemptListener taskAttemptListener;
    protected JobTokenSecretManager jobTokenSecretManager =
            new JobTokenSecretManager();
    private JobId jobId;
    private boolean newApiCommitter;
    private ClassLoader jobClassLoader;
    private OutputCommitter committer;
    private JobEventDispatcher jobEventDispatcher;
    private JobHistoryEventHandler jobHistoryEventHandler;
    private SpeculatorEventDispatcher speculatorEventDispatcher;
    private AMPreemptionPolicy preemptionPolicy;
    private byte[] encryptedSpillKey;

    // After a task attempt completes from TaskUmbilicalProtocol's point of view,
    // it will be transitioned to finishing state.
    // taskAttemptFinishingMonitor is just a timer for attempts in finishing
    // state. If the attempt stays in finishing state for too long,
    // taskAttemptFinishingMonitor will notify the attempt via TA_TIMED_OUT
    // event.
    private TaskAttemptFinishingMonitor taskAttemptFinishingMonitor;

    private Job job;
    private Credentials jobCredentials = new Credentials(); // Filled during init
    protected UserGroupInformation currentUser; // Will be setup during init

    @VisibleForTesting
    protected volatile boolean isLastAMRetry = false;
    //Something happened and we should shut down right after we start up.
    boolean errorHappenedShutDown = false;
    private String shutDownMessage = null;
    JobStateInternal forcedState = null;
    private final ScheduledExecutorService logSyncer;

    private long recoveredJobStartTime = -1L;
    private static boolean mainStarted = false;

    @VisibleForTesting
    protected AtomicBoolean successfullyUnregistered =
            new AtomicBoolean(false);

    public MRAppMaster(ApplicationAttemptId applicationAttemptId,
                       ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
                       long appSubmitTime) {
        // 往下追
        this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
                SystemClock.getInstance(), appSubmitTime);
    }

    public MRAppMaster(ApplicationAttemptId applicationAttemptId,
                       ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
                       Clock clock, long appSubmitTime) {
        super(MRAppMaster.class.getName());
        this.clock = clock;
        this.startTime = clock.getTime();
        this.appSubmitTime = appSubmitTime;
        this.appAttemptID = applicationAttemptId;
        this.containerID = containerId;
        this.nmHost = nmHost;
        this.nmPort = nmPort;
        this.nmHttpPort = nmHttpPort;
        this.metrics = MRAppMetrics.create();
        logSyncer = TaskLog.createLogSyncer();
        // Created MRAppMaster for application appattempt_1684656010852_0006_000001
        LOG.info("Created MRAppMaster for application " + applicationAttemptId);
    }

    protected TaskAttemptFinishingMonitor createTaskAttemptFinishingMonitor(
            EventHandler eventHandler) {
        // 创建 TaskAttemptFinishingMonitor
        TaskAttemptFinishingMonitor monitor =
                new TaskAttemptFinishingMonitor(eventHandler);
        return monitor;
    }

    @Override
    protected void serviceInit(final Configuration conf) throws Exception {
        // create the job classloader if enabled
        // 如果开启了任务类加载器 默认不开启
        createJobClassLoader(conf);

        initJobCredentialsAndUGI(conf);

        // 创建并添加 AsyncDispatcher 服务
        dispatcher = createDispatcher();
        addIfService(dispatcher);

        // 创建并添加任务完成监控服务 TaskAttemptFinishingMonitor
        taskAttemptFinishingMonitor = createTaskAttemptFinishingMonitor(dispatcher.getEventHandler());
        addIfService(taskAttemptFinishingMonitor);

        // 创建 AM 运行上下文对象 RunningAppContext
        context = new RunningAppContext(conf, taskAttemptFinishingMonitor);

        // Job name is the same as the app name util we support DAG of jobs
        // for an app later
        // 获取任务名称
        appName = conf.get(MRJobConfig.JOB_NAME, "<missing app name>");

        // 设置 key = mapreduce.job.application.attempt.id value = ApplicationAttemptId
        conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, appAttemptID.getAttemptId());

        newApiCommitter = false;
        jobId = MRBuilderUtils.newJobId(appAttemptID.getApplicationId(),
                appAttemptID.getApplicationId().getId());
        int numReduceTasks = conf.getInt(MRJobConfig.NUM_REDUCES, 0);
        if ((numReduceTasks > 0 &&
                conf.getBoolean("mapred.reducer.new-api", false)) ||
                (numReduceTasks == 0 &&
                        conf.getBoolean("mapred.mapper.new-api", false))) {
            newApiCommitter = true;
            // Using mapred newApiCommitter.
            LOG.info("Using mapred newApiCommitter.");
        }

        boolean copyHistory = false;
        // 创建 FileOutputCommitter
        committer = createOutputCommitter(conf);
        try {
            String user = UserGroupInformation.getCurrentUser().getShortUserName();
            Path stagingDir = MRApps.getStagingAreaDir(conf, user);
            FileSystem fs = getFileSystem(conf);

            boolean stagingExists = fs.exists(stagingDir);
            Path startCommitFile = MRApps.getStartJobCommitFile(conf, user, jobId);
            boolean commitStarted = fs.exists(startCommitFile);
            Path endCommitSuccessFile = MRApps.getEndJobCommitSuccessFile(conf, user, jobId);
            boolean commitSuccess = fs.exists(endCommitSuccessFile);
            Path endCommitFailureFile = MRApps.getEndJobCommitFailureFile(conf, user, jobId);
            boolean commitFailure = fs.exists(endCommitFailureFile);

            if (!stagingExists) {
                isLastAMRetry = true;
                LOG.info("Attempt num: " + appAttemptID.getAttemptId() +
                        " is last retry: " + isLastAMRetry +
                        " because the staging dir doesn't exist.");
                errorHappenedShutDown = true;
                forcedState = JobStateInternal.ERROR;
                shutDownMessage = "Staging dir does not exist " + stagingDir;
                LOG.error(shutDownMessage);
            } else if (commitStarted) {
                //A commit was started so this is the last time, we just need to know
                // what result we will use to notify, and how we will unregister
                errorHappenedShutDown = true;
                isLastAMRetry = true;
                LOG.info("Attempt num: " + appAttemptID.getAttemptId() +
                        " is last retry: " + isLastAMRetry +
                        " because a commit was started.");
                copyHistory = true;
                if (commitSuccess) {
                    shutDownMessage =
                            "Job commit succeeded in a prior MRAppMaster attempt " +
                                    "before it crashed. Recovering.";
                    forcedState = JobStateInternal.SUCCEEDED;
                } else if (commitFailure) {
                    shutDownMessage =
                            "Job commit failed in a prior MRAppMaster attempt " +
                                    "before it crashed. Not retrying.";
                    forcedState = JobStateInternal.FAILED;
                } else {
                    if (isCommitJobRepeatable()) {
                        // cleanup previous half done commits if committer supports
                        // repeatable job commit.
                        errorHappenedShutDown = false;
                        cleanupInterruptedCommit(conf, fs, startCommitFile);
                    } else {
                        //The commit is still pending, commit error
                        shutDownMessage =
                                "Job commit from a prior MRAppMaster attempt is " +
                                        "potentially in progress. Preventing multiple commit executions";
                        forcedState = JobStateInternal.ERROR;
                    }
                }
            }
        } catch (IOException e) {
            throw new YarnRuntimeException("Error while initializing", e);
        }

        // 默认 false
        if (errorHappenedShutDown) {
            NoopEventHandler eater = new NoopEventHandler();
            //We do not have a JobEventDispatcher in this path
            dispatcher.register(JobEventType.class, eater);

            EventHandler<JobHistoryEvent> historyService = null;
            if (copyHistory) {
                historyService =
                        createJobHistoryHandler(context);
                dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
                        historyService);
            } else {
                dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
                        eater);
            }

            if (copyHistory) {
                // Now that there's a FINISHING state for application on RM to give AMs
                // plenty of time to clean up after unregister it's safe to clean staging
                // directory after unregistering with RM. So, we start the staging-dir
                // cleaner BEFORE the ContainerAllocator so that on shut-down,
                // ContainerAllocator unregisters first and then the staging-dir cleaner
                // deletes staging directory.
                addService(createStagingDirCleaningService());
            }

            // service to allocate containers from RM (if non-uber) or to fake it (uber)
            containerAllocator = createContainerAllocator(null, context);
            addIfService(containerAllocator);
            dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);

            if (copyHistory) {
                // Add the JobHistoryEventHandler last so that it is properly stopped first.
                // This will guarantee that all history-events are flushed before AM goes
                // ahead with shutdown.
                // Note: Even though JobHistoryEventHandler is started last, if any
                // component creates a JobHistoryEvent in the meanwhile, it will be just be
                // queued inside the JobHistoryEventHandler
                addIfService(historyService);

                JobHistoryCopyService cpHist = new JobHistoryCopyService(appAttemptID,
                        dispatcher.getEventHandler());
                addIfService(cpHist);
            }
        } else {

            //service to handle requests from JobClient
            // 创建 MRClientService 服务 该服务是与 JobClient 客户端交互(也即提交任务的客户端)
            // JobClient 持续获取提交任务的状态信息
            // 通讯协议接口为 MRClientProtocol
            clientService = createClientService(context);
            // Init ClientService separately so that we stop it separately, since this
            // service needs to wait some time before it stops so clients can know the
            // final states
            // 初始化 MRClientService
            // (调用 MRClientService的 父类 AbstractService.serviceInit())
            clientService.init(conf);

            // 创建容器申请器 ContainerAllocatorRouter
            containerAllocator = createContainerAllocator(clientService, context);

            //service to handle the output committer
            // 创建并添加 CommitterEventHandler 服务
            committerEventHandler = createCommitterEventHandler(context, committer);
            addIfService(committerEventHandler);

            //policy handling preemption requests from RM
            callWithJobClassLoader(conf, new Action<Void>() {
                public Void call(Configuration conf) {
                    preemptionPolicy = createPreemptionPolicy(conf);
                    preemptionPolicy.init(context);
                    return null;
                }
            });

            //service to handle requests to TaskUmbilicalProtocol
            // 创建并添加任务监听服务 TaskAttemptListenerImpl
            taskAttemptListener = createTaskAttemptListener(context, preemptionPolicy);
            addIfService(taskAttemptListener);

            //service to log job history events
            // 创建并注册任务历史事件处理器 JobHistoryEventHandler
            EventHandler<JobHistoryEvent> historyService =
                    createJobHistoryHandler(context);
            dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
                    historyService);

            // 创建任务事件分发器 JobEventDispatcher
            this.jobEventDispatcher = new JobEventDispatcher();

            //register the event dispatchers
            // 注册相关事件类型
            dispatcher.register(JobEventType.class, jobEventDispatcher);
            dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
            dispatcher.register(TaskAttemptEventType.class,
                    new TaskAttemptEventDispatcher());
            dispatcher.register(CommitterEventType.class, committerEventHandler);

            // 是否开启任务的推测执行
            if (conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
                    || conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
                //optional service to speculate on task attempts' progress
                // 创建并注册任务推测执行 DefaultSpeculator 服务
                speculator = createSpeculator(conf, context);
                addIfService(speculator);
            }

            // 注册推测执行事件类型
            speculatorEventDispatcher = new SpeculatorEventDispatcher(conf);
            dispatcher.register(Speculator.EventType.class,
                    speculatorEventDispatcher);

            // Now that there's a FINISHING state for application on RM to give AMs
            // plenty of time to clean up after unregister it's safe to clean staging
            // directory after unregistering with RM. So, we start the staging-dir
            // cleaner BEFORE the ContainerAllocator so that on shut-down,
            // ContainerAllocator unregisters first and then the staging-dir cleaner
            // deletes staging directory.
            // 创建任务清除目录服务 StagingDirCleaningService
            addService(createStagingDirCleaningService());

            // service to allocate containers from RM (if non-uber) or to fake it (uber)
            // 添加容器申请器 ContainerAllocatorRouter
            addIfService(containerAllocator);
            dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);

            // corresponding service to launch allocated containers via NodeManager
            // 创建并注册启动容器服务 ContainerLauncherRouter
            containerLauncher = createContainerLauncher(context);
            addIfService(containerLauncher);
            dispatcher.register(ContainerLauncher.EventType.class, containerLauncher);

            // Add the JobHistoryEventHandler last so that it is properly stopped first.
            // This will guarantee that all history-events are flushed before AM goes
            // ahead with shutdown.
            // Note: Even though JobHistoryEventHandler is started last, if any
            // component creates a JobHistoryEvent in the meanwhile, it will be just be
            // queued inside the JobHistoryEventHandler
            // 注册任务历史事件处理器 JobHistoryEventHandler
            addIfService(historyService);
        }
        /**
         * MRAppMaster 组合服务的 AsyncDispatcher 异步事件分发器注册了哪些事件注册表
         * 1 EventType               -> JobHistoryEventHandler (任务历史事件处理器)
         * 2 JobEventType            -> JobEventDispatcher (任务事件分发器)
         * 3 TaskEventType           -> TaskEventDispatcher (Task事件分发器)
         * 4 TaskAttemptEventType    -> TaskAttemptEventDispatcher (Task 尝试事件分发器)
         * 5 CommitterEventType      -> CommitterEventHandler (提交事件处理器)
         * 6 Speculator.EventType    -> SpeculatorEventDispatcher (推测事件处理器)
         * 7 ContainerAllocator.EventType -> ContainerAllocatorRouter (容器申请事件处理器)
         * 8 ContainerLauncher.EventType  -> ContainerLauncherRouter (启动容器事件处理器)
         */

        /**
         * MRAppMaster 组合服务有哪些子服务？
         * 1 异步事件分发器        -> AsyncDispatcher
         * 2 任务完成监控服务      -> TaskAttemptFinishingMonitor
         * 3 提交者事件处理        -> CommitterEventHandler
         * 4 Task监听服务         -> TaskAttemptListenerImpl
         * 5 任务推测执行服务       -> DefaultSpeculator
         * 6 任务清除目录服务       -> StagingDirCleaningService
         * 7 容器申请器服务         -> ContainerAllocatorRouter
         * 8 启动容器服务          -> ContainerLauncherRouter
         * 9 任务历史事件处理器服务  -> JobHistoryEventHandler
         */

        // 调用 MRAppMaster 组合服务的所有子服务 serviceInit()
        super.serviceInit(conf);
    } // end of init()

    protected Dispatcher createDispatcher() {
        return new AsyncDispatcher();
    }

    private boolean isCommitJobRepeatable() throws IOException {
        boolean isRepeatable = false;
        Configuration conf = getConfig();
        if (committer != null) {
            final JobContext jobContext = getJobContextFromConf(conf);

            isRepeatable = callWithJobClassLoader(conf,
                    new ExceptionAction<Boolean>() {
                        public Boolean call(Configuration conf) throws IOException {
                            return committer.isCommitJobRepeatable(jobContext);
                        }
                    });
        }
        return isRepeatable;
    }

    private JobContext getJobContextFromConf(Configuration conf) {
        if (newApiCommitter) {
            return new JobContextImpl(conf, TypeConverter.fromYarn(getJobId()));
        } else {
            return new org.apache.hadoop.mapred.JobContextImpl(
                    new JobConf(conf), TypeConverter.fromYarn(getJobId()));
        }
    }

    private void cleanupInterruptedCommit(Configuration conf,
                                          FileSystem fs, Path startCommitFile) throws IOException {
        LOG.info("Delete startJobCommitFile in case commit is not finished as " +
                "successful or failed.");
        fs.delete(startCommitFile, false);
    }

    private OutputCommitter createOutputCommitter(Configuration conf) {
        return callWithJobClassLoader(conf, new Action<OutputCommitter>() {
            public OutputCommitter call(Configuration conf) {
                OutputCommitter committer = null;

                // OutputCommitter set in config null
                LOG.info("OutputCommitter set in config "
                        + conf.get("mapred.output.committer.class"));

                if (newApiCommitter) {
                    org.apache.hadoop.mapreduce.v2.api.records.TaskId taskID =
                            MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
                    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
                            MRBuilderUtils.newTaskAttemptId(taskID, 0);
                    TaskAttemptContext taskContext = new TaskAttemptContextImpl(conf,
                            TypeConverter.fromYarn(attemptID));
                    OutputFormat outputFormat;
                    try {
                        outputFormat = ReflectionUtils.newInstance(taskContext
                                .getOutputFormatClass(), conf);
                        committer = outputFormat.getOutputCommitter(taskContext);
                    } catch (Exception e) {
                        throw new YarnRuntimeException(e);
                    }
                } else {
                    committer = ReflectionUtils.newInstance(conf.getClass(
                            "mapred.output.committer.class", FileOutputCommitter.class,
                            org.apache.hadoop.mapred.OutputCommitter.class), conf);
                }
                // OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
                LOG.info("OutputCommitter is " + committer.getClass().getName());
                return committer;
            }
        });
    }

    protected AMPreemptionPolicy createPreemptionPolicy(Configuration conf) {
        return ReflectionUtils.newInstance(conf.getClass(
                MRJobConfig.MR_AM_PREEMPTION_POLICY,
                NoopAMPreemptionPolicy.class, AMPreemptionPolicy.class), conf);
    }

    private boolean isJobNamePatternMatch(JobConf conf, String jobTempDir) {
        // Matched staging files should be preserved after job is finished.
        if (conf.getKeepTaskFilesPattern() != null && jobTempDir != null) {
            java.nio.file.Path pathName = Paths.get(jobTempDir).getFileName();
            if (pathName != null) {
                String jobFileName = pathName.toString();
                Pattern pattern = Pattern.compile(conf.getKeepTaskFilesPattern());
                Matcher matcher = pattern.matcher(jobFileName);
                return matcher.find();
            }
        }
        return false;
    }

    private boolean isKeepFailedTaskFiles(JobConf conf) {
        // TODO: Decide which failed task files that should
        // be kept are in application log directory.
        return conf.getKeepFailedTaskFiles();
    }

    protected boolean keepJobFiles(JobConf conf, String jobTempDir) {
        return isJobNamePatternMatch(conf, jobTempDir)
                || isKeepFailedTaskFiles(conf);
    }

    /**
     * Create the default file System for this job.
     *
     * @param conf the conf object
     * @return the default filesystem for this job
     * @throws IOException
     */
    protected FileSystem getFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    protected Credentials getCredentials() {
        return jobCredentials;
    }

    /**
     * clean up staging directories for the job.
     *
     * @throws IOException
     */
    public void cleanupStagingDir() throws IOException {
        /* make sure we clean the staging files */
        String jobTempDir = getConfig().get(MRJobConfig.MAPREDUCE_JOB_DIR);
        FileSystem fs = getFileSystem(getConfig());
        try {
            if (!keepJobFiles(new JobConf(getConfig()), jobTempDir)) {
                if (jobTempDir == null) {
                    LOG.warn("Job Staging directory is null");
                    return;
                }
                Path jobTempDirPath = new Path(jobTempDir);
                LOG.info("Deleting staging directory " + FileSystem.getDefaultUri(getConfig()) +
                        " " + jobTempDir);
                fs.delete(jobTempDirPath, true);
            }
        } catch (IOException io) {
            LOG.error("Failed to cleanup staging dir " + jobTempDir, io);
        }
    }

    /**
     * Exit call. Just in a function call to enable testing.
     */
    protected void sysexit() {
        System.exit(0);
    }

    @VisibleForTesting
    public void shutDownJob() {
        // job has finished
        // this is the only job, so shut down the Appmaster
        // note in a workflow scenario, this may lead to creation of a new
        // job (FIXME?)

        JobEndNotifier notifier = null;
        if (getConfig().get(MRJobConfig.MR_JOB_END_NOTIFICATION_URL) != null) {
            notifier = new JobEndNotifier();
            notifier.setConf(getConfig());
        }

        try {
            //if isLastAMRetry comes as true, should never set it to false
            if (!isLastAMRetry) {
                if (((JobImpl) job).getInternalState() != JobStateInternal.REBOOT) {
                    LOG.info("Job finished cleanly, recording last MRAppMaster retry");
                    isLastAMRetry = true;
                }
            }
            notifyIsLastAMRetry(isLastAMRetry);
            // Stop all services
            // This will also send the final report to the ResourceManager
            LOG.info("Calling stop for all the services");
            MRAppMaster.this.stop();

            if (isLastAMRetry && notifier != null) {
                // Send job-end notification when it is safe to report termination to
                // users and it is the last AM retry
                sendJobEndNotify(notifier);
                notifier = null;
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            clientService.stop();
        } catch (Throwable t) {
            LOG.warn("Graceful stop failed. Exiting.. ", t);
            exitMRAppMaster(1, t);
        } finally {
            if (isLastAMRetry && notifier != null) {
                sendJobEndNotify(notifier);
            }
        }
        exitMRAppMaster(0, null);
    }

    private void sendJobEndNotify(JobEndNotifier notifier) {
        try {
            LOG.info("Job end notification started for jobID : "
                    + job.getReport().getJobId());
            // If unregistration fails, the final state is unavailable. However,
            // at the last AM Retry, the client will finally be notified FAILED
            // from RM, so we should let users know FAILED via notifier as well
            JobReport report = job.getReport();
            if (!context.hasSuccessfullyUnregistered()) {
                report.setJobState(JobState.FAILED);
            }
            notifier.notify(report);
        } catch (InterruptedException ie) {
            LOG.warn("Job end notification interrupted for jobID : "
                    + job.getReport().getJobId(), ie);
        }
    }

    /**
     * MRAppMaster exit method which has been instrumented for both runtime and
     * unit testing.
     * If the main thread has not been started, this method was called from a
     * test. In that case, configure the ExitUtil object to not exit the JVM.
     *
     * @param status integer indicating exit status
     * @param t      throwable exception that could be null
     */
    private void exitMRAppMaster(int status, Throwable t) {
        if (!mainStarted) {
            ExitUtil.disableSystemExit();
        }
        try {
            if (t != null) {
                ExitUtil.terminate(status, t);
            } else {
                ExitUtil.terminate(status);
            }
        } catch (ExitUtil.ExitException ee) {
            // ExitUtil.ExitException is only thrown from the ExitUtil test code when
            // SystemExit has been disabled. It is always thrown in in the test code,
            // even when no error occurs. Ignore the exception so that tests don't
            // need to handle it.
        }
    }

    private class JobFinishEventHandler implements EventHandler<JobFinishEvent> {
        @Override
        public void handle(JobFinishEvent event) {
            // Create a new thread to shutdown the AM. We should not do it in-line
            // to avoid blocking the dispatcher itself.
            new Thread() {

                @Override
                public void run() {
                    shutDownJob();
                }
            }.start();
        }
    }

    /**
     * create an event handler that handles the job finish event.
     *
     * @return the job finish event handler.
     */
    protected EventHandler<JobFinishEvent> createJobFinishEventHandler() {
        // 创建 JobFinishEventHandler
        return new JobFinishEventHandler();
    }

    /**
     * Create and initialize (but don't start) a single job.
     *
     * @param forcedState a state to force the job into or null for normal operation.
     * @param diagnostic  a diagnostic message to include with the job.
     */
    protected Job createJob(Configuration conf, JobStateInternal forcedState,
                            String diagnostic) {

        // create single job
        // 创建 JobImpl
        Job newJob =
                new JobImpl(jobId, appAttemptID, conf, dispatcher.getEventHandler(),
                        taskAttemptListener, jobTokenSecretManager, jobCredentials, clock,
                        completedTasksFromPreviousRun, metrics,
                        committer, newApiCommitter,
                        currentUser.getUserName(), appSubmitTime, amInfos, context,
                        forcedState, diagnostic);
        // 缓存
        ((RunningAppContext) context).jobs.put(newJob.getID(), newJob);

        // 注册
        dispatcher.register(JobFinishEvent.Type.class,
                createJobFinishEventHandler());
        return newJob;
    } // end createJob()


    /**
     * Obtain the tokens needed by the job and put them in the UGI
     *
     * @param conf
     */
    protected void initJobCredentialsAndUGI(Configuration conf) {

        try {
            this.currentUser = UserGroupInformation.getCurrentUser();
            this.jobCredentials = ((JobConf) conf).getCredentials();
            if (CryptoUtils.isEncryptedSpillEnabled(conf)) {
                int keyLen = conf.getInt(
                        MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS,
                        MRJobConfig
                                .DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS);
                KeyGenerator keyGen =
                        KeyGenerator.getInstance(INTERMEDIATE_DATA_ENCRYPTION_ALGO);
                keyGen.init(keyLen);
                encryptedSpillKey = keyGen.generateKey().getEncoded();
            } else {
                encryptedSpillKey = new byte[]{0};
            }
        } catch (IOException e) {
            throw new YarnRuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new YarnRuntimeException(e);
        }
    }

    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
            AppContext context) {
        this.jobHistoryEventHandler = new JobHistoryEventHandler(context,
                getStartCount());
        return this.jobHistoryEventHandler;
    }

    protected AbstractService createStagingDirCleaningService() {
        // 创建 StagingDirCleaningService
        return new StagingDirCleaningService();
    }

    protected Speculator createSpeculator(Configuration conf,
                                          final AppContext context) {
        return callWithJobClassLoader(conf, new Action<Speculator>() {
            public Speculator call(Configuration conf) {
                Class<? extends Speculator> speculatorClass;
                try {
                    speculatorClass
                            // "yarn.mapreduce.job.speculator.class"
                            = conf.getClass(MRJobConfig.MR_AM_JOB_SPECULATOR,
                            DefaultSpeculator.class,
                            Speculator.class);
                    Constructor<? extends Speculator> speculatorConstructor
                            = speculatorClass.getConstructor
                            (Configuration.class, AppContext.class);
                    Speculator result = speculatorConstructor.newInstance(conf, context);

                    return result;
                } catch (InstantiationException ex) {
                    LOG.error("Can't make a speculator -- check "
                            + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
                    throw new YarnRuntimeException(ex);
                } catch (IllegalAccessException ex) {
                    LOG.error("Can't make a speculator -- check "
                            + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
                    throw new YarnRuntimeException(ex);
                } catch (InvocationTargetException ex) {
                    LOG.error("Can't make a speculator -- check "
                            + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
                    throw new YarnRuntimeException(ex);
                } catch (NoSuchMethodException ex) {
                    LOG.error("Can't make a speculator -- check "
                            + MRJobConfig.MR_AM_JOB_SPECULATOR, ex);
                    throw new YarnRuntimeException(ex);
                }
            }
        });
    }

    protected TaskAttemptListener createTaskAttemptListener(AppContext context,
                                                            AMPreemptionPolicy preemptionPolicy) {
        // 创建 TaskAttemptListenerImpl
        TaskAttemptListener lis =
                new TaskAttemptListenerImpl(context, jobTokenSecretManager,
                        getRMHeartbeatHandler(), preemptionPolicy, encryptedSpillKey);
        return lis;
    }

    protected EventHandler<CommitterEvent> createCommitterEventHandler(
            AppContext context, OutputCommitter committer) {
        // 创建 CommitterEventHandler
        return new CommitterEventHandler(
                context, committer,
                // 获取 AM 与 RM 心跳处理器 也即 ContainerAllocatorRouter
                getRMHeartbeatHandler(),
                jobClassLoader);
    }

    protected ContainerAllocator createContainerAllocator(
            final ClientService clientService, final AppContext context) {
        // 创建 ContainerAllocatorRouter
        return new ContainerAllocatorRouter(clientService, context);
    }

    protected RMHeartbeatHandler getRMHeartbeatHandler() {
        return (RMHeartbeatHandler) containerAllocator;
    }

    protected ContainerLauncher
    createContainerLauncher(final AppContext context) {
        // 创建 ContainerLauncherRouter
        return new ContainerLauncherRouter(context);
    }

    //TODO:should have an interface for MRClientService
    protected ClientService createClientService(AppContext context) {
        // 创建 MRClientService
        return new MRClientService(context);
    }

    public ApplicationId getAppID() {
        return appAttemptID.getApplicationId();
    }

    public ApplicationAttemptId getAttemptID() {
        return appAttemptID;
    }

    public JobId getJobId() {
        return jobId;
    }

    public OutputCommitter getCommitter() {
        return committer;
    }

    public boolean isNewApiCommitter() {
        return newApiCommitter;
    }

    public int getStartCount() {
        return appAttemptID.getAttemptId();
    }

    public AppContext getContext() {
        return context;
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public Map<TaskId, TaskInfo> getCompletedTaskFromPreviousRun() {
        return completedTasksFromPreviousRun;
    }

    public List<AMInfo> getAllAMInfos() {
        return amInfos;
    }

    public ContainerAllocator getContainerAllocator() {
        return containerAllocator;
    }

    public ContainerLauncher getContainerLauncher() {
        return containerLauncher;
    }

    public TaskAttemptListener getTaskAttemptListener() {
        return taskAttemptListener;
    }

    public Boolean isLastAMRetry() {
        return isLastAMRetry;
    }

    /**
     * By the time life-cycle of this router starts, job-init would have already
     * happened.
     */
    private final class ContainerAllocatorRouter extends AbstractService
            implements ContainerAllocator, RMHeartbeatHandler {
        private final ClientService clientService;
        private final AppContext context;
        private ContainerAllocator containerAllocator;

        ContainerAllocatorRouter(ClientService clientService,
                                 AppContext context) {
            super(ContainerAllocatorRouter.class.getName());
            this.clientService = clientService;
            this.context = context;
        }

        @Override
        protected void serviceStart() throws Exception {
            if (job.isUber()) {
                MRApps.setupDistributedCacheLocal(getConfig());
                this.containerAllocator = new LocalContainerAllocator(
                        this.clientService, this.context, nmHost, nmPort, nmHttpPort
                        , containerID);
            } else {
                // 创建从 RM 申请容器服务 RMContainerAllocator
                this.containerAllocator = new RMContainerAllocator(
                        this.clientService, this.context, preemptionPolicy);
            }
            // 初始化调用 RMContainerAllocator.serviceInit()
            ((Service) this.containerAllocator).init(getConfig());
            // 启动调用 RMContainerAllocator.serviceStart()
            ((Service) this.containerAllocator).start();
            super.serviceStart();
        }

        @Override
        protected void serviceStop() throws Exception {
            ServiceOperations.stop((Service) this.containerAllocator);
            super.serviceStop();
        }

        @Override
        public void handle(ContainerAllocatorEvent event) {
            // eventType = ContainerAllocator.EventType.CONTAINER_REQ
            // 调用 RMContainerAllocator.handle(ContainerAllocator.EventType.CONTAINER_REQ)
            this.containerAllocator.handle(event);
        }

        public void setSignalled(boolean isSignalled) {
            ((RMCommunicator) containerAllocator).setSignalled(isSignalled);
        }

        public void setShouldUnregister(boolean shouldUnregister) {
            ((RMCommunicator) containerAllocator).setShouldUnregister(shouldUnregister);
        }

        @Override
        public long getLastHeartbeatTime() {
            return ((RMCommunicator) containerAllocator).getLastHeartbeatTime();
        }

        @Override
        public void runOnNextHeartbeat(Runnable callback) {
            ((RMCommunicator) containerAllocator).runOnNextHeartbeat(callback);
        }
    }

    /**
     * By the time life-cycle of this router starts, job-init would have already
     * happened.
     */
    private final class ContainerLauncherRouter extends AbstractService
            implements ContainerLauncher {
        private final AppContext context;
        private ContainerLauncher containerLauncher;

        ContainerLauncherRouter(AppContext context) {
            super(ContainerLauncherRouter.class.getName());
            this.context = context;
        }

        @Override
        protected void serviceStart() throws Exception {
            if (job.isUber()) {
                this.containerLauncher = new LocalContainerLauncher(context,
                        (TaskUmbilicalProtocol) taskAttemptListener, jobClassLoader);
                ((LocalContainerLauncher) this.containerLauncher)
                        .setEncryptedSpillKey(encryptedSpillKey);
            } else {
                // 创建启动容器服务 ContainerLauncherImpl
                this.containerLauncher = new ContainerLauncherImpl(context);
            }
            // 初始化调用 ContainerLauncherImpl.serviceInit()
            ((Service) this.containerLauncher).init(getConfig());
            // 启动调用 ContainerLauncherImpl.serviceStart()
            ((Service) this.containerLauncher).start();
            // 调用父类
            super.serviceStart();
        }

        @Override
        public void handle(ContainerLauncherEvent event) {
            // eventType = ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH
            // 调用 ContainerLauncherImpl.handle(ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH)
            this.containerLauncher.handle(event);
        }

        @Override
        protected void serviceStop() throws Exception {
            ServiceOperations.stop((Service) this.containerLauncher);
            super.serviceStop();
        }
    }

    private final class StagingDirCleaningService extends AbstractService {
        StagingDirCleaningService() {
            super(StagingDirCleaningService.class.getName());
        }

        @Override
        protected void serviceStop() throws Exception {
            try {
                if (isLastAMRetry) {
                    cleanupStagingDir();
                } else {
                    LOG.info("Skipping cleaning up the staging dir. "
                            + "assuming AM will be retried.");
                }
            } catch (IOException io) {
                LOG.error("Failed to cleanup staging dir: ", io);
            }
            super.serviceStop();
        }
    }

    public class RunningAppContext implements AppContext {

        private final Map<JobId, Job> jobs = new ConcurrentHashMap<JobId, Job>();
        private final Configuration conf;
        private final ClusterInfo clusterInfo = new ClusterInfo();
        private final ClientToAMTokenSecretManager clientToAMTokenSecretManager;
        private TimelineClient timelineClient = null;
        private TimelineV2Client timelineV2Client = null;
        private String historyUrl = null;

        private final TaskAttemptFinishingMonitor taskAttemptFinishingMonitor;

        public RunningAppContext(Configuration config,
                                 TaskAttemptFinishingMonitor taskAttemptFinishingMonitor) {
            this.conf = config;
            this.clientToAMTokenSecretManager =
                    new ClientToAMTokenSecretManager(appAttemptID, null);
            this.taskAttemptFinishingMonitor = taskAttemptFinishingMonitor;
            if (conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA,
                    MRJobConfig.DEFAULT_MAPREDUCE_JOB_EMIT_TIMELINE_DATA)
                    && YarnConfiguration.timelineServiceEnabled(conf)) {

                if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
                    // create new version TimelineClient
                    timelineV2Client = TimelineV2Client.createTimelineClient(
                            appAttemptID.getApplicationId());
                } else {
                    timelineClient = TimelineClient.createTimelineClient();
                }
            }
        }

        @Override
        public ApplicationAttemptId getApplicationAttemptId() {
            return appAttemptID;
        }

        @Override
        public ApplicationId getApplicationID() {
            return appAttemptID.getApplicationId();
        }

        @Override
        public String getApplicationName() {
            return appName;
        }

        @Override
        public long getStartTime() {
            return startTime;
        }

        @Override
        public Job getJob(JobId jobID) {
            return jobs.get(jobID);
        }

        @Override
        public Map<JobId, Job> getAllJobs() {
            return jobs;
        }

        @Override
        public EventHandler<Event> getEventHandler() {
            return dispatcher.getEventHandler();
        }

        @Override
        public CharSequence getUser() {
            return this.conf.get(MRJobConfig.USER_NAME);
        }

        @Override
        public Clock getClock() {
            return clock;
        }

        @Override
        public ClusterInfo getClusterInfo() {
            return this.clusterInfo;
        }

        @Override
        public Set<String> getBlacklistedNodes() {
            return ((RMContainerRequestor) containerAllocator).getBlacklistedNodes();
        }

        @Override
        public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
            return clientToAMTokenSecretManager;
        }

        @Override
        public boolean isLastAMRetry() {
            return isLastAMRetry;
        }

        @Override
        public boolean hasSuccessfullyUnregistered() {
            return successfullyUnregistered.get();
        }

        public void markSuccessfulUnregistration() {
            successfullyUnregistered.set(true);
        }

        public void resetIsLastAMRetry() {
            isLastAMRetry = false;
        }

        @Override
        public String getNMHostname() {
            return nmHost;
        }

        @Override
        public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
            return taskAttemptFinishingMonitor;
        }

        public TimelineClient getTimelineClient() {
            return timelineClient;
        }

        // Get Timeline Collector's address (get sync from RM)
        public TimelineV2Client getTimelineV2Client() {
            return timelineV2Client;
        }

        @Override
        public String getHistoryUrl() {
            return historyUrl;
        }

        @Override
        public void setHistoryUrl(String historyUrl) {
            this.historyUrl = historyUrl;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void serviceStart() throws Exception {

        /**
         * MRAppMaster 组合服务的 AsyncDispatcher 异步事件分发器注册了哪些事件注册表
         * 1 jobhistory.EventType    -> JobHistoryEventHandler (任务历史事件处理器)
         * 2 JobEventType            -> JobEventDispatcher (任务事件分发器)
         * 3 TaskEventType           -> TaskEventDispatcher (Task事件分发器)
         * 4 TaskAttemptEventType    -> TaskAttemptEventDispatcher (Task 尝试事件分发器)
         * 5 CommitterEventType      -> CommitterEventHandler (提交事件处理器)
         * 6 Speculator.EventType    -> SpeculatorEventDispatcher (推测事件处理器)
         * 7 ContainerAllocator.EventType -> ContainerAllocatorRouter (容器申请事件处理器)
         * 8 ContainerLauncher.EventType  -> ContainerLauncherRouter (启动容器事件处理器)
         * 9 JobFinishEvent.Type          -> JobFinishEventHandler (任务完成事件处理器)
         */

        amInfos = new LinkedList<AMInfo>();
        completedTasksFromPreviousRun = new HashMap<TaskId, TaskInfo>();
        processRecovery();
        cleanUpPreviousJobOutput();

        // Current an AMInfo for the current AM generation.
        // 创建当前 AM 信息 AMInfo
        AMInfo amInfo =
                MRBuilderUtils.newAMInfo(appAttemptID, startTime, containerID, nmHost,
                        nmPort, nmHttpPort);

        // /////////////////// Create the job itself.
        // 创建 JobImpl
        job = createJob(getConfig(), forcedState, shutDownMessage);

        // End of creating the job.

        // Send out an MR AM inited event for all previous AMs.
        // 恢复上一个 AM (默认 amInfos 为空)
        for (AMInfo info : amInfos) {
            dispatcher.getEventHandler().handle(
                    new JobHistoryEvent(job.getID(), new AMStartedEvent(info
                            .getAppAttemptId(), info.getStartTime(), info.getContainerId(),
                            info.getNodeManagerHost(), info.getNodeManagerPort(), info
                            .getNodeManagerHttpPort(), appSubmitTime)));
        }

        // Send out an MR AM inited event for this AM.
        // 调用 JobHistoryEventHandler.handle(AMStartedEvent)
        dispatcher.getEventHandler().handle(
                // 创建 JobHistoryEvent
                new JobHistoryEvent(
                        job.getID(),
                        new AMStartedEvent(
                                amInfo.getAppAttemptId(),
                                amInfo.getStartTime(),
                                amInfo.getContainerId(),
                                amInfo.getNodeManagerHost(),
                                amInfo.getNodeManagerPort(), amInfo
                                .getNodeManagerHttpPort(),
                                this.forcedState == null ? null : this.forcedState.toString(), appSubmitTime)
                ));
        amInfos.add(amInfo);

        // metrics system init is really init & start.
        // It's more test friendly to put it here.
        DefaultMetricsSystem.initialize("MRAppMaster");

        boolean initFailed = false;
        if (!errorHappenedShutDown) {
            // create a job event for job initialization
            // 初始化 Job (也即创建 MapTask ReduceTask 等待后续被执行调度)
            JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
            // Send init to the job (this does NOT trigger job execution)
            // This is a synchronous call, not an event through dispatcher. We want
            // job-init to be done completely here.
            // 调用 JobEventDispatcher.handle(JobEventType.JOB_INIT)
            jobEventDispatcher.handle(initJobEvent);

            // If job is still not initialized, an error happened during
            // initialization. Must complete starting all of the services so failure
            // events can be processed.
            initFailed = (((JobImpl) job).getInternalState() != JobStateInternal.INITED);

            // JobImpl's InitTransition is done (call above is synchronous), so the
            // "uber-decision" (MR-1220) has been made.  Query job and switch to
            // ubermode if appropriate (by registering different container-allocator
            // and container-launcher services/event-handlers).

            // 默认 false
            if (job.isUber()) {
                speculatorEventDispatcher.disableSpeculation();
                LOG.info("MRAppMaster uberizing job " + job.getID()
                        + " in local container (\"uber-AM\") on node "
                        + nmHost + ":" + nmPort + ".");
            } else {
                // send init to speculator only for non-uber jobs.
                // This won't yet start as dispatcher isn't started yet.
                // 调用 SpeculatorEventDispatcher.handle(Speculator.EventType.JOB_CREATE)
                dispatcher.getEventHandler().handle(
                        new SpeculatorEvent(job.getID(), clock.getTime()));
                // MRAppMaster launching normal, non-uberized, multi-container
                // job job_1684656010852_0006
                LOG.info("MRAppMaster launching normal, non-uberized, multi-container "
                        + "job " + job.getID() + ".");
            }
            // Start ClientService here, since it's not initialized if
            // errorHappenedShutDown is true
            // 启动 MRClientService RPC 服务 其调用 serviceStart()
            clientService.start();
        }
        //start all the components
        /**
         * MRAppMaster 组合服务有哪些子服务？调用其 serviceStart()
         * 1 异步事件分发器        -> AsyncDispatcher
         * 2 任务完成监控服务      -> TaskAttemptFinishingMonitor
         * 3 提交者事件处理        -> CommitterEventHandler
         * 4 Task监听服务         -> TaskAttemptListenerImpl
         * 5 任务推测执行服务       -> DefaultSpeculator
         * 6 任务清除目录服务       -> StagingDirCleaningService
         * 7 容器申请器服务         -> ContainerAllocatorRouter
         * 8 启动容器服务          -> ContainerLauncherRouter
         * 9 任务历史事件处理器服务  -> JobHistoryEventHandler
         */
        super.serviceStart();

        // finally set the job classloader
        MRApps.setClassLoader(jobClassLoader, getConfig());

        if (initFailed) {
            JobEvent initFailedEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT_FAILED);
            jobEventDispatcher.handle(initFailedEvent);
        } else {
            // All components have started, start the job.
            // 全部的组件已经启动 开始启动任务
            startJobs();
        }
    }


    @Override
    public void stop() {
        super.stop();
    }

    private boolean isRecoverySupported() throws IOException {
        boolean isSupported = false;
        Configuration conf = getConfig();
        if (committer != null) {
            final JobContext _jobContext = getJobContextFromConf(conf);
            isSupported = callWithJobClassLoader(conf,
                    new ExceptionAction<Boolean>() {
                        public Boolean call(Configuration conf) throws IOException {
                            return committer.isRecoverySupported(_jobContext);
                        }
                    });
        }
        return isSupported;
    }

    private void processRecovery() throws IOException {
        boolean attemptRecovery = shouldAttemptRecovery();
        boolean recoverySucceeded = true;
        if (attemptRecovery) {

            LOG.info("Attempting to recover.");
            try {
                parsePreviousJobHistory();
            } catch (IOException e) {
                LOG.warn("Unable to parse prior job history, aborting recovery", e);
                recoverySucceeded = false;
            }
        }

        if (!isFirstAttempt() && (!attemptRecovery || !recoverySucceeded)) {
            amInfos.addAll(readJustAMInfos());
        }
    }

    private boolean isFirstAttempt() {
        return appAttemptID.getAttemptId() == 1;
    }

    /**
     * Check if the current job attempt should try to recover from previous
     * job attempts if any.
     */
    private boolean shouldAttemptRecovery() throws IOException {
        if (isFirstAttempt()) {
            return false;  // no need to recover on the first attempt
        }

        boolean recoveryEnabled = getConfig().getBoolean(
                MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE,
                MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE_DEFAULT);
        if (!recoveryEnabled) {
            LOG.info("Not attempting to recover. Recovery disabled. To enable " +
                    "recovery, set " + MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE);
            return false;
        }

        boolean recoverySupportedByCommitter = isRecoverySupported();
        if (!recoverySupportedByCommitter) {
            LOG.info("Not attempting to recover. Recovery is not supported by " +
                    committer.getClass() + ". Use an OutputCommitter that supports" +
                    " recovery.");
            return false;
        }

        int reducerCount = getConfig().getInt(MRJobConfig.NUM_REDUCES, 0);

        // If a shuffle secret was not provided by the job client, one will be
        // generated in this job attempt. However, that disables recovery if
        // there are reducers as the shuffle secret would be job attempt specific.
        boolean shuffleKeyValidForRecovery =
                TokenCache.getShuffleSecretKey(jobCredentials) != null;
        if (reducerCount > 0 && !shuffleKeyValidForRecovery) {
            LOG.info("Not attempting to recover. The shuffle key is invalid for " +
                    "recovery.");
            return false;
        }

        // If the intermediate data is encrypted, recovering the job requires the
        // access to the key. Until the encryption key is persisted, we should
        // avoid attempts to recover.
        boolean spillEncrypted = CryptoUtils.isEncryptedSpillEnabled(getConfig());
        if (reducerCount > 0 && spillEncrypted) {
            LOG.info("Not attempting to recover. Intermediate spill encryption" +
                    " is enabled.");
            return false;
        }

        return true;
    }

    private void cleanUpPreviousJobOutput() {
        // recovered application masters should not remove data from previous job
        if (!isFirstAttempt() && !recovered()) {
            JobContext jobContext = getJobContextFromConf(getConfig());
            try {
                LOG.info("Starting to clean up previous job's temporary files");
                this.committer.abortJob(jobContext, State.FAILED);
                LOG.info("Finished cleaning up previous job temporary files");
            } catch (FileNotFoundException e) {
                LOG.info("Previous job temporary files do not exist, " +
                        "no clean up was necessary.");
            } catch (Exception e) {
                // the clean up of a previous attempt is not critical to the success
                // of this job - only logging the error
                LOG.error("Error while trying to clean up previous job's temporary " +
                        "files", e);
            }
        }
    }

    private static FSDataInputStream getPreviousJobHistoryStream(
            Configuration conf, ApplicationAttemptId appAttemptId)
            throws IOException {
        Path historyFile = JobHistoryUtils.getPreviousJobHistoryPath(conf,
                appAttemptId);
        LOG.info("Previous history file is at " + historyFile);
        return historyFile.getFileSystem(conf).open(historyFile);
    }

    private void parsePreviousJobHistory() throws IOException {
        FSDataInputStream in = getPreviousJobHistoryStream(getConfig(),
                appAttemptID);
        JobHistoryParser parser = new JobHistoryParser(in);
        JobInfo jobInfo = parser.parse();
        Exception parseException = parser.getParseException();
        if (parseException != null) {
            LOG.info("Got an error parsing job-history file" +
                    ", ignoring incomplete events.", parseException);
        }
        Map<org.apache.hadoop.mapreduce.TaskID, TaskInfo> taskInfos = jobInfo
                .getAllTasks();
        for (TaskInfo taskInfo : taskInfos.values()) {
            if (TaskState.SUCCEEDED.toString().equals(taskInfo.getTaskStatus())) {
                Iterator<Entry<TaskAttemptID, TaskAttemptInfo>> taskAttemptIterator =
                        taskInfo.getAllTaskAttempts().entrySet().iterator();
                while (taskAttemptIterator.hasNext()) {
                    Map.Entry<TaskAttemptID, TaskAttemptInfo> currentEntry = taskAttemptIterator.next();
                    if (!jobInfo.getAllCompletedTaskAttempts().containsKey(currentEntry.getKey())) {
                        taskAttemptIterator.remove();
                    }
                }
                completedTasksFromPreviousRun
                        .put(TypeConverter.toYarn(taskInfo.getTaskId()), taskInfo);
                LOG.info("Read from history task "
                        + TypeConverter.toYarn(taskInfo.getTaskId()));
            }
        }
        LOG.info("Read completed tasks from history "
                + completedTasksFromPreviousRun.size());
        recoveredJobStartTime = jobInfo.getLaunchTime();

        // recover AMInfos
        List<JobHistoryParser.AMInfo> jhAmInfoList = jobInfo.getAMInfos();
        if (jhAmInfoList != null) {
            for (JobHistoryParser.AMInfo jhAmInfo : jhAmInfoList) {
                AMInfo amInfo = MRBuilderUtils.newAMInfo(jhAmInfo.getAppAttemptId(),
                        jhAmInfo.getStartTime(), jhAmInfo.getContainerId(),
                        jhAmInfo.getNodeManagerHost(), jhAmInfo.getNodeManagerPort(),
                        jhAmInfo.getNodeManagerHttpPort());
                amInfos.add(amInfo);
            }
        }
    }

    private List<AMInfo> readJustAMInfos() {
        List<AMInfo> amInfos = new ArrayList<AMInfo>();
        FSDataInputStream inputStream = null;
        try {
            inputStream = getPreviousJobHistoryStream(getConfig(), appAttemptID);
            EventReader jobHistoryEventReader = new EventReader(inputStream);

            // All AMInfos are contiguous. Track when the first AMStartedEvent
            // appears.
            boolean amStartedEventsBegan = false;

            HistoryEvent event;
            while ((event = jobHistoryEventReader.getNextEvent()) != null) {
                if (event.getEventType() == EventType.AM_STARTED) {
                    if (!amStartedEventsBegan) {
                        // First AMStartedEvent.
                        amStartedEventsBegan = true;
                    }
                    AMStartedEvent amStartedEvent = (AMStartedEvent) event;
                    amInfos.add(MRBuilderUtils.newAMInfo(
                            amStartedEvent.getAppAttemptId(), amStartedEvent.getStartTime(),
                            amStartedEvent.getContainerId(),
                            StringInterner.weakIntern(amStartedEvent.getNodeManagerHost()),
                            amStartedEvent.getNodeManagerPort(),
                            amStartedEvent.getNodeManagerHttpPort()));
                } else if (amStartedEventsBegan) {
                    // This means AMStartedEvents began and this event is a
                    // non-AMStarted event.
                    // No need to continue reading all the other events.
                    break;
                }
            }
        } catch (IOException e) {
            LOG.warn("Could not parse the old history file. "
                    + "Will not have old AMinfos ", e);
        } finally {
            if (inputStream != null) {
                IOUtils.closeQuietly(inputStream);
            }
        }
        return amInfos;
    }

    public boolean recovered() {
        return recoveredJobStartTime > 0;
    }

    /**
     * This can be overridden to instantiate multiple jobs and create a
     * workflow.
     * <p>
     * TODO:  Rework the design to actually support this.  Currently much of the
     * job stuff has been moved to init() above to support uberization (MR-1220).
     * In a typical workflow, one presumably would want to uberize only a subset
     * of the jobs (the "small" ones), which is awkward with the current design.
     */
    @SuppressWarnings("unchecked")
    protected void startJobs() {
        /** create a job-start event to get this ball rolling */
        // 创建 job-start 事件
        // eventType = JobEventType.JOB_START
        JobEvent startJobEvent = new JobStartEvent(job.getID(),
                recoveredJobStartTime);
        /** send the job-start event. this triggers the job execution. */
        // 调用 JobEventDispatcher.handle(JobEventType.JOB_START)
        dispatcher.getEventHandler().handle(startJobEvent);
    }

    private class JobEventDispatcher implements EventHandler<JobEvent> {
        @SuppressWarnings("unchecked")
        @Override
        public void handle(JobEvent event) {
            // eventType = JobEventType.JOB_INIT
            // 调用 JobImpl.handle(JobEventType.JOB_INIT)

            // eventType = JobEventType.JOB_START
            // 调用 JobImpl.handle(JobEventType.JOB_START)

            // eventType = JobEventType.JOB_SETUP_COMPLETED
            // 调用 JobImpl.handle(JobEventType.JOB_SETUP_COMPLETED)

            // evenType = JobEventType.JOB_COMPLETED
            // 调用  JobImpl.handle(JobEventType.JOB_COMPLETED)
            ((EventHandler<JobEvent>) context.getJob(event.getJobId())).handle(event);
        }
    }

    private class TaskEventDispatcher implements EventHandler<TaskEvent> {
        @SuppressWarnings("unchecked")
        @Override
        public void handle(TaskEvent event) {
            // eventType = TaskEventType.T_SCHEDULE
            Task task = context.getJob(event.getTaskID().getJobId()).getTask(
                    event.getTaskID());
            // 如果是 MapTask 调用 MapTaskImpl 父类的 TaskImpl.handle(TaskEventType.T_SCHEDULE)
            // 如果是 ReduceTask 调用 ReduceTaskImpl 父类的 TaskImpl.handle(TaskEventType.T_SCHEDULE)
            ((EventHandler<TaskEvent>) task).handle(event);
        }
    }

    private class TaskAttemptEventDispatcher
            implements EventHandler<TaskAttemptEvent> {
        @SuppressWarnings("unchecked")
        @Override
        public void handle(TaskAttemptEvent event) {
            // eventType = TaskAttemptEventType.TA_SCHEDULE
            // eventType = TaskAttemptEventType.TA_ASSIGNED
            Job job = context.getJob(event.getTaskAttemptID().getTaskId().getJobId());
            Task task = job.getTask(event.getTaskAttemptID().getTaskId());
            TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());
            // 如果是 MapTask 调用 MapTaskAttemptImpl 的父类 TaskAttemptImpl.handle(TaskAttemptEventType.TA_SCHEDULE)
            // 如果是 ReduceTask 调用 ReduceTaskAttemptImpl 的父类 TaskAttemptImpl.handle(TaskAttemptEventType.TA_SCHEDULE)

            // 如果是 MapTask 调用 MapTaskAttemptImpl 的父类 TaskAttemptImpl.handle(TaskAttemptEventType.TA_ASSIGNED)
            // 如果是 ReduceTask 调用 ReduceTaskAttemptImpl 的父类 TaskAttemptImpl.handle(TaskAttemptEventType.TA_ASSIGNED)
            ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
        }
    }

    private class SpeculatorEventDispatcher implements
            EventHandler<SpeculatorEvent> {
        private final Configuration conf;
        private volatile boolean disabled;

        public SpeculatorEventDispatcher(Configuration config) {
            this.conf = config;
        }

        @Override
        public void handle(final SpeculatorEvent event) {

            if (disabled) {
                return;
            }

            TaskId tId = event.getTaskID();
            TaskType tType = null;
            /* event's TaskId will be null if the event type is JOB_CREATE or
             * ATTEMPT_STATUS_UPDATE
             */
            if (tId != null) {
                tType = tId.getTaskType();
            }
            boolean shouldMapSpec =
                    conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false);
            boolean shouldReduceSpec =
                    conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

            /* The point of the following is to allow the MAP and REDUCE speculative
             * config values to be independent:
             * IF spec-exec is turned on for maps AND the task is a map task
             * OR IF spec-exec is turned on for reduces AND the task is a reduce task
             * THEN call the speculator to handle the event.
             */
            if ((shouldMapSpec && (tType == null || tType == TaskType.MAP))
                    || (shouldReduceSpec && (tType == null || tType == TaskType.REDUCE))) {
                // Speculator IS enabled, direct the event to there.
                callWithJobClassLoader(conf, new Action<Void>() {
                    public Void call(Configuration conf) {
                        // event = Speculator.EventType.JOB_CREATE
                        // 调用 DefaultSpeculator.handle(Speculator.EventType.JOB_CREATE)

                        // eventType = Speculator.EventType.TASK_CONTAINER_NEED_UPDATE
                        // 调用 DefaultSpeculator.handle(Speculator.EventType.TASK_CONTAINER_NEED_UPDATE)
                        speculator.handle(event);
                        return null;
                    }
                });
            }
        }

        public void disableSpeculation() {
            disabled = true;
        }

    }

    /**
     * Eats events that are not needed in some error cases.
     */
    private static class NoopEventHandler implements EventHandler<Event> {

        @Override
        public void handle(Event event) {
            //Empty
        }
    }

    private static void validateInputParam(String value, String param)
            throws IOException {
        if (value == null) {
            String msg = param + " is null";
            LOG.error(msg);
            throw new IOException(msg);
        }
    }

    public static void main(String[] args) {
        try {
            mainStarted = true;
            Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
            // 从当前 NodeManager 的节点获取当前启动容器的环境变量 key = CONTAINER_ID
            /**
             * 这些环境变量存储在 launch_container 脚本里
             * 以 export key = value 形式暴露
             */
            String containerIdStr =
                    System.getenv(Environment.CONTAINER_ID.name());
            String nodeHostString = System.getenv(Environment.NM_HOST.name());
            String nodePortString = System.getenv(Environment.NM_PORT.name());
            String nodeHttpPortString =
                    System.getenv(Environment.NM_HTTP_PORT.name());
            String appSubmitTimeStr =
                    System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
            validateInputParam(containerIdStr,
                    Environment.CONTAINER_ID.name());
            validateInputParam(nodeHostString, Environment.NM_HOST.name());
            validateInputParam(nodePortString, Environment.NM_PORT.name());
            validateInputParam(nodeHttpPortString,
                    Environment.NM_HTTP_PORT.name());
            validateInputParam(appSubmitTimeStr,
                    ApplicationConstants.APP_SUBMIT_TIME_ENV);

            ContainerId containerId = ContainerId.fromString(containerIdStr);
            ApplicationAttemptId applicationAttemptId =
                    containerId.getApplicationAttemptId();
            if (applicationAttemptId != null) {
                CallerContext.setCurrent(new CallerContext.Builder(
                        "mr_appmaster_" + applicationAttemptId.toString()).build());
            }
            long appSubmitTime = Long.parseLong(appSubmitTimeStr);

            // 创建 MRAppMaster
            MRAppMaster appMaster =
                    new MRAppMaster(
                            applicationAttemptId,
                            containerId,
                            nodeHostString,
                            Integer.parseInt(nodePortString),
                            Integer.parseInt(nodeHttpPortString),
                            appSubmitTime);

            ShutdownHookManager.get().addShutdownHook(
                    new MRAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);
            JobConf conf = new JobConf(new YarnConfiguration());
            conf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));

            MRWebAppUtil.initialize(conf);
            // log the system properties
            String systemPropsToLog = MRApps.getSystemPropertiesToLog(conf);
            if (systemPropsToLog != null) {
                /**
                 * /************************************************************
                 * [system properties]
                 * os.name: Linux
                 * os.version: 3.10.0-1062.el7.x86_64
                 * java.home: /opt/app/jdk1.8.0_212/jre
                 * java.runtime.version: 1.8.0_212-b10
                 * java.vendor: Oracle Corporation
                 * java.version: 1.8.0_212
                 * java.vm.name: Java HotSpot(TM) 64-Bit Server VM
                 * java.class.path: /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/tanbs/appcache/application_1684656010852_0006/container_1684656010852_0006_01_000001:/opt/app/hadoop-3.1.3/etc/hadoop:/opt/app/hadoop-3.1.3/share/hadoop/common/hadoop-common-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/hadoop-common-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/hadoop-kms-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/hadoop-nfs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-io-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/accessors-smart-1.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jcip-annotations-1.0-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/animal-sniffer-annotations-1.17.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/netty-3.10.5.Final.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/asm-5.0.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jersey-core-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/audience-annotations-0.5.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/nimbus-jose-jwt-4.41.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/avro-1.7.7.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-security-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/checker-qual-2.5.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-server-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-beanutils-1.9.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/paranamer-2.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-cli-1.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jsr311-api-1.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-codec-1.11.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jersey-json-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-collections-3.2.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jul-to-slf4j-1.7.25.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-compress-1.18.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jersey-servlet-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-configuration2-2.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-io-2.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/re2j-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-lang-2.6.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-admin-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-lang3-3.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-client-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-logging-1.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-common-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-math3-3.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/slf4j-api-1.7.25.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-net-3.6.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-core-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/curator-client-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-crypto-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/curator-framework-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-identity-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/curator-recipes-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jersey-server-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/error_prone_annotations-2.2.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-server-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/failureaccess-1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/gson-2.2.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/snappy-java-1.0.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/guava-27.0-jre.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-simplekdc-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/hadoop-annotations-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-util-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/hadoop-auth-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jsch-0.1.54.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/htrace-core4-4.1.0-incubating.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/stax2-api-3.1.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/httpclient-4.5.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/token-provider-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/httpcore-4.4.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-asn1-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/j2objc-annotations-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jettison-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-annotations-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-config-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-core-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-pkix-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-util-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-databind-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-xdr-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-http-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/log4j-1.2.17.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/javax.servlet-api-3.1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/woodstox-core-5.0.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jaxb-api-2.2.11.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/metrics-core-3.2.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-servlet-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/json-smart-2.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-util-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jsp-api-2.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-webapp-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jsr305-3.0.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-xml-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/zookeeper-3.4.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-client-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-client-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-httpfs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-native-client-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-native-client-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-nfs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-rbf-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-rbf-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-http-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/accessors-smart-1.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jaxb-impl-2.2.3-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/animal-sniffer-annotations-1.17.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/log4j-1.2.17.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/asm-5.0.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jcip-annotations-1.0-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/audience-annotations-0.5.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/netty-3.10.5.Final.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/avro-1.7.7.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-io-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/checker-qual-2.5.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-security-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-beanutils-1.9.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/netty-all-4.0.52.Final.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/json-smart-2.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-codec-1.11.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jersey-core-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-collections-3.2.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jsr305-3.0.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-compress-1.18.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jersey-server-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-configuration2-2.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jsr311-api-1.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/nimbus-jose-jwt-4.41.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-io-2.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/okhttp-2.7.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-lang-2.6.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-admin-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-lang3-3.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-client-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-common-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-math3-3.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/okio-1.6.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-net-3.6.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-core-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/curator-client-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-crypto-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/curator-framework-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-identity-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/curator-recipes-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jersey-json-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/error_prone_annotations-2.2.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-server-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/failureaccess-1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/paranamer-2.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/gson-2.2.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/guava-27.0-jre.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-simplekdc-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/hadoop-annotations-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-util-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/hadoop-auth-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-webapp-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/htrace-core4-4.1.0-incubating.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/re2j-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/httpclient-4.5.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/snappy-java-1.0.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/httpcore-4.4.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-asn1-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/j2objc-annotations-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jersey-servlet-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-annotations-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-config-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-core-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-pkix-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-util-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-databind-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-xdr-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-jaxrs-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jettison-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-xc-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/javax.servlet-api-3.1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/stax2-api-3.1.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jaxb-api-2.2.11.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-server-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-xml-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-servlet-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jsch-0.1.54.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-util-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/json-simple-1.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-util-ajax-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/token-provider-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/woodstox-core-5.0.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/zookeeper-3.4.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-api-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-client-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-common-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-registry-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-common-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-nodemanager-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-router-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-tests-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-timeline-pluginstorage-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-web-proxy-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-services-api-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-services-core-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/HikariCP-java7-2.4.12.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/aopalliance-1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/dnsjava-2.1.7.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/ehcache-3.3.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/fst-2.50.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/geronimo-jcache_1.0_spec-1.0-alpha-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/guice-4.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/guice-servlet-4.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jackson-jaxrs-base-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jackson-jaxrs-json-provider-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jackson-module-jaxb-annotations-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/java-util-1.9.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/javax.inject-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jersey-client-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jersey-guice-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/json-io-2.5.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/metrics-core-3.2.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/mssql-jdbc-6.2.1.jre7.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/objenesis-1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/snakeyaml-1.16.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/swagger-annotations-1.5.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-nativetask-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-uploader-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/lib/junit-4.11.jar:job.jar/job.jar:job.jar/classes/:job.jar/lib/*:/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/tanbs/appcache/application_1684656010852_0006/container_1684656010852_0006_01_000001/job.jar
                 * java.io.tmpdir: /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/tanbs/appcache/application_1684656010852_0006/container_1684656010852_0006_01_000001/tmp
                 * user.dir: /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/tanbs/appcache/application_1684656010852_0006/container_1684656010852_0006_01_000001
                 * user.name: tanbs
                 * ************************************************************
                 */
                LOG.info(systemPropsToLog);
            }

            String jobUserName = System
                    .getenv(ApplicationConstants.Environment.USER.name());
            conf.set(MRJobConfig.USER_NAME, jobUserName);

            // 初始化并启动 AM
            initAndStartAppMaster(appMaster, conf, jobUserName);
        } catch (Throwable t) {
            LOG.error("Error starting MRAppMaster", t);
            ExitUtil.terminate(1, t);
        }
    }

    // The shutdown hook that runs when a signal is received AND during normal
    // close of the JVM.
    static class MRAppMasterShutdownHook implements Runnable {
        MRAppMaster appMaster;

        MRAppMasterShutdownHook(MRAppMaster appMaster) {
            this.appMaster = appMaster;
        }

        public void run() {
            LOG.info("MRAppMaster received a signal. Signaling RMCommunicator and "
                    + "JobHistoryEventHandler.");

            // Notify the JHEH and RMCommunicator that a SIGTERM has been received so
            // that they don't take too long in shutting down
            if (appMaster.containerAllocator instanceof ContainerAllocatorRouter) {
                ((ContainerAllocatorRouter) appMaster.containerAllocator)
                        .setSignalled(true);
            }
            appMaster.notifyIsLastAMRetry(appMaster.isLastAMRetry);
            appMaster.stop();
        }
    }

    public void notifyIsLastAMRetry(boolean isLastAMRetry) {
        if (containerAllocator instanceof ContainerAllocatorRouter) {
            LOG.info("Notify RMCommunicator isAMLastRetry: " + isLastAMRetry);
            ((ContainerAllocatorRouter) containerAllocator)
                    .setShouldUnregister(isLastAMRetry);
        }
        if (jobHistoryEventHandler != null) {
            LOG.info("Notify JHEH isAMLastRetry: " + isLastAMRetry);
            jobHistoryEventHandler.setForcejobCompletion(isLastAMRetry);
        }
    }

    protected static void initAndStartAppMaster(final MRAppMaster appMaster,
                                                final JobConf conf, String jobUserName) throws IOException,
            InterruptedException {
        UserGroupInformation.setConfiguration(conf);
        // MAPREDUCE-6565: need to set configuration for SecurityUtil.
        SecurityUtil.setConfiguration(conf);
        // Security framework already loaded the tokens into current UGI, just use
        // them
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();

        // Executing with tokens:
        // [Kind: YARN_AM_RM_TOKEN, Service: ,
        // Ident: (appAttemptId { application_id
        // { id: 6 cluster_timestamp: 1684656010852 } attemptId: 1 } keyId: -1993740727)]
        LOG.info("Executing with tokens: {}", credentials.getAllTokens());

        UserGroupInformation appMasterUgi = UserGroupInformation
                .createRemoteUser(jobUserName);
        appMasterUgi.addCredentials(credentials);

        // Now remove the AM->RM token so tasks don't have it
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        conf.getCredentials().addAll(credentials);
        appMasterUgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                // 初始化 MRAppMaster (调用 MRAppMaster.serviceInit())
                appMaster.init(conf);
                // 启动 MRAppMaster (调用 MRAppMaster.serviceStart())
                appMaster.start();
                if (appMaster.errorHappenedShutDown) {
                    throw new IOException("Was asked to shut down.");
                }
                return null;
            }
        });
    }

    /**
     * Creates a job classloader based on the configuration if the job classloader
     * is enabled. It is a no-op if the job classloader is not enabled.
     */
    private void createJobClassLoader(Configuration conf) throws IOException {
        jobClassLoader = MRApps.createJobClassLoader(conf);
    }

    /**
     * Executes the given action with the job classloader set as the configuration
     * classloader as well as the thread context class loader if the job
     * classloader is enabled. After the call, the original classloader is
     * restored.
     * <p>
     * If the job classloader is enabled and the code needs to load user-supplied
     * classes via configuration or thread context classloader, this method should
     * be used in order to load them.
     *
     * @param conf   the configuration on which the classloader will be set
     * @param action the callable action to be executed
     */
    <T> T callWithJobClassLoader(Configuration conf, Action<T> action) {
        // if the job classloader is enabled, we may need it to load the (custom)
        // classes; we make the job classloader available and unset it once it is
        // done
        ClassLoader currentClassLoader = conf.getClassLoader();
        boolean setJobClassLoader =
                jobClassLoader != null && currentClassLoader != jobClassLoader;
        if (setJobClassLoader) {
            MRApps.setClassLoader(jobClassLoader, conf);
        }
        try {
            return action.call(conf);
        } finally {
            if (setJobClassLoader) {
                // restore the original classloader
                MRApps.setClassLoader(currentClassLoader, conf);
            }
        }
    }

    /**
     * Executes the given action that can throw a checked exception with the job
     * classloader set as the configuration classloader as well as the thread
     * context class loader if the job classloader is enabled. After the call, the
     * original classloader is restored.
     * <p>
     * If the job classloader is enabled and the code needs to load user-supplied
     * classes via configuration or thread context classloader, this method should
     * be used in order to load them.
     *
     * @param conf   the configuration on which the classloader will be set
     * @param action the callable action to be executed
     * @throws IOException          if the underlying action throws an IOException
     * @throws YarnRuntimeException if the underlying action throws an exception
     *                              other than an IOException
     */
    <T> T callWithJobClassLoader(Configuration conf, ExceptionAction<T> action)
            throws IOException {
        // if the job classloader is enabled, we may need it to load the (custom)
        // classes; we make the job classloader available and unset it once it is
        // done
        ClassLoader currentClassLoader = conf.getClassLoader();
        boolean setJobClassLoader =
                jobClassLoader != null && currentClassLoader != jobClassLoader;
        if (setJobClassLoader) {
            MRApps.setClassLoader(jobClassLoader, conf);
        }
        try {
            return action.call(conf);
        } catch (IOException e) {
            throw e;
        } catch (YarnRuntimeException e) {
            throw e;
        } catch (Exception e) {
            // wrap it with a YarnRuntimeException
            throw new YarnRuntimeException(e);
        } finally {
            if (setJobClassLoader) {
                // restore the original classloader
                MRApps.setClassLoader(currentClassLoader, conf);
            }
        }
    }

    /**
     * Action to be wrapped with setting and unsetting the job classloader
     */
    private static interface Action<T> {
        T call(Configuration conf);
    }

    private static interface ExceptionAction<T> {
        T call(Configuration conf) throws Exception;
    }

    @Override
    protected void serviceStop() throws Exception {
        super.serviceStop();
    }

    public ClientService getClientService() {
        return clientService;
    }
}
