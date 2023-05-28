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

package org.apache.hadoop.yarn.server.nodemanager;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.NodeHealthScriptRunner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.collectormanager.NMCollectorService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.ConfigurationNodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.ScriptBasedNodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMLeveldbStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.state.MultiStateTransitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeManager extends CompositeService
        implements EventHandler<NodeManagerEvent>, NodeManagerMXBean {

    /**
     * Node manager return status codes.
     */
    public enum NodeManagerStatus {
        NO_ERROR(0),
        EXCEPTION(1);

        private int exitCode;

        NodeManagerStatus(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }

    /**
     * Priority of the NodeManager shutdown hook.
     */
    public static final int SHUTDOWN_HOOK_PRIORITY = 30;

    private static final Logger LOG =
            LoggerFactory.getLogger(NodeManager.class);
    private static long nmStartupTime = System.currentTimeMillis();
    protected final NodeManagerMetrics metrics = NodeManagerMetrics.create();
    private JvmPauseMonitor pauseMonitor;
    private ApplicationACLsManager aclsManager;
    private NodeHealthCheckerService nodeHealthChecker;
    private NodeLabelsProvider nodeLabelsProvider;
    private LocalDirsHandlerService dirsHandler;
    private Context context;
    private AsyncDispatcher dispatcher;
    private ContainerManagerImpl containerManager;
    // the NM collector service is set only if the timeline service v.2 is enabled
    private NMCollectorService nmCollectorService;
    private NodeStatusUpdater nodeStatusUpdater;
    private AtomicBoolean resyncingWithRM = new AtomicBoolean(false);
    private NodeResourceMonitor nodeResourceMonitor;
    private static CompositeServiceShutdownHook nodeManagerShutdownHook;
    private NMStateStoreService nmStore = null;

    private AtomicBoolean isStopping = new AtomicBoolean(false);
    private boolean rmWorkPreservingRestartEnabled;
    private boolean shouldExitOnShutdownEvent = false;

    private NMLogAggregationStatusTracker nmLogAggregationStatusTracker;

    /**
     * Default Container State transition listener.
     */
    public static class DefaultContainerStateListener extends
            MultiStateTransitionListener
                    <ContainerImpl, ContainerEvent, ContainerState>
            implements ContainerStateTransitionListener {
        @Override
        public void init(Context context) {
        }
    }

    public NodeManager() {
        // 往下追
        super(NodeManager.class.getName());
    }

    public static long getNMStartupTime() {
        return nmStartupTime;
    }

    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
                                                        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        // 创建 NodeStatusUpdaterImpl
        return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
                metrics, nodeLabelsProvider);
    }

    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
                                                        Dispatcher dispatcher, NodeHealthCheckerService healthChecker,
                                                        NodeLabelsProvider nodeLabelsProvider) {
        return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
                metrics, nodeLabelsProvider);
    }

    protected NodeLabelsProvider createNodeLabelsProvider(Configuration conf)
            throws IOException {
        NodeLabelsProvider provider = null;
        // 默认 null
        String providerString =
                conf.get(YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG, null);
        if (providerString == null || providerString.trim().length() == 0) {
            // Seems like Distributed Node Labels configuration is not enabled
            return provider;
        }
        switch (providerString.trim().toLowerCase()) {
            case YarnConfiguration.CONFIG_NODE_LABELS_PROVIDER:
                provider = new ConfigurationNodeLabelsProvider();
                break;
            case YarnConfiguration.SCRIPT_NODE_LABELS_PROVIDER:
                provider = new ScriptBasedNodeLabelsProvider();
                break;
            default:
                try {
                    Class<? extends NodeLabelsProvider> labelsProviderClass =
                            conf.getClass(YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG,
                                    null, NodeLabelsProvider.class);
                    provider = labelsProviderClass.newInstance();
                } catch (InstantiationException | IllegalAccessException
                        | RuntimeException e) {
                    LOG.error("Failed to create NodeLabelsProvider based on Configuration",
                            e);
                    throw new IOException(
                            "Failed to create NodeLabelsProvider : " + e.getMessage(), e);
                }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Distributed Node Labels is enabled"
                    + " with provider class as : " + provider.getClass().toString());
        }
        return provider;
    }

    protected NodeResourceMonitor createNodeResourceMonitor() {
        // 创建 NodeResourceMonitorImpl
        return new NodeResourceMonitorImpl(context);
    }

    protected ContainerManagerImpl createContainerManager(Context context,
                                                          ContainerExecutor exec, DeletionService del,
                                                          NodeStatusUpdater nodeStatusUpdater, ApplicationACLsManager aclsManager,
                                                          LocalDirsHandlerService dirsHandler) {
        // 创建 ContainerManagerImpl
        return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
                metrics, dirsHandler);
    }

    protected NMCollectorService createNMCollectorService(Context ctxt) {
        return new NMCollectorService(ctxt);
    }

    protected WebServer createWebServer(Context nmContext,
                                        ResourceView resourceView, ApplicationACLsManager aclsManager,
                                        LocalDirsHandlerService dirsHandler) {
        // 创建 WebServer
        return new WebServer(nmContext, resourceView, aclsManager, dirsHandler);
    }

    protected DeletionService createDeletionService(ContainerExecutor exec) {
        // 创建 DeletionService
        return new DeletionService(exec, nmStore);
    }

    protected NMContext createNMContext(
            NMContainerTokenSecretManager containerTokenSecretManager,
            NMTokenSecretManagerInNM nmTokenSecretManager,
            NMStateStoreService stateStore, boolean isDistSchedulerEnabled,
            Configuration conf) {
        // 容器状态监听器 默认为空
        List<ContainerStateTransitionListener> listeners =
                conf.getInstances(
                        YarnConfiguration.NM_CONTAINER_STATE_TRANSITION_LISTENERS,
                        ContainerStateTransitionListener.class);

        // 创建 NM 上下文对象 NMContext
        NMContext nmContext = new NMContext(containerTokenSecretManager,
                nmTokenSecretManager, dirsHandler, aclsManager, stateStore,
                isDistSchedulerEnabled, conf);

        nmContext.setNodeManagerMetrics(metrics);

        DefaultContainerStateListener defaultListener =
                new DefaultContainerStateListener();
        nmContext.setContainerStateTransitionListener(defaultListener);
        // 啥也不干
        defaultListener.init(nmContext);

        for (ContainerStateTransitionListener listener : listeners) {
            listener.init(nmContext);
            defaultListener.addListener(listener);
        }

        return nmContext;
    }

    protected void doSecureLogin() throws IOException {
        SecurityUtil.login(getConfig(), YarnConfiguration.NM_KEYTAB,
                YarnConfiguration.NM_PRINCIPAL);
    }

    private void initAndStartRecoveryStore(Configuration conf)
            throws IOException {
        // 默认 false
        boolean recoveryEnabled = conf.getBoolean(
                YarnConfiguration.NM_RECOVERY_ENABLED,
                YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
        if (recoveryEnabled) {
            FileSystem recoveryFs = FileSystem.getLocal(conf);
            String recoveryDirName = conf.get(YarnConfiguration.NM_RECOVERY_DIR);
            if (recoveryDirName == null) {
                throw new IllegalArgumentException("Recovery is enabled but " +
                        YarnConfiguration.NM_RECOVERY_DIR + " is not set.");
            }
            Path recoveryRoot = new Path(recoveryDirName);
            recoveryFs.mkdirs(recoveryRoot, new FsPermission((short) 0700));
            nmStore = new NMLeveldbStateStoreService();
        } else {
            nmStore = new NMNullStateStoreService();
        }
        nmStore.init(conf);
        nmStore.start();
    }

    private void stopRecoveryStore() throws IOException {
        if (null != nmStore) {
            nmStore.stop();
            if (null != context) {
                if (context.getDecommissioned() && nmStore.canRecover()) {
                    LOG.info("Removing state store due to decommission");
                    Configuration conf = getConfig();
                    Path recoveryRoot =
                            new Path(conf.get(YarnConfiguration.NM_RECOVERY_DIR));
                    LOG.info("Removing state store at " + recoveryRoot
                            + " due to decommission");
                    FileSystem recoveryFs = FileSystem.getLocal(conf);
                    if (!recoveryFs.delete(recoveryRoot, true)) {
                        LOG.warn("Unable to delete " + recoveryRoot);
                    }
                }
            }
        }
    }

    private void recoverTokens(NMTokenSecretManagerInNM nmTokenSecretManager,
                               NMContainerTokenSecretManager containerTokenSecretManager)
            throws IOException {
        if (nmStore.canRecover()) {
            nmTokenSecretManager.recover();
            containerTokenSecretManager.recover();
        }
    }

    public static NodeHealthScriptRunner getNodeHealthScriptRunner(Configuration conf) {
        // 默认为 null
        String nodeHealthScript =
                conf.get(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH);
        // 默认没有配置节点监控检查执行脚本
        if (!NodeHealthScriptRunner.shouldRun(nodeHealthScript)) {
            LOG.info("Node Manager health check script is not available "
                    + "or doesn't have execute permission, so not "
                    + "starting the node health script runner.");
            return null;
        }
        long nmCheckintervalTime = conf.getLong(
                YarnConfiguration.NM_HEALTH_CHECK_INTERVAL_MS,
                YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_INTERVAL_MS);
        long scriptTimeout = conf.getLong(
                YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS,
                YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS);
        String[] scriptArgs = conf.getStrings(
                YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_OPTS, new String[]{});
        return new NodeHealthScriptRunner(nodeHealthScript,
                nmCheckintervalTime, scriptTimeout, scriptArgs);
    }

    @VisibleForTesting
    protected ResourcePluginManager createResourcePluginManager() {
        // 创建 ResourcePluginManager
        return new ResourcePluginManager();
    }

    @VisibleForTesting
    protected ContainerExecutor createContainerExecutor(Configuration conf) {
        // 反射创建 org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor 无参构造
        // 并调用其 setConf() 设置配置信息
        return ReflectionUtils.newInstance(
                conf.getClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
                        DefaultContainerExecutor.class, ContainerExecutor.class), conf);
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        UserGroupInformation.setConfiguration(conf);

        // 默认 true
        rmWorkPreservingRestartEnabled = conf.getBoolean(YarnConfiguration
                        .RM_WORK_PRESERVING_RECOVERY_ENABLED,
                YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED);

        try {
            // 如果 NodeManager 开启了持久化恢复 则执行恢复操作(默认不开启该功能)
            initAndStartRecoveryStore(conf);
        } catch (IOException e) {
            String recoveryDirName = conf.get(YarnConfiguration.NM_RECOVERY_DIR);
            throw new
                    YarnRuntimeException("Unable to initialize recovery directory at "
                    + recoveryDirName, e);
        }

        // 创建容器认证管理 NMContainerTokenSecretManager
        NMContainerTokenSecretManager containerTokenSecretManager =
                new NMContainerTokenSecretManager(conf, nmStore);

        // 创建 NodeManager Token Secret 管理者
        NMTokenSecretManagerInNM nmTokenSecretManager =
                new NMTokenSecretManagerInNM(nmStore);

        // 恢复 Token
        recoverTokens(nmTokenSecretManager, containerTokenSecretManager);

        // 创建 Application 认证管理器 ApplicationACLsManager
        this.aclsManager = new ApplicationACLsManager(conf);

        // 创建周期性检查当前 NodeManager 健康状态信息并保存到本地磁盘 LocalDirsHandlerService 服务
        this.dirsHandler = new LocalDirsHandlerService(metrics);

        // 默认 false
        boolean isDistSchedulingEnabled =
                conf.getBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED,
                        YarnConfiguration.DEFAULT_DIST_SCHEDULING_ENABLED);

        // 创建 NodeManager 上下文对象 NMContext
        this.context = createNMContext(
                containerTokenSecretManager,
                nmTokenSecretManager,
                nmStore, isDistSchedulingEnabled, conf);

        // 创建资源插件管理 ResourcePluginManager
        ResourcePluginManager pluginManager = createResourcePluginManager();
        // 初始化 ResourcePluginManager (默认啥也不干)
        pluginManager.initialize(context);
        ((NMContext) context).setResourcePluginManager(pluginManager);

        // 反射创建容器执行器 默认 org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor
        ContainerExecutor exec = createContainerExecutor(conf);
        try {
            // 初始化容器执行器 (啥也不干)
            exec.init(context);
        } catch (IOException e) {
            throw new YarnRuntimeException("Failed to initialize container executor", e);
        }
        // 创建并添加 DeletionService 服务
        DeletionService del = createDeletionService(exec);
        addService(del);

        // NodeManager level dispatcher
        // 创建 NodeManager 的异步分发器 AsyncDispatcher
        this.dispatcher = createNMDispatcher();

        // 创建并添加节点监控检查服务 NodeHealthCheckerService
        nodeHealthChecker =
                new NodeHealthCheckerService(
                        // 获取节点监控检查执行脚本 (默认没有配置节点监控检查执行脚本)
                        getNodeHealthScriptRunner(conf),
                        dirsHandler);
        addService(nodeHealthChecker);

        ((NMContext) context).setContainerExecutor(exec);
        ((NMContext) context).setDeletionService(del);

        // 创建节点标签提供商 默认为 null
        nodeLabelsProvider = createNodeLabelsProvider(conf);
        if (null == nodeLabelsProvider) {
            // 创建节点状态更新对象 NodeStatusUpdaterImpl
            nodeStatusUpdater =
                    createNodeStatusUpdater(context, dispatcher, nodeHealthChecker);
        } else {
            addIfService(nodeLabelsProvider);
            nodeStatusUpdater =
                    createNodeStatusUpdater(context, dispatcher, nodeHealthChecker,
                            nodeLabelsProvider);
        }

        // 创建并添加节点资源监控服务 NodeResourceMonitorImpl
        nodeResourceMonitor = createNodeResourceMonitor();
        addService(nodeResourceMonitor);
        ((NMContext) context).setNodeResourceMonitor(nodeResourceMonitor);

        // 创建并添加容器管理服务 ContainerManagerImpl (组合服务)
        containerManager =
                createContainerManager(context, exec, del, nodeStatusUpdater,
                        this.aclsManager, dirsHandler);
        addService(containerManager);
        ((NMContext) context).setContainerManager(containerManager);

        // 创建并添加 NM 日志聚合追踪服务 NMLogAggregationStatusTracker
        this.nmLogAggregationStatusTracker = createNMLogAggregationStatusTracker(
                context);
        addService(nmLogAggregationStatusTracker);
        ((NMContext) context).setNMLogAggregationStatusTracker(
                this.nmLogAggregationStatusTracker);

        // 创建并添加 NM Web 服务 WebServer
        WebServer webServer = createWebServer(context, containerManager
                .getContainersMonitor(), this.aclsManager, dirsHandler);
        addService(webServer);
        ((NMContext) context).setWebServer(webServer);

        ((NMContext) context).setQueueableContainerAllocator(
                new OpportunisticContainerAllocator(
                        context.getContainerTokenSecretManager()));

        // 往 NodeManager 的 AsyncDispatcher 注册 EventType 与 EventHandler 映射表
        dispatcher.register(ContainerManagerEventType.class, containerManager);
        dispatcher.register(NodeManagerEventType.class, this);
        addService(dispatcher);

        // 创建并添加 JVM 监控服务 JvmPauseMonitor
        pauseMonitor = new JvmPauseMonitor();
        addService(pauseMonitor);
        metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);

        DefaultMetricsSystem.initialize("NodeManager");

        // 默认不开启
        if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
            this.nmCollectorService = createNMCollectorService(context);
            addService(nmCollectorService);
        }

        // StatusUpdater should be added last so that it get started last
        // so that we make sure everything is up before registering with RM.
        // 创建并添加节点状态更新服务 NodeStatusUpdaterImpl
        addService(nodeStatusUpdater);
        ((NMContext) context).setNodeStatusUpdater(nodeStatusUpdater);
        nmStore.setNodeStatusUpdater(nodeStatusUpdater);

        // Do secure login before calling init for added services.
        try {
            doSecureLogin();
        } catch (IOException e) {
            throw new YarnRuntimeException("Failed NodeManager login", e);
        }

        registerMXBean();

        /**
         * ContainerManagerImpl 容器管理服务是一个组合服务，里面添加了如下子服务
         * 1 ResourceLocalizationService     -> 本地资源服务
         * 2 ContainersLauncher              -> 容器执行器服务
         * 3 ContainerScheduler              -> 容器调度服务
         * 4 AuxServices                     -> Aux 服务
         * 5 ContainersMonitorImpl           -> 容器监控服务
         * 6 AsyncDispatcher                 -> 容器异步分发器服务
         * 7 LogAggregationService           -> yarn 日志聚合服务
         * 8 SharedCacheUploadService        -> 本地上传文件服务
         *
         * 容器管理服务 ContainerManagerImpl 的异步分发器 AsyncDispatcher 注册了
         * 哪些 EventType 与 EventHandler 映射关系
         * 1 ContainerEventType          -> ContainerEventDispatcher
         * 2 ApplicationEventType        -> ApplicationEventDispatcher
         * 3 LocalizationEventType       -> LocalizationEventHandlerWrapper
         * 4 AuxServicesEventType        -> AuxServices
         * 5 ContainersMonitorEventType  -> ContainersMonitorImpl
         * 6 ContainersLauncherEventType -> ContainersLauncher
         * 7 ContainerSchedulerEventType -> ContainerScheduler
         * 8 LocalizerEventType          -> LocalizerTracker
         * 9 LogHandlerEventType         -> LogAggregationService
         * 10 SharedCacheUploadEventType  -> SharedCacheUploadService
         */

        /**
         * NodeManager 组合服务添加了哪些子服务 (调用其 serviceInit())
         * 1 DeletionService                  -> 删除容器服务
         * 2 NodeHealthCheckerService         -> 节点监控检查服务
         * 3 NodeResourceMonitorImpl          -> 节点资源监控服务
         * 4 ContainerManagerImpl             -> 容器管理服务
         * 5 NMLogAggregationStatusTracker    -> NM 日志聚合追踪服务
         * 6 WebServer                        -> NM Web 服务
         * 7 AsyncDispatcher                  -> NM 异步分发器服务
         * 8 JvmPauseMonitor                  -> NM JVM 监控服务
         * 9 NodeStatusUpdaterImpl            -> NM 节点状态更新服务
         *
         * NodeManager 组合服务的异步分发器 AsyncDispatcher 注册了
         * 哪些 EventType 与 EventHandler 映射关系
         * 1 ContainerManagerEventType   -> ContainerManagerImpl
         * 2 NodeManagerEventType        -> NodeManager
         */
        super.serviceInit(conf);
        // TODO add local dirs to del
    }

    @Override
    protected void serviceStop() throws Exception {
        if (isStopping.getAndSet(true)) {
            return;
        }
        try {
            super.serviceStop();
            DefaultMetricsSystem.shutdown();

            // Cleanup ResourcePluginManager
            if (null != context) {
                ResourcePluginManager rpm = context.getResourcePluginManager();
                if (rpm != null) {
                    rpm.cleanup();
                }
            }
        } finally {
            // YARN-3641: NM's services stop get failed shouldn't block the
            // release of NMLevelDBStore.
            stopRecoveryStore();
        }
    }

    public String getName() {
        return "NodeManager";
    }

    protected void shutDown(final int exitCode) {
        new Thread() {
            @Override
            public void run() {
                try {
                    NodeManager.this.stop();
                } catch (Throwable t) {
                    LOG.error("Error while shutting down NodeManager", t);
                } finally {
                    if (shouldExitOnShutdownEvent
                            && !ShutdownHookManager.get().isShutdownInProgress()) {
                        ExitUtil.terminate(exitCode);
                    }
                }
            }
        }.start();
    }

    protected void resyncWithRM() {
        // Create a thread for resync because we do not want to block dispatcher
        // thread here. Also use locking to make sure only one thread is running at
        // a time.
        if (this.resyncingWithRM.getAndSet(true)) {
            // Some other thread is already created for resyncing, do nothing
        } else {
            // We have got the lock, create a new thread
            new Thread() {
                @Override
                public void run() {
                    try {
                        if (!rmWorkPreservingRestartEnabled) {
                            LOG.info("Cleaning up running containers on resync");
                            containerManager.cleanupContainersOnNMResync();
                            // Clear all known collectors for resync.
                            if (context.getKnownCollectors() != null) {
                                context.getKnownCollectors().clear();
                            }
                        } else {
                            LOG.info("Preserving containers on resync");
                            // Re-register known timeline collectors.
                            reregisterCollectors();
                        }
                        ((NodeStatusUpdaterImpl) nodeStatusUpdater)
                                .rebootNodeStatusUpdaterAndRegisterWithRM();
                    } catch (YarnRuntimeException e) {
                        LOG.error("Error while rebooting NodeStatusUpdater.", e);
                        shutDown(NodeManagerStatus.EXCEPTION.getExitCode());
                    } finally {
                        // Release lock
                        resyncingWithRM.set(false);
                    }
                }
            }.start();
        }
    }

    /**
     * Reregisters all collectors known by this node to the RM. This method is
     * called when the RM needs to resync with the node.
     */
    protected void reregisterCollectors() {
        Map<ApplicationId, AppCollectorData> knownCollectors
                = context.getKnownCollectors();
        if (knownCollectors == null) {
            return;
        }
        ConcurrentMap<ApplicationId, AppCollectorData> registeringCollectors
                = context.getRegisteringCollectors();
        for (Map.Entry<ApplicationId, AppCollectorData> entry
                : knownCollectors.entrySet()) {
            Application app = context.getApplications().get(entry.getKey());
            if ((app != null)
                    && !ApplicationState.FINISHED.equals(app.getApplicationState())) {
                registeringCollectors.putIfAbsent(entry.getKey(), entry.getValue());
                AppCollectorData data = entry.getValue();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(entry.getKey() + " : " + data.getCollectorAddr() + "@<"
                            + data.getRMIdentifier() + ", " + data.getVersion() + ">");
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Remove collector data for done app " + entry.getKey());
                }
            }
        }
        knownCollectors.clear();
    }

    public static class NMContext implements Context {

        private NodeId nodeId = null;

        private Configuration conf = null;

        private NodeManagerMetrics metrics = null;

        protected final ConcurrentMap<ApplicationId, Application> applications =
                new ConcurrentHashMap<ApplicationId, Application>();

        private volatile Map<ApplicationId, Credentials> systemCredentials =
                new HashMap<ApplicationId, Credentials>();

        protected final ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentSkipListMap<ContainerId, Container>();

        private ConcurrentMap<ApplicationId, AppCollectorData>
                registeringCollectors;

        private ConcurrentMap<ApplicationId, AppCollectorData> knownCollectors;

        protected final ConcurrentMap<ContainerId,
                org.apache.hadoop.yarn.api.records.Container> increasedContainers =
                new ConcurrentHashMap<>();

        private final NMContainerTokenSecretManager containerTokenSecretManager;
        private final NMTokenSecretManagerInNM nmTokenSecretManager;
        private ContainerManager containerManager;
        private NodeResourceMonitor nodeResourceMonitor;
        private final LocalDirsHandlerService dirsHandler;
        private final ApplicationACLsManager aclsManager;
        private WebServer webServer;
        private final NodeHealthStatus nodeHealthStatus = RecordFactoryProvider
                .getRecordFactory(null).newRecordInstance(NodeHealthStatus.class);
        private final NMStateStoreService stateStore;
        private boolean isDecommissioned = false;
        private final ConcurrentLinkedQueue<LogAggregationReport>
                logAggregationReportForApps;
        private NodeStatusUpdater nodeStatusUpdater;
        private final boolean isDistSchedulingEnabled;
        private DeletionService deletionService;

        private OpportunisticContainerAllocator containerAllocator;

        private ContainerExecutor executor;

        private NMTimelinePublisher nmTimelinePublisher;

        private ContainerStateTransitionListener containerStateTransitionListener;

        private ResourcePluginManager resourcePluginManager;

        private NMLogAggregationStatusTracker nmLogAggregationStatusTracker;

        public NMContext(NMContainerTokenSecretManager containerTokenSecretManager,
                         NMTokenSecretManagerInNM nmTokenSecretManager,
                         LocalDirsHandlerService dirsHandler, ApplicationACLsManager aclsManager,
                         NMStateStoreService stateStore, boolean isDistSchedulingEnabled,
                         Configuration conf) {
            if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
                this.registeringCollectors = new ConcurrentHashMap<>();
                this.knownCollectors = new ConcurrentHashMap<>();
            }
            this.containerTokenSecretManager = containerTokenSecretManager;
            this.nmTokenSecretManager = nmTokenSecretManager;
            this.dirsHandler = dirsHandler;
            this.aclsManager = aclsManager;
            this.nodeHealthStatus.setIsNodeHealthy(true);
            this.nodeHealthStatus.setHealthReport("Healthy");
            this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
            this.stateStore = stateStore;
            this.logAggregationReportForApps = new ConcurrentLinkedQueue<
                    LogAggregationReport>();
            this.isDistSchedulingEnabled = isDistSchedulingEnabled;
            this.conf = conf;
        }

        /**
         * Usable only after ContainerManager is started.
         */
        @Override
        public NodeId getNodeId() {
            return this.nodeId;
        }

        @Override
        public int getHttpPort() {
            return this.webServer.getPort();
        }

        @Override
        public ConcurrentMap<ApplicationId, Application> getApplications() {
            return this.applications;
        }

        @Override
        public Configuration getConf() {
            return this.conf;
        }

        @Override
        public ConcurrentMap<ContainerId, Container> getContainers() {
            return this.containers;
        }

        @Override
        public ConcurrentMap<ContainerId, org.apache.hadoop.yarn.api.records.Container>
        getIncreasedContainers() {
            return this.increasedContainers;
        }

        @Override
        public NMContainerTokenSecretManager getContainerTokenSecretManager() {
            return this.containerTokenSecretManager;
        }

        @Override
        public NMTokenSecretManagerInNM getNMTokenSecretManager() {
            return this.nmTokenSecretManager;
        }

        @Override
        public NodeHealthStatus getNodeHealthStatus() {
            return this.nodeHealthStatus;
        }

        @Override
        public NodeResourceMonitor getNodeResourceMonitor() {
            return this.nodeResourceMonitor;
        }

        public void setNodeResourceMonitor(NodeResourceMonitor nodeResourceMonitor) {
            this.nodeResourceMonitor = nodeResourceMonitor;
        }

        @Override
        public ContainerManager getContainerManager() {
            return this.containerManager;
        }

        public void setContainerManager(ContainerManager containerManager) {
            this.containerManager = containerManager;
        }

        public void setWebServer(WebServer webServer) {
            this.webServer = webServer;
        }

        public void setNodeId(NodeId nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public LocalDirsHandlerService getLocalDirsHandler() {
            return dirsHandler;
        }

        @Override
        public ApplicationACLsManager getApplicationACLsManager() {
            return aclsManager;
        }

        @Override
        public NMStateStoreService getNMStateStore() {
            return stateStore;
        }

        @Override
        public boolean getDecommissioned() {
            return isDecommissioned;
        }

        @Override
        public void setDecommissioned(boolean isDecommissioned) {
            this.isDecommissioned = isDecommissioned;
        }

        @Override
        public Map<ApplicationId, Credentials> getSystemCredentialsForApps() {
            return systemCredentials;
        }

        public void setSystemCrendentialsForApps(
                Map<ApplicationId, Credentials> systemCredentials) {
            this.systemCredentials = systemCredentials;
        }

        @Override
        public ConcurrentLinkedQueue<LogAggregationReport>
        getLogAggregationStatusForApps() {
            return this.logAggregationReportForApps;
        }

        public NodeStatusUpdater getNodeStatusUpdater() {
            return this.nodeStatusUpdater;
        }

        public void setNodeStatusUpdater(NodeStatusUpdater nodeStatusUpdater) {
            this.nodeStatusUpdater = nodeStatusUpdater;
        }

        public boolean isDistributedSchedulingEnabled() {
            return isDistSchedulingEnabled;
        }

        public void setQueueableContainerAllocator(
                OpportunisticContainerAllocator containerAllocator) {
            this.containerAllocator = containerAllocator;
        }

        @Override
        public OpportunisticContainerAllocator getContainerAllocator() {
            return containerAllocator;
        }

        @Override
        public ConcurrentMap<ApplicationId, AppCollectorData>
        getRegisteringCollectors() {
            return this.registeringCollectors;
        }

        @Override
        public ConcurrentMap<ApplicationId, AppCollectorData> getKnownCollectors() {
            return this.knownCollectors;
        }

        @Override
        public void setNMTimelinePublisher(NMTimelinePublisher nmMetricsPublisher) {
            this.nmTimelinePublisher = nmMetricsPublisher;
        }

        @Override
        public NMTimelinePublisher getNMTimelinePublisher() {
            return nmTimelinePublisher;
        }

        public ContainerExecutor getContainerExecutor() {
            return this.executor;
        }

        public void setContainerExecutor(ContainerExecutor executor) {
            this.executor = executor;
        }

        @Override
        public ContainerStateTransitionListener
        getContainerStateTransitionListener() {
            return this.containerStateTransitionListener;
        }

        public void setContainerStateTransitionListener(
                ContainerStateTransitionListener transitionListener) {
            this.containerStateTransitionListener = transitionListener;
        }

        public ResourcePluginManager getResourcePluginManager() {
            return resourcePluginManager;
        }

        /**
         * Returns the {@link NodeManagerMetrics} instance of this node.
         * This might return a null if the instance was not set to the context.
         *
         * @return node manager metrics.
         */
        @Override
        public NodeManagerMetrics getNodeManagerMetrics() {
            return metrics;
        }

        public void setNodeManagerMetrics(NodeManagerMetrics nmMetrics) {
            this.metrics = nmMetrics;
        }

        public void setResourcePluginManager(
                ResourcePluginManager resourcePluginManager) {
            this.resourcePluginManager = resourcePluginManager;
        }

        /**
         * Return the NM's {@link DeletionService}.
         *
         * @return the NM's {@link DeletionService}.
         */
        public DeletionService getDeletionService() {
            return this.deletionService;
        }

        /**
         * Set the NM's {@link DeletionService}.
         *
         * @param deletionService the {@link DeletionService} to add to the Context.
         */
        public void setDeletionService(DeletionService deletionService) {
            this.deletionService = deletionService;
        }

        public void setNMLogAggregationStatusTracker(
                NMLogAggregationStatusTracker nmLogAggregationStatusTracker) {
            this.nmLogAggregationStatusTracker = nmLogAggregationStatusTracker;
        }

        @Override
        public NMLogAggregationStatusTracker getNMLogAggregationStatusTracker() {
            return nmLogAggregationStatusTracker;
        }
    }

    /**
     * @return the node health checker
     */
    public NodeHealthCheckerService getNodeHealthChecker() {
        return nodeHealthChecker;
    }

    private void initAndStartNodeManager(Configuration conf, boolean hasToReboot) {
        try {
            // Failed to start if we're a Unix based system but we don't have bash.
            // Bash is necessary to launch containers under Unix-based systems.
            if (!Shell.WINDOWS) {
                if (!Shell.checkIsBashSupported()) {
                    String message =
                            "Failing NodeManager start since we're on a "
                                    + "Unix-based system but bash doesn't seem to be available.";
                    LOG.error(message);
                    throw new YarnRuntimeException(message);
                }
            }

            // Remove the old hook if we are rebooting.
            if (hasToReboot && null != nodeManagerShutdownHook) {
                ShutdownHookManager.get().removeShutdownHook(nodeManagerShutdownHook);
            }

            // 注册钩子程序
            nodeManagerShutdownHook = new CompositeServiceShutdownHook(this);
            ShutdownHookManager.get().addShutdownHook(nodeManagerShutdownHook,
                    SHUTDOWN_HOOK_PRIORITY);

            // System exit should be called only when NodeManager is instantiated from
            // main() funtion
            this.shouldExitOnShutdownEvent = true;

            // 初始化 NodeManager (直接调用 serviceInit() )
            this.init(conf);

            // 启动 NodeManager
            this.start();
        } catch (Throwable t) {
            LOG.error("Error starting NodeManager", t);
            System.exit(-1);
        }
    }

    @Override
    public void handle(NodeManagerEvent event) {
        switch (event.getType()) {
            case SHUTDOWN:
                shutDown(NodeManagerStatus.NO_ERROR.getExitCode());
                break;
            case RESYNC:
                resyncWithRM();
                break;
            default:
                LOG.warn("Invalid shutdown event " + event.getType() + ". Ignoring.");
        }
    }

    /**
     * Register NodeManagerMXBean.
     */
    private void registerMXBean() {
        MBeans.register("NodeManager", "NodeManager", this);
    }

    @Override
    public boolean isSecurityEnabled() {
        return UserGroupInformation.isSecurityEnabled();
    }

    // For testing
    NodeManager createNewNodeManager() {
        return new NodeManager();
    }

    // For testing
    ContainerManagerImpl getContainerManager() {
        return containerManager;
    }

    /**
     * Unit test friendly.
     */
    protected AsyncDispatcher createNMDispatcher() {
        // 创建 AsyncDispatcher
        return new AsyncDispatcher("NM Event dispatcher");
    }

    //For testing
    Dispatcher getNMDispatcher() {
        return dispatcher;
    }

    @VisibleForTesting
    public Context getNMContext() {
        return this.context;
    }

    /**
     * Returns the NM collector service. It should be used only for testing
     * purposes.
     *
     * @return the NM collector service, or null if the timeline service v.2 is
     * not enabled
     */
    @VisibleForTesting
    NMCollectorService getNMCollectorService() {
        return this.nmCollectorService;
    }

    public static void main(String[] args) throws IOException {
            Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
            StringUtils.startupShutdownMessage(NodeManager.class, args, LOG);

            @SuppressWarnings("resource")
            // 创建 NodeManager 组合服务
            NodeManager nodeManager = new NodeManager();
            // 创建 YarnConfiguration
            Configuration conf = new YarnConfiguration();
            // 解析入参并传入 YarnConfiguration
            new GenericOptionsParser(conf, args);
            // 初始化并启动 NodeManager
            nodeManager.initAndStartNodeManager(conf, false);
    }

    @VisibleForTesting
    @Private
    public NodeStatusUpdater getNodeStatusUpdater() {
        return nodeStatusUpdater;
    }

    private NMLogAggregationStatusTracker createNMLogAggregationStatusTracker(
            Context ctxt) {
        // 创建 NMLogAggregationStatusTracker
        return new NMLogAggregationStatusTracker(ctxt);
    }
}
