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

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.federation.FederationStateStoreService;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.NoOpSystemMetricPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV1Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV2Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.CombinedSystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.AbstractReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.MemoryPlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.service.SystemServiceManager;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources belong to us..."
 */
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService
        implements Recoverable, ResourceManagerMXBean {

    /**
     * Priority of the ResourceManager shutdown hook.
     */
    public static final int SHUTDOWN_HOOK_PRIORITY = 30;

    /**
     * Used for generation of various ids.
     */
    public static final int EPOCH_BIT_SHIFT = 40;

    private static final Log LOG = LogFactory.getLog(ResourceManager.class);
    private static long clusterTimeStamp = System.currentTimeMillis();

    /*
     * UI2 webapp name
     */
    public static final String UI2_WEBAPP_NAME = "/ui2";

    /**
     * "Always On" services. Services that need to run always irrespective of
     * the HA state of the RM.
     */
    @VisibleForTesting
    protected RMContextImpl rmContext;
    private Dispatcher rmDispatcher;
    @VisibleForTesting
    protected AdminService adminService;

    /**
     * "Active" services. Services that need to run only on the Active RM.
     * These services are managed (initialized, started, stopped) by the
     * {@link CompositeService} RMActiveServices.
     * <p>
     * RM is active when (1) HA is disabled, or (2) HA is enabled and the RM is
     * in Active state.
     */
    protected RMActiveServices activeServices;
    protected RMSecretManagerService rmSecretManagerService;

    protected ResourceScheduler scheduler;
    protected ReservationSystem reservationSystem;
    private ClientRMService clientRM;
    protected ApplicationMasterService masterService;
    protected NMLivelinessMonitor nmLivelinessMonitor;
    protected NodesListManager nodesListManager;
    protected RMAppManager rmAppManager;
    protected ApplicationACLsManager applicationACLsManager;
    protected QueueACLsManager queueACLsManager;
    private FederationStateStoreService federationStateStoreService;
    private WebApp webApp;
    private AppReportFetcher fetcher = null;
    protected ResourceTrackerService resourceTracker;
    private JvmMetrics jvmMetrics;
    private boolean curatorEnabled = false;
    private ZKCuratorManager zkManager;
    private final String zkRootNodePassword =
            Long.toString(new SecureRandom().nextLong());
    private boolean recoveryEnabled;

    @VisibleForTesting
    protected String webAppAddress;
    private ConfigurationProvider configurationProvider = null;
    /**
     * End of Active services
     */

    private Configuration conf;

    private UserGroupInformation rmLoginUGI;

    public ResourceManager() {
        // 往下追
        super("ResourceManager");
    }

    public RMContext getRMContext() {
        return this.rmContext;
    }

    public static long getClusterTimeStamp() {
        return clusterTimeStamp;
    }

    public String getRMLoginUser() {
        return rmLoginUGI.getShortUserName();
    }

    @VisibleForTesting
    protected static void setClusterTimeStamp(long timestamp) {
        clusterTimeStamp = timestamp;
    }

    @VisibleForTesting
    Dispatcher getRmDispatcher() {
        return rmDispatcher;
    }

    @VisibleForTesting
    protected ResourceProfilesManager createResourceProfileManager() {
        // 创建 ResourceProfilesManagerImpl
        return new ResourceProfilesManagerImpl();
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.conf = conf;
        UserGroupInformation.setConfiguration(conf);
        // 创建 ResourceManager 上下文对象 RMContextImpl
        this.rmContext = new RMContextImpl();
        rmContext.setResourceManager(this);

        // 默认返回 LocalConfigurationProvider (底层会加载 HDFS YARN 默认配置文件)
        this.configurationProvider =
                ConfigurationProviderFactory.getConfigurationProvider(conf);
        this.configurationProvider.init(this.conf);
        rmContext.setConfigurationProvider(configurationProvider);

        // load core-site.xml
        // 加载 core-site.xml
        loadConfigurationXml(YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);

        // Do refreshSuperUserGroupsConfiguration with loaded core-site.xml
        // Or use RM specific configurations to overwrite the common ones first
        // if they exist
        RMServerUtils.processRMProxyUsersConf(conf);
        ProxyUsers.refreshSuperUserGroupsConfiguration(this.conf);

        // load yarn-site.xml
        // 加载 yarn-site.xml
        loadConfigurationXml(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

        // 校验配置是否合法
        validateConfigs(this.conf);

        // Set HA configuration should be done before login
        // 判断 ResourceManager 是否启动 HA 模式
        this.rmContext.setHAEnabled(HAUtil.isHAEnabled(this.conf));
        if (this.rmContext.isHAEnabled()) {
            HAUtil.verifyAndSetConfiguration(this.conf);
        }

        // Set UGI and do login
        // If security is enabled, use login user
        // If security is not enabled, use current user
        this.rmLoginUGI = UserGroupInformation.getCurrentUser();
        try {
            doSecureLogin();
        } catch (IOException ie) {
            throw new YarnRuntimeException("Failed to login", ie);
        }

        // register the handlers for all AlwaysOn services using setupDispatcher().
        // 创建 AsyncDispatcher
        /**
         * 0 RMFatalEventType -> RMFatalEventDispatcher
         */
        rmDispatcher = setupDispatcher();

        // ResourceManager(本身是一个服务) 是一个组合服务 故将 AsyncDispatcher 服务添加组合服务
        addIfService(rmDispatcher);

        // ResourceManager 上下文对象引用 AsyncDispatcher
        rmContext.setDispatcher(rmDispatcher);

        // The order of services below should not be changed as services will be
        // started in same order
        // As elector service needs admin service to be initialized and started,
        // first we add admin service then elector service

        // 创建并添加 ResourceManager 管理者服务 AdminService
        adminService = createAdminService();
        addService(adminService);
        rmContext.setRMAdminService(adminService);

        // elector must be added post adminservice
        if (this.rmContext.isHAEnabled()) {
            // If the RM is configured to use an embedded leader elector,
            // initialize the leader elector.
            if (HAUtil.isAutomaticFailoverEnabled(conf)
                    && HAUtil.isAutomaticFailoverEmbedded(conf)) {
                // 如果 ResourceManager 启动 HA 模式
                // 默认返回 ActiveStandbyElectorBasedElectorService
                // 这种 HA 模式选举跟 HDFS 的 NameNode (ZKFC 维护 HA 选举对象) 类似, 故不在介绍
                EmbeddedElector elector = createEmbeddedElector();
                // 类似 AsyncDispatcher 服务添加到 ResourceManager 组合服务
                addIfService(elector);
                rmContext.setLeaderElectorService(elector);
            }
        }

        rmContext.setYarnConfiguration(conf);

        // 创建并初始化 ResourceManager 是 Active 角色情况下对应的服务 RMActiveServices (是一个组合服务)
        // RMActiveServices 初始化了很多组件 直接调用 RMActiveServices.serviceInit()
        // RMActiveServices 属于 ResourceManager 内部类
        createAndInitActiveServices(false);
        /** 来自于 RMActiveServices.serviceInit()
         * 哪些事件处理器往 ResourceManager 的 AsyncDispatcher 注册事件类型
         * 1 NodesListManagerEventType -> NodesListManager
         * 2 SchedulerEventType        -> EventDispatcher
         * 3 RMAppEventType            -> ApplicationEventDispatcher
         * 4 RMAppAttemptEventType     -> ApplicationAttemptEventDispatcher
         * 5 RMNodeEventType           -> NodeEventDispatcher
         * 6 RMAppManagerEventType     -> RMAppManager
         * 7 AMLauncherEventType       -> ApplicationMasterLauncher
         */

        /** 来自于 RMActiveServices.serviceInit() 并执行如下服务的 serviceInit()
         * 1 RMSecretManagerService            (安全服务)
         * 2 ContainerAllocationExpirer        (容器过期服务)
         * 3 AMLivelinessMonitor               (ApplicationMaster 存活监控服务)
         * 4 AMLivelinessMonitor               (ApplicationMaster 已完成监控服务)
         * 5 RMAppLifetimeMonitor              (ApplicationMaster 生命周期监控服务)
         * 6 RMNodeLabelsManager               (ResourceManager 节点标签管理服务)
         * 7 MemoryPlacementConstraintManager  (基于内存的安置约束管理器服务)
         * 8 NodesListManager                  (NodeManager 管理器服务)
         * 9 CapacityScheduler                 (资源调度器服务)
         * 10 EventDispatcher                  (资源调度事件分发器)
         * 11 NMLivelinessMonitor              (NodeManager 存活监控服务)
         * 12 ResourceTrackerService           (NodeManager 注册上报资源处理服务)
         * 13 ApplicationMasterService         (管理 ApplicationMaster 服务)
         * 14 ClientRMService                  (客户端[可以提交任务、停止任务等等]发送 RPC 请求处理服务)
         * 15 ApplicationMasterLauncher        (负责启动 ApplicationMaster 服务)
         */

        // ResourceManager WEB UI 默认 0.0.0.0:8088
        webAppAddress = WebAppUtils.getWebAppBindURL(this.conf,
                YarnConfiguration.RM_BIND_HOST,
                WebAppUtils.getRMWebAppURLWithoutScheme(this.conf));

        // 创建并添加 ResourceManager 历史服务器 RMApplicationHistoryWriter
        RMApplicationHistoryWriter rmApplicationHistoryWriter =
                createRMApplicationHistoryWriter();
        addService(rmApplicationHistoryWriter);
        rmContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

        // initialize the RM timeline collector first so that the system metrics
        // publisher can bind to it
        if (YarnConfiguration.timelineServiceV2Enabled(this.conf)) {
            RMTimelineCollectorManager timelineCollectorManager =
                    createRMTimelineCollectorManager();
            addService(timelineCollectorManager);
            rmContext.setRMTimelineCollectorManager(timelineCollectorManager);
        }

        // 系统监控发布服务
        SystemMetricsPublisher systemMetricsPublisher =
                createSystemMetricsPublisher();
        addIfService(systemMetricsPublisher);
        rmContext.setSystemMetricsPublisher(systemMetricsPublisher);

        registerMXBean();

        /**
         * 1 调用 AsyncDispatcher.serviceInit()
         * 2 调用 AdminService.serviceInit()
         * 3 调用 ActiveStandbyElectorBasedElectorService.serviceInit()
         * 4 调用 RMApplicationHistoryWriter.serviceInit()
         * 5 调用 CombinedSystemMetricsPublisher.serviceInit()
         */
        super.serviceInit(this.conf);
    }

    private void loadConfigurationXml(String configurationFile)
            throws YarnException, IOException {
        InputStream configurationInputStream =
                this.configurationProvider.getConfigurationInputStream(this.conf,
                        configurationFile);
        if (configurationInputStream != null) {
            this.conf.addResource(configurationInputStream, configurationFile);
        }
    }

    protected EmbeddedElector createEmbeddedElector() throws IOException {
        EmbeddedElector elector;
        // 默认为 false
        curatorEnabled =
                conf.getBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR,
                        YarnConfiguration.DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED);
        if (curatorEnabled) {
            this.zkManager = createAndStartZKManager(conf);
            elector = new CuratorBasedElectorService(this);
        } else {
            // 当 ResourceManager 启动 HA 模式
            // 默认创建 ActiveStandbyElectorBasedElectorService 服务来选举
            elector = new ActiveStandbyElectorBasedElectorService(this);
        }
        return elector;
    }

    /**
     * Get ZooKeeper Curator manager, creating and starting if not exists.
     *
     * @param config Configuration for the ZooKeeper curator.
     * @return ZooKeeper Curator manager.
     * @throws IOException If it cannot create the manager.
     */
    public ZKCuratorManager createAndStartZKManager(Configuration
                                                            config) throws IOException {
        ZKCuratorManager manager = new ZKCuratorManager(config);

        // Get authentication
        List<AuthInfo> authInfos = new ArrayList<>();
        if (HAUtil.isHAEnabled(config) && HAUtil.getConfValueForRMInstance(
                YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, config) == null) {
            String zkRootNodeUsername = HAUtil.getConfValueForRMInstance(
                    YarnConfiguration.RM_ADDRESS,
                    YarnConfiguration.DEFAULT_RM_ADDRESS, config);
            String defaultFencingAuth =
                    zkRootNodeUsername + ":" + zkRootNodePassword;
            byte[] defaultFencingAuthData =
                    defaultFencingAuth.getBytes(Charset.forName("UTF-8"));
            String scheme = new DigestAuthenticationProvider().getScheme();
            AuthInfo authInfo = new AuthInfo(scheme, defaultFencingAuthData);
            authInfos.add(authInfo);
        }

        manager.start(authInfos);
        return manager;
    }

    public ZKCuratorManager getZKManager() {
        return zkManager;
    }

    public CuratorFramework getCurator() {
        if (this.zkManager == null) {
            return null;
        }
        return this.zkManager.getCurator();
    }

    public String getZkRootNodePassword() {
        return this.zkRootNodePassword;
    }


    protected QueueACLsManager createQueueACLsManager(ResourceScheduler scheduler,
                                                      Configuration conf) {
        // 创建 QueueACLsManager
        return new QueueACLsManager(scheduler, conf);
    }

    @VisibleForTesting
    protected void setRMStateStore(RMStateStore rmStore) {
        rmStore.setRMDispatcher(rmDispatcher);
        rmStore.setResourceManager(this);
        rmContext.setStateStore(rmStore);
    }

    protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
        // 创建 EventDispatcher
        return new EventDispatcher(this.scheduler, "SchedulerEventDispatcher");
    }

    protected Dispatcher createDispatcher() {
        // 创建 AsyncDispatcher
        return new AsyncDispatcher("RM Event dispatcher");
    }

    protected ResourceScheduler createScheduler() {
        // 默认调度器为容量调度器
        // org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler
        String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER,
                YarnConfiguration.DEFAULT_RM_SCHEDULER);
        LOG.info("Using Scheduler: " + schedulerClassName);
        try {
            Class<?> schedulerClazz = Class.forName(schedulerClassName);
            if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
                // 1 反射创建 CapacityScheduler 并调用其无参构造函数
                // 2 调用其 setConf() 设置 Configuration
                return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz,
                        this.conf);
            } else {
                throw new YarnRuntimeException("Class: " + schedulerClassName
                        + " not instance of " + ResourceScheduler.class.getCanonicalName());
            }
        } catch (ClassNotFoundException e) {
            throw new YarnRuntimeException("Could not instantiate Scheduler: "
                    + schedulerClassName, e);
        }
    }

    protected ReservationSystem createReservationSystem() {
        String reservationClassName =
                conf.get(YarnConfiguration.RM_RESERVATION_SYSTEM_CLASS,
                        AbstractReservationSystem.getDefaultReservationSystem(scheduler));
        if (reservationClassName == null) {
            return null;
        }
        LOG.info("Using ReservationSystem: " + reservationClassName);
        try {
            Class<?> reservationClazz = Class.forName(reservationClassName);
            if (ReservationSystem.class.isAssignableFrom(reservationClazz)) {
                return (ReservationSystem) ReflectionUtils.newInstance(
                        reservationClazz, this.conf);
            } else {
                throw new YarnRuntimeException("Class: " + reservationClassName
                        + " not instance of " + ReservationSystem.class.getCanonicalName());
            }
        } catch (ClassNotFoundException e) {
            throw new YarnRuntimeException(
                    "Could not instantiate ReservationSystem: " + reservationClassName, e);
        }
    }

    protected SystemServiceManager createServiceManager() {
        String schedulerClassName =
                YarnConfiguration.DEFAULT_YARN_API_SYSTEM_SERVICES_CLASS;
        LOG.info("Using SystemServiceManager: " + schedulerClassName);
        try {
            Class<?> schedulerClazz = Class.forName(schedulerClassName);
            if (SystemServiceManager.class.isAssignableFrom(schedulerClazz)) {
                return (SystemServiceManager) ReflectionUtils
                        .newInstance(schedulerClazz, this.conf);
            } else {
                throw new YarnRuntimeException(
                        "Class: " + schedulerClassName + " not instance of "
                                + SystemServiceManager.class.getCanonicalName());
            }
        } catch (ClassNotFoundException e) {
            throw new YarnRuntimeException(
                    "Could not instantiate SystemServiceManager: " + schedulerClassName,
                    e);
        }
    }

    protected ApplicationMasterLauncher createAMLauncher() {
        // 创建 ApplicationMasterLauncher
        return new ApplicationMasterLauncher(this.rmContext);
    }

    private NMLivelinessMonitor createNMLivelinessMonitor() {
        // 创建 NMLivelinessMonitor
        return new NMLivelinessMonitor(this.rmContext
                .getDispatcher());
    }

    protected AMLivelinessMonitor createAMLivelinessMonitor() {
        // 创建 AMLivelinessMonitor
        return new AMLivelinessMonitor(this.rmDispatcher);
    }

    protected RMNodeLabelsManager createNodeLabelManager()
            throws InstantiationException, IllegalAccessException {
        // 创建 RMNodeLabelsManager
        return new RMNodeLabelsManager();
    }

    protected AllocationTagsManager createAllocationTagsManager() {
        // 创建 AllocationTagsManager
        return new AllocationTagsManager(this.rmContext);
    }

    protected PlacementConstraintManagerService
    createPlacementConstraintManager() {
        // Use the in memory Placement Constraint Manager.
        // 创建 MemoryPlacementConstraintManager
        return new MemoryPlacementConstraintManager();
    }

    protected DelegationTokenRenewer createDelegationTokenRenewer() {
        return new DelegationTokenRenewer();
    }

    protected RMAppManager createRMAppManager() {
        // 创建 RMAppManager
        return new RMAppManager(this.rmContext, this.scheduler, this.masterService,
                this.applicationACLsManager, this.conf);
    }

    protected RMApplicationHistoryWriter createRMApplicationHistoryWriter() {
        // 创建RMApplicationHistoryWriter
        return new RMApplicationHistoryWriter();
    }

    private RMTimelineCollectorManager createRMTimelineCollectorManager() {
        return new RMTimelineCollectorManager(this);
    }

    private FederationStateStoreService createFederationStateStoreService() {
        return new FederationStateStoreService(rmContext);
    }

    protected SystemMetricsPublisher createSystemMetricsPublisher() {
        List<SystemMetricsPublisher> publishers =
                new ArrayList<SystemMetricsPublisher>();
        if (YarnConfiguration.timelineServiceV1Enabled(conf)) {
            SystemMetricsPublisher publisherV1 = new TimelineServiceV1Publisher();
            publishers.add(publisherV1);
        }
        if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
            // we're dealing with the v.2.x publisher
            LOG.info("system metrics publisher with the timeline service V2 is "
                    + "configured");
            SystemMetricsPublisher publisherV2 = new TimelineServiceV2Publisher(
                    rmContext.getRMTimelineCollectorManager());
            publishers.add(publisherV2);
        }
        if (publishers.isEmpty()) {
            LOG.info("TimelineServicePublisher is not configured");
            SystemMetricsPublisher noopPublisher = new NoOpSystemMetricPublisher();
            publishers.add(noopPublisher);
        }

        for (SystemMetricsPublisher publisher : publishers) {
            addIfService(publisher);
        }

        // 创建 CombinedSystemMetricsPublisher
        SystemMetricsPublisher combinedPublisher =
                new CombinedSystemMetricsPublisher(publishers);
        return combinedPublisher;
    }

    // sanity check for configurations
    protected static void validateConfigs(Configuration conf) {
        // validate max-attempts
        // 2
        int globalMaxAppAttempts =
                conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
                        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
        if (globalMaxAppAttempts <= 0) {
            throw new YarnRuntimeException("Invalid global max attempts configuration"
                    + ", " + YarnConfiguration.RM_AM_MAX_ATTEMPTS
                    + "=" + globalMaxAppAttempts + ", it should be a positive integer.");
        }

        // validate expireIntvl >= heartbeatIntvl
        // 600 * 1000L
        long expireIntvl = conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
        // 1000L
        long heartbeatIntvl =
                conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
                        YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
        if (expireIntvl < heartbeatIntvl) {
            throw new YarnRuntimeException("Nodemanager expiry interval should be no"
                    + " less than heartbeat interval, "
                    + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "=" + expireIntvl
                    + ", " + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS + "="
                    + heartbeatIntvl);
        }
    }

    /**
     * RMActiveServices handles all the Active services in the RM.
     */
    @Private
    public class RMActiveServices extends CompositeService {

        private DelegationTokenRenewer delegationTokenRenewer;
        private EventHandler<SchedulerEvent> schedulerDispatcher;
        private ApplicationMasterLauncher applicationMasterLauncher;
        private ContainerAllocationExpirer containerAllocationExpirer;
        private ResourceManager rm;
        private boolean fromActive = false;
        private StandByTransitionRunnable standByTransitionRunnable;
        private RMNMInfo rmnmInfo;

        RMActiveServices(ResourceManager rm) {
            super("RMActiveServices");
            // ResourceManager
            this.rm = rm;
        }

        @Override
        protected void serviceInit(Configuration configuration) throws Exception {
            // 创建 StandByTransitionRunnable 线程 (Runnable 接口实现类)
            standByTransitionRunnable = new StandByTransitionRunnable();

            // 创建并添加 ResourceManager 安全服务 RMSecretManagerService
            rmSecretManagerService = createRMSecretManagerService();
            addService(rmSecretManagerService);

            // 创建并添加容器过期服务 ContainerAllocationExpirer
            // 比如 ResourceManager 分配了容器给其他计算程序(MR, Spark, Flink) 但是计算程序迟迟没有
            // 使用 则 ResourceManager 需要回收
            containerAllocationExpirer = new ContainerAllocationExpirer(rmDispatcher);
            addService(containerAllocationExpirer);
            rmContext.setContainerAllocationExpirer(containerAllocationExpirer);

            // 创建并添加 ApplicationMaster (Spark -> Driver) 存活监控服务 AMLivelinessMonitor
            AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
            addService(amLivelinessMonitor);
            rmContext.setAMLivelinessMonitor(amLivelinessMonitor);

            // 同上 (只不过这个服务是监控 ApplicationMaster 已经完成) AMLivelinessMonitor
            AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
            addService(amFinishingMonitor);
            rmContext.setAMFinishingMonitor(amFinishingMonitor);

            // 创建并添加监控 ApplicationMaster 生命周期服务 RMAppLifetimeMonitor
            RMAppLifetimeMonitor rmAppLifetimeMonitor = createRMAppLifetimeMonitor();
            addService(rmAppLifetimeMonitor);
            rmContext.setRMAppLifetimeMonitor(rmAppLifetimeMonitor);

            // 创建并添加 ResourceManager 节点标签管理服务 RMNodeLabelsManager
            RMNodeLabelsManager nlm = createNodeLabelManager();
            nlm.setRMContext(rmContext);
            addService(nlm);
            rmContext.setNodeLabelManager(nlm);

            // 创建分配标签管理对象 AllocationTagsManager
            AllocationTagsManager allocationTagsManager =
                    createAllocationTagsManager();
            rmContext.setAllocationTagsManager(allocationTagsManager);

            // 创建并添加 MemoryPlacementConstraintManager 服务 (基于内存的安置约束管理器)
            PlacementConstraintManagerService placementConstraintManager =
                    createPlacementConstraintManager();
            addService(placementConstraintManager);
            rmContext.setPlacementConstraintManager(placementConstraintManager);

            // add resource profiles here because it's used by AbstractYarnScheduler
            // 创建并初始化 ResourceProfilesManagerImpl (资源配置文件管理器 通常被资源调度器使用)
            ResourceProfilesManager resourceProfilesManager =
                    createResourceProfileManager();
            // 初始化 ResourceProfilesManagerImpl (默认啥也不干)
            resourceProfilesManager.init(conf);
            rmContext.setResourceProfilesManager(resourceProfilesManager);

            // 创建 ResourceManager 节点标签周期性更新服务(默认没有开启该服务)
            RMDelegatedNodeLabelsUpdater delegatedNodeLabelsUpdater =
                    createRMDelegatedNodeLabelsUpdater();
            if (delegatedNodeLabelsUpdater != null) {
                addService(delegatedNodeLabelsUpdater);
                rmContext.setRMDelegatedNodeLabelsUpdater(delegatedNodeLabelsUpdater);
            }

            // 是否启动 ResourceManager 自动恢复 默认为 false
            // 但是一般启动 ResourceManager HA模式 该功能一般在 yarn-site.xml 文件配置为 true
            recoveryEnabled = conf.getBoolean(YarnConfiguration.RECOVERY_ENABLED,
                    YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);

            RMStateStore rmStore = null;
            if (recoveryEnabled) {
                // 默认返回 FileSystemRMStateStore (反射创建)
                rmStore = RMStateStoreFactory.getStore(conf);
                // 默认 true
                boolean isWorkPreservingRecoveryEnabled =
                        conf.getBoolean(
                                YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
                                YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED);
                rmContext
                        .setWorkPreservingRecoveryEnabled(isWorkPreservingRecoveryEnabled);
            } else {
                rmStore = new NullRMStateStore();
            }

            try {
                // 设置 ResourceManager
                rmStore.setResourceManager(rm);
                // 初始化 FileSystemRMStateStore 调用其父类 RMStateStore 的 serviceInit()
                // RMStateStore 底层创建 AsyncDispatcher 并添加一个
                // EventHandler(ResourceManager 简单状态机)
                rmStore.init(conf);
                rmStore.setRMDispatcher(rmDispatcher);
            } catch (Exception e) {
                // the Exception from stateStore.init() needs to be handled for
                // HA and we need to give up master status if we got fenced
                LOG.error("Failed to init state store", e);
                throw e;
            }
            rmContext.setStateStore(rmStore);

            // 是否启动用户组安全功能
            if (UserGroupInformation.isSecurityEnabled()) {
                delegationTokenRenewer = createDelegationTokenRenewer();
                rmContext.setDelegationTokenRenewer(delegationTokenRenewer);
            }

            // Register event handler for NodesListManager
            // 创建 NodeManager 节点管理器服务 NodesListManager
            // NodesListManager 同时也是一个 EventHandler
            nodesListManager = new NodesListManager(rmContext);
            // 往 ResourceManager 的异步事件分发器注册事件处理器 NodesListManager
            rmDispatcher.register(NodesListManagerEventType.class, nodesListManager);
            addService(nodesListManager);
            rmContext.setNodesListManager(nodesListManager);

            // Initialize the scheduler
            // 反射创建资源调度器(默认 CapacityScheduler 对象)
            scheduler = createScheduler();
            scheduler.setRMContext(rmContext);
            // 添加 CapacityScheduler 服务到 ResourceManager 组合服务
            addIfService(scheduler);
            rmContext.setScheduler(scheduler);

            // 创建并添加资源调度事件分发器 EventDispatcher 并往 AsyncDispatcher 注册一个事件处理器
            // EventDispatcher 与 AsyncDispatcher 类似
            schedulerDispatcher = createSchedulerEventDispatcher();
            // 添加服务
            addIfService(schedulerDispatcher);
            // 注册事件处理器
            rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);

            // Register event handler for RmAppEvents
            // 注册事件处理器 ApplicationEventDispatcher
            rmDispatcher.register(RMAppEventType.class,
                    // 创建 ApplicationEventDispatcher
                    new ApplicationEventDispatcher(rmContext));

            // Register event handler for RmAppAttemptEvents
            // 注册事件处理器 ApplicationAttemptEventDispatcher
            rmDispatcher.register(RMAppAttemptEventType.class,
                    // 创建 ApplicationAttemptEventDispatcher
                    new ApplicationAttemptEventDispatcher(rmContext));

            // Register event handler for RmNodes
            // 注册事件处理器 NodeEventDispatcher
            rmDispatcher.register(RMNodeEventType.class,
                    // 创建 NodeEventDispatcher
                    new NodeEventDispatcher(rmContext));

            // 创建并添加 NodeManager 节点存活监控服务 NMLivelinessMonitor
            nmLivelinessMonitor = createNMLivelinessMonitor();
            addService(nmLivelinessMonitor);

            // 创建并添加 NodeManager 上报资源处理 ResourceTrackerService 服务
            resourceTracker = createResourceTrackerService();
            addService(resourceTracker);
            rmContext.setResourceTrackerService(resourceTracker);

            // ResourceManager 监控相关
            MetricsSystem ms = DefaultMetricsSystem.initialize("ResourceManager");
            if (fromActive) {
                JvmMetrics.reattach(ms, jvmMetrics);
                UserGroupInformation.reattachMetrics();
            } else {
                jvmMetrics = JvmMetrics.initSingleton("ResourceManager", null);
            }
            JvmPauseMonitor pauseMonitor = new JvmPauseMonitor();
            addService(pauseMonitor);
            jvmMetrics.setPauseMonitor(pauseMonitor);

            // Initialize the Reservation system
            // 是否开启 ResourceManager 保护系统功能 默认为 false
            if (conf.getBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
                    YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_ENABLE)) {
                reservationSystem = createReservationSystem();
                if (reservationSystem != null) {
                    reservationSystem.setRMContext(rmContext);
                    addIfService(reservationSystem);
                    rmContext.setReservationSystem(reservationSystem);
                    LOG.info("Initialized Reservation system");
                }
            }

            // 创建并添加管理 ApplicationMaster 服务 ApplicationMasterService
            masterService = createApplicationMasterService();
            // 默认不开启里面功能
            createAndRegisterOpportunisticDispatcher(masterService);
            // 添加 ApplicationMasterService 服务
            addService(masterService);
            rmContext.setApplicationMasterService(masterService);

            // 创建 ApplicationACLsManager (ACL Access Control List)
            applicationACLsManager = new ApplicationACLsManager(conf);
            // 创建 QueueACLsManager
            queueACLsManager = createQueueACLsManager(scheduler, conf);

            // 创建管理 Application 事件处理器 RMAppManager
            rmAppManager = createRMAppManager();
            // Register event handler for RMAppManagerEvents
            rmDispatcher.register(RMAppManagerEventType.class, rmAppManager);

            // 创建并添加 Application 客户端(可以提交任务、停止任务等等)发送 RPC 请求处理服务 ClientRMService
            // ClientRMService 服务实现 RPC 协议 ApplicationClientProtocol
            clientRM = createClientRMService();
            addService(clientRM);
            rmContext.setClientRMService(clientRM);

            // 创建 ApplicationMasterLauncher 服务 同时其也是一个事件处理器
            // 负责启动 ApplicationMaster 服务
            applicationMasterLauncher = createAMLauncher();
            // 注册事件处理器
            rmDispatcher.register(AMLauncherEventType.class,
                    applicationMasterLauncher);
            // 添加服务
            addService(applicationMasterLauncher);

            if (UserGroupInformation.isSecurityEnabled()) {
                addService(delegationTokenRenewer);
                delegationTokenRenewer.setRMContext(rmContext);
            }

            //  ResourceManager 是否开启联邦功能 默认不开启
            if (HAUtil.isFederationEnabled(conf)) {
                String cId = YarnConfiguration.getClusterId(conf);
                if (cId.isEmpty()) {
                    String errMsg =
                            "Cannot initialize RM as Federation is enabled"
                                    + " but cluster id is not configured.";
                    LOG.error(errMsg);
                    throw new YarnRuntimeException(errMsg);
                }
                federationStateStoreService = createFederationStateStoreService();
                addIfService(federationStateStoreService);
                LOG.info("Initialized Federation membership.");
            }

            // 创建 RMNMInfo (管理 NodeManager 状态列表信息)
            rmnmInfo = new RMNMInfo(rmContext, scheduler);

            if (conf.getBoolean(YarnConfiguration.YARN_API_SERVICES_ENABLE,
                    false)) {
                SystemServiceManager systemServiceManager = createServiceManager();
                addIfService(systemServiceManager);
            }

            /**
             * 哪些事件处理器往 ResourceManager 的 AsyncDispatcher 注册事件类型
             * 1 NodesListManagerEventType -> NodesListManager
             * 2 SchedulerEventType        -> EventDispatcher
             * 3 RMAppEventType            -> ApplicationEventDispatcher
             * 4 RMAppAttemptEventType     -> ApplicationAttemptEventDispatcher
             * 5 RMNodeEventType           -> NodeEventDispatcher
             * 6 RMAppManagerEventType     -> RMAppManager
             * 7 AMLauncherEventType       -> ApplicationMasterLauncher
             */

            // 初始化服务
            /**
             * 1 RMSecretManagerService            (安全服务)
             * 2 ContainerAllocationExpirer        (容器过期服务)
             * 3 AMLivelinessMonitor               (ApplicationMaster 存活监控服务)
             * 4 AMLivelinessMonitor               (ApplicationMaster 已完成监控服务)
             * 5 RMAppLifetimeMonitor              (ApplicationMaster 生命周期监控服务)
             * 6 RMNodeLabelsManager               (ResourceManager 节点标签管理服务)
             * 7 MemoryPlacementConstraintManager  (基于内存的安置约束管理器服务)
             * 8 NodesListManager                  (NodeManager 管理器服务)
             * 9 CapacityScheduler                 (资源调度器服务)
             * 10 EventDispatcher                  (资源调度事件分发器)
             * 11 NMLivelinessMonitor              (NodeManager 存活监控服务)
             * 12 ResourceTrackerService           (NodeManager 上报资源处理服务)
             * 13 ApplicationMasterService         (管理 ApplicationMaster 服务)
             * 14 ClientRMService                  (客户端[可以提交任务、停止任务等等]发送 RPC 请求处理服务)
             * 15 ApplicationMasterLauncher        (负责启动 ApplicationMaster 服务)
             */
            super.serviceInit(conf);
        }

        private void createAndRegisterOpportunisticDispatcher(
                ApplicationMasterService service) {
            if (!isOpportunisticSchedulingEnabled(conf)) {
                return;
            }
            // 创建 EventDispatcher
            EventDispatcher oppContainerAllocEventDispatcher = new EventDispatcher(
                    (OpportunisticContainerAllocatorAMService) service,
                    OpportunisticContainerAllocatorAMService.class.getName());
            // Add an event dispatcher for the
            // OpportunisticContainerAllocatorAMService to handle node
            // additions, updates and removals. Since the SchedulerEvent is currently
            // a super set of theses, we register interest for it.
            addService(oppContainerAllocEventDispatcher);
            // 注册事件处理器
            rmDispatcher
                    .register(SchedulerEventType.class, oppContainerAllocEventDispatcher);
        }

        @Override
        protected void serviceStart() throws Exception {
            RMStateStore rmStore = rmContext.getStateStore();
            // The state store needs to start irrespective of recoveryEnabled as apps
            // need events to move to further states.
            rmStore.start();

            if (recoveryEnabled) {
                try {
                    LOG.info("Recovery started");
                    rmStore.checkVersion();
                    if (rmContext.isWorkPreservingRecoveryEnabled()) {
                        rmContext.setEpoch(rmStore.getAndIncrementEpoch());
                    }
                    RMState state = rmStore.loadState();
                    recover(state);
                    LOG.info("Recovery ended");
                } catch (Exception e) {
                    // the Exception from loadState() needs to be handled for
                    // HA and we need to give up master status if we got fenced
                    LOG.error("Failed to load/recover state", e);
                    throw e;
                }
            } else {
                if (HAUtil.isFederationEnabled(conf)) {
                    long epoch = conf.getLong(YarnConfiguration.RM_EPOCH,
                            YarnConfiguration.DEFAULT_RM_EPOCH);
                    rmContext.setEpoch(epoch);
                    LOG.info("Epoch set for Federation: " + epoch);
                }
            }

            super.serviceStart();
        }

        @Override
        protected void serviceStop() throws Exception {

            super.serviceStop();

            DefaultMetricsSystem.shutdown();
            // unregister rmnmInfo bean
            if (rmnmInfo != null) {
                rmnmInfo.unregister();
            }
            if (rmContext != null) {
                RMStateStore store = rmContext.getStateStore();
                try {
                    if (null != store) {
                        store.close();
                    }
                } catch (Exception e) {
                    LOG.error("Error closing store.", e);
                }
            }

        }
    }

    @Private
    private class RMFatalEventDispatcher implements EventHandler<RMFatalEvent> {
        @Override
        public void handle(RMFatalEvent event) {
            LOG.error("Received " + event);

            if (HAUtil.isHAEnabled(getConfig())) {
                // If we're in an HA config, the right answer is always to go into
                // standby.
                LOG.warn("Transitioning the resource manager to standby.");
                handleTransitionToStandByInNewThread();
            } else {
                // If we're stand-alone, we probably want to shut down, but the if and
                // how depends on the event.
                switch (event.getType()) {
                    case STATE_STORE_FENCED:
                        LOG.fatal("State store fenced even though the resource manager " +
                                "is not configured for high availability. Shutting down this " +
                                "resource manager to protect the integrity of the state store.");
                        ExitUtil.terminate(1, event.getExplanation());
                        break;
                    case STATE_STORE_OP_FAILED:
                        if (YarnConfiguration.shouldRMFailFast(getConfig())) {
                            LOG.fatal("Shutting down the resource manager because a state " +
                                    "store operation failed, and the resource manager is " +
                                    "configured to fail fast. See the yarn.fail-fast and " +
                                    "yarn.resourcemanager.fail-fast properties.");
                            ExitUtil.terminate(1, event.getExplanation());
                        } else {
                            LOG.warn("Ignoring state store operation failure because the " +
                                    "resource manager is not configured to fail fast. See the " +
                                    "yarn.fail-fast and yarn.resourcemanager.fail-fast " +
                                    "properties.");
                        }
                        break;
                    default:
                        LOG.fatal("Shutting down the resource manager.");
                        ExitUtil.terminate(1, event.getExplanation());
                }
            }
        }
    }

    /**
     * Transition to standby state in a new thread. The transition operation is
     * asynchronous to avoid deadlock caused by cyclic dependency.
     */
    private void handleTransitionToStandByInNewThread() {
        Thread standByTransitionThread =
                new Thread(activeServices.standByTransitionRunnable);
        standByTransitionThread.setName("StandByTransitionThread");
        standByTransitionThread.start();
    }

    /**
     * The class to transition RM to standby state. The same
     * {@link StandByTransitionRunnable} object could be used in multiple threads,
     * but runs only once. That's because RM can go back to active state after
     * transition to standby state, the same runnable in the old context can't
     * transition RM to standby state again. A new runnable is created every time
     * RM transitions to active state.
     */
    private class StandByTransitionRunnable implements Runnable {
        // The atomic variable to make sure multiple threads with the same runnable
        // run only once.
        private final AtomicBoolean hasAlreadyRun = new AtomicBoolean(false);

        @Override
        public void run() {
            // Run this only once, even if multiple threads end up triggering
            // this simultaneously.
            if (hasAlreadyRun.getAndSet(true)) {
                return;
            }

            if (rmContext.isHAEnabled()) {
                try {
                    // Transition to standby and reinit active services
                    LOG.info("Transitioning RM to Standby mode");
                    transitionToStandby(true);
                    EmbeddedElector elector = rmContext.getLeaderElectorService();
                    if (elector != null) {
                        elector.rejoinElection();
                    }
                } catch (Exception e) {
                    LOG.fatal("Failed to transition RM to Standby mode.", e);
                    ExitUtil.terminate(1, e);
                }
            }
        }
    }

    @Private
    public static final class ApplicationEventDispatcher implements
            EventHandler<RMAppEvent> {

        private final RMContext rmContext;

        public ApplicationEventDispatcher(RMContext rmContext) {
            this.rmContext = rmContext;
        }

        @Override
        public void handle(RMAppEvent event) {
            // 针对启动 AM -> ResourceManager 的异步事件分发器提交 RMAppEventType.START 事件
            // event = new RMAppEvent(ApplicationId, RMAppEventType.START)

            // event = new RMAppEvent(ApplicationId, RMAppEventType.APP_NEW_SAVED)

            // event = new RMAppEvent(ApplicationId, RMAppEventType.APP_ACCEPTED)
            ApplicationId appID = event.getApplicationId();
            RMApp rmApp = this.rmContext.getRMApps().get(appID);
            if (rmApp != null) {
                try {
                    // 调用 RMAppImpl.handler()
                    rmApp.handle(event);
                } catch (Throwable t) {
                    LOG.error("Error in handling event type " + event.getType()
                            + " for application " + appID, t);
                }
            }
        }
    }

    @Private
    public static final class ApplicationAttemptEventDispatcher implements
            EventHandler<RMAppAttemptEvent> {

        private final RMContext rmContext;

        public ApplicationAttemptEventDispatcher(RMContext rmContext) {
            this.rmContext = rmContext;
        }

        @Override
        public void handle(RMAppAttemptEvent event) {
            // event = new RMAppStartAttemptEvent(AppAttemptId, false)
            // eventType = RMAppAttemptEventType.START

            // event = RMAppAttemptEvent
            // eventType= RMAppAttemptEventType.ATTEMPT_ADDED
            ApplicationAttemptId appAttemptId = event.getApplicationAttemptId();
            ApplicationId appId = appAttemptId.getApplicationId();

            RMApp rmApp = this.rmContext.getRMApps().get(appId);
            if (rmApp != null) {
                RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptId);
                if (rmAppAttempt != null) {
                    try {
                        // 调用 RMAppAttemptImpl.handler()
                        rmAppAttempt.handle(event);
                    } catch (Throwable t) {
                        LOG.error("Error in handling event type " + event.getType()
                                + " for applicationAttempt " + appAttemptId, t);
                    }
                } else if (rmApp.getApplicationSubmissionContext() != null
                        && rmApp.getApplicationSubmissionContext()
                        .getKeepContainersAcrossApplicationAttempts()
                        && event.getType() == RMAppAttemptEventType.CONTAINER_FINISHED) {
                    // For work-preserving AM restart, failed attempts are still
                    // capturing CONTAINER_FINISHED events and record the finished
                    // containers which will be used by current attempt.
                    // We just keep 'yarn.resourcemanager.am.max-attempts' in
                    // RMStateStore. If the finished container's attempt is deleted, we
                    // use the first attempt in app.attempts to deal with these events.

                    RMAppAttempt previousFailedAttempt =
                            rmApp.getAppAttempts().values().iterator().next();
                    if (previousFailedAttempt != null) {
                        try {
                            LOG.debug("Event " + event.getType() + " handled by "
                                    + previousFailedAttempt);
                            previousFailedAttempt.handle(event);
                        } catch (Throwable t) {
                            LOG.error("Error in handling event type " + event.getType()
                                    + " for applicationAttempt " + appAttemptId
                                    + " with " + previousFailedAttempt, t);
                        }
                    } else {
                        LOG.error("Event " + event.getType()
                                + " not handled, because previousFailedAttempt is null");
                    }
                }
            }
        }
    }

    @Private
    public static final class NodeEventDispatcher implements
            EventHandler<RMNodeEvent> {

        private final RMContext rmContext;

        public NodeEventDispatcher(RMContext rmContext) {
            this.rmContext = rmContext;
        }

        @Override
        public void handle(RMNodeEvent event) {
            NodeId nodeId = event.getNodeId();
            RMNode node = this.rmContext.getRMNodes().get(nodeId);
            if (node != null) {
                try {
                    // 调用
                    ((EventHandler<RMNodeEvent>) node).handle(event);
                } catch (Throwable t) {
                    LOG.error("Error in handling event type " + event.getType()
                            + " for node " + nodeId, t);
                }
            }
        }
    }

    /**
     * Return a HttpServer.Builder that the journalnode / namenode / secondary
     * namenode can use to initialize their HTTP / HTTPS server.
     *
     * @param conf      configuration object
     * @param httpAddr  HTTP address
     * @param httpsAddr HTTPS address
     * @param name      Name of the server
     * @return builder object
     * @throws IOException from Builder
     */
    public static HttpServer2.Builder httpServerTemplateForRM(Configuration conf,
                                                              final InetSocketAddress httpAddr, final InetSocketAddress httpsAddr,
                                                              String name) throws IOException {
        HttpServer2.Builder builder = new HttpServer2.Builder().setName(name)
                .setConf(conf).setSecurityEnabled(false);

        if (httpAddr.getPort() == 0) {
            builder.setFindPort(true);
        }

        URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddr));
        builder.addEndpoint(uri);
        LOG.info("Starting Web-server for " + name + " at: " + uri);

        return builder;
    }

    protected void startWepApp() {
        Map<String, String> serviceConfig = null;
        Configuration conf = getConfig();

        RMWebAppUtil.setupSecurityAndFilters(conf,
                getClientRMService().rmDTSecretManager);

        Map<String, String> params = new HashMap<String, String>();
        if (getConfig().getBoolean(YarnConfiguration.YARN_API_SERVICES_ENABLE,
                false)) {
            String apiPackages = "org.apache.hadoop.yarn.service.webapp;" +
                    "org.apache.hadoop.yarn.webapp";
            params.put("com.sun.jersey.config.property.resourceConfigClass",
                    "com.sun.jersey.api.core.PackagesResourceConfig");
            params.put("com.sun.jersey.config.property.packages", apiPackages);
        }

        Builder<ResourceManager> builder =
                WebApps
                        .$for("cluster", ResourceManager.class, this,
                                "ws")
                        .with(conf)
                        .withServlet("API-Service", "/app/*",
                                ServletContainer.class, params, false)
                        .withHttpSpnegoPrincipalKey(
                                YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
                        .withHttpSpnegoKeytabKey(
                                YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
                        .withCSRFProtection(YarnConfiguration.RM_CSRF_PREFIX)
                        .withXFSProtection(YarnConfiguration.RM_XFS_PREFIX)
                        .at(webAppAddress);
        String proxyHostAndPort = rmContext.getProxyHostAndPort(conf);
        if (WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf).
                equals(proxyHostAndPort)) {
            if (HAUtil.isHAEnabled(conf)) {
                fetcher = new AppReportFetcher(conf);
            } else {
                fetcher = new AppReportFetcher(conf, getClientRMService());
            }
            builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
                    ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
            builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
            String[] proxyParts = proxyHostAndPort.split(":");
            builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);
        }

        WebAppContext uiWebAppContext = null;
        if (getConfig().getBoolean(YarnConfiguration.YARN_WEBAPP_UI2_ENABLE,
                YarnConfiguration.DEFAULT_YARN_WEBAPP_UI2_ENABLE)) {
            String onDiskPath = getConfig()
                    .get(YarnConfiguration.YARN_WEBAPP_UI2_WARFILE_PATH);

            uiWebAppContext = new WebAppContext();
            uiWebAppContext.setContextPath(UI2_WEBAPP_NAME);

            if (null == onDiskPath) {
                String war = "hadoop-yarn-ui-" + VersionInfo.getVersion() + ".war";
                URLClassLoader cl = (URLClassLoader) ClassLoader.getSystemClassLoader();
                URL url = cl.findResource(war);

                if (null == url) {
                    onDiskPath = getWebAppsPath("ui2");
                } else {
                    onDiskPath = url.getFile();
                }
            }
            if (onDiskPath == null || onDiskPath.isEmpty()) {
                LOG.error("No war file or webapps found for ui2 !");
            } else {
                if (onDiskPath.endsWith(".war")) {
                    uiWebAppContext.setWar(onDiskPath);
                    LOG.info("Using war file at: " + onDiskPath);
                } else {
                    uiWebAppContext.setResourceBase(onDiskPath);
                    LOG.info("Using webapps at: " + onDiskPath);
                }
            }
        }

        webApp = builder.start(new RMWebApp(this), uiWebAppContext);
    }

    private String getWebAppsPath(String appName) {
        URL url = getClass().getClassLoader().getResource("webapps/" + appName);
        if (url == null) {
            return "";
        }
        return url.toString();
    }

    /**
     * Helper method to create and init {@link #activeServices}. This creates an
     * instance of {@link RMActiveServices} and initializes it.
     *
     * @param fromActive Indicates if the call is from the active state transition
     *                   or the RM initialization.
     */
    protected void createAndInitActiveServices(boolean fromActive) {
        // 创建 RMActiveServices
        activeServices = new RMActiveServices(this);
        // 默认 false
        activeServices.fromActive = fromActive;
        // 初始化 RMActiveServices 服务 (直接调用其服务的 serviceInit() )
        activeServices.init(conf);
    }

    /**
     * Helper method to start {@link #activeServices}.
     *
     * @throws Exception
     */
    void startActiveServices() throws Exception {
        if (activeServices != null) {
            clusterTimeStamp = System.currentTimeMillis();
            activeServices.start();
        }
    }

    /**
     * Helper method to stop {@link #activeServices}.
     *
     * @throws Exception
     */
    void stopActiveServices() {
        if (activeServices != null) {
            activeServices.stop();
            activeServices = null;
        }
    }

    void reinitialize(boolean initialize) {
        ClusterMetrics.destroy();
        QueueMetrics.clearQueueMetrics();
        getResourceScheduler().resetSchedulerMetrics();
        if (initialize) {
            resetRMContext();
            createAndInitActiveServices(true);
        }
    }

    @VisibleForTesting
    protected boolean areActiveServicesRunning() {
        return activeServices != null && activeServices.isInState(STATE.STARTED);
    }

    synchronized void transitionToActive() throws Exception {
        if (rmContext.getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE) {
            LOG.info("Already in active state");
            return;
        }
        LOG.info("Transitioning to active state");

        this.rmLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try {
                    startActiveServices();
                    return null;
                } catch (Exception e) {
                    reinitialize(true);
                    throw e;
                }
            }
        });

        rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
        LOG.info("Transitioned to active state");
    }

    synchronized void transitionToStandby(boolean initialize)
            throws Exception {
        if (rmContext.getHAServiceState() ==
                HAServiceProtocol.HAServiceState.STANDBY) {
            LOG.info("Already in standby state");
            return;
        }

        // ResourceManager 进入 Standby 状态
        LOG.info("Transitioning to standby state");
        HAServiceState state = rmContext.getHAServiceState();
        rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.STANDBY);

        if (state == HAServiceProtocol.HAServiceState.ACTIVE) {
            // 停止 RMActiveServices 服务 (里面初始化了很多服务)
            stopActiveServices();
            // initialize = false
            reinitialize(initialize);
        }
        LOG.info("Transitioned to standby state");
    }

    @Override
    protected void serviceStart() throws Exception {
        // 如果 ResourceManager 开启 HA 则先进入 Standby 状态
        if (this.rmContext.isHAEnabled()) {
            // ResourceManager 一启动先进入 Active 状态 故往下是关闭 RMActiveService 所有的子服务
            // 一旦 ResourceManager 选举成功 则 transitionToActive() 重新启动 RMActiveService 所有的子服务
            // 因为 ResourceManager HA 选举跟 HDFS 的 NameNode HA 选举相同
            // 故不再介绍(默认启动了 RMActiveService 服务)
            /** 只有 ResourceManager 被选举成 Active 时  也即调用 transitionToActive() ->
             * 来自于 RMActiveServices.serviceStart() 并执行如下服务的 serviceStart()
             * 1 RMSecretManagerService            (安全服务)
             * 2 ContainerAllocationExpirer        (容器过期服务)
             * 3 AMLivelinessMonitor               (ApplicationMaster 存活监控服务)
             * 4 AMLivelinessMonitor               (ApplicationMaster 已完成监控服务)
             * 5 RMAppLifetimeMonitor              (ApplicationMaster 生命周期监控服务)
             * 6 RMNodeLabelsManager               (ResourceManager 节点标签管理服务)
             * 7 MemoryPlacementConstraintManager  (基于内存的安置约束管理器服务)
             * 8 NodesListManager                  (NodeManager 管理器服务)
             * 9 CapacityScheduler                 (资源调度器服务)
             * 10 EventDispatcher                  (资源调度事件分发器)
             * 11 NMLivelinessMonitor              (NodeManager 存活监控服务)
             * 12 ResourceTrackerService           (NodeManager 上报资源处理服务)
             * 13 ApplicationMasterService         (管理 ApplicationMaster 服务)
             * 14 ClientRMService                  (客户端[可以提交任务、停止任务等等]发送 RPC 请求处理服务)
             * 15 ApplicationMasterLauncher        (负责启动 ApplicationMaster 服务)
             */
            transitionToStandby(false);
        }

        // 启动 RMWebApp 服务
        startWepApp();

        if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER,
                false)) {
            int port = webApp.port();
            WebAppUtils.setRMWebAppPort(conf, port);
        }

        /** 来自于 ResourceManager.serviceStart() 并执行如下服务的 serviceStart()
         * 1 AsyncDispatcher                   (异步中央事件分发器服务)
         * 2 AdminService                      (ResourceManager 管理者服务)
         * 3 ActiveStandbyElectorBasedElectorService (ResourceManager HA 选举服务)
         * 4 RMApplicationHistoryWriter              (ResourceManager 历史服务器)
         * 5 CombinedSystemMetricsPublisher          (监控发布服务)
         */
        super.serviceStart();

        // Non HA case, start after RM services are started.
        // 如果 ResourceManager 不是 HA 模式 直接进入 Active 状态
        if (!this.rmContext.isHAEnabled()) {
            transitionToActive();
        }
    }

    protected void doSecureLogin() throws IOException {
        InetSocketAddress socAddr = getBindAddress(conf);
        SecurityUtil.login(this.conf, YarnConfiguration.RM_KEYTAB,
                YarnConfiguration.RM_PRINCIPAL, socAddr.getHostName());

        // if security is enable, set rmLoginUGI as UGI of loginUser
        if (UserGroupInformation.isSecurityEnabled()) {
            this.rmLoginUGI = UserGroupInformation.getLoginUser();
        }
    }

    @Override
    protected void serviceStop() throws Exception {
        if (webApp != null) {
            webApp.stop();
        }
        if (fetcher != null) {
            fetcher.stop();
        }
        if (configurationProvider != null) {
            configurationProvider.close();
        }
        super.serviceStop();
        if (zkManager != null) {
            zkManager.close();
        }
        transitionToStandby(false);
        rmContext.setHAServiceState(HAServiceState.STOPPING);
    }

    protected ResourceTrackerService createResourceTrackerService() {
        // 创建 ResourceTrackerService
        return new ResourceTrackerService(this.rmContext, this.nodesListManager,
                this.nmLivelinessMonitor,
                this.rmContext.getContainerTokenSecretManager(),
                this.rmContext.getNMTokenSecretManager());
    }

    protected ClientRMService createClientRMService() {
        // 创建 ClientRMService
        return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
                this.applicationACLsManager, this.queueACLsManager,
                this.rmContext.getRMDelegationTokenSecretManager());
    }

    protected ApplicationMasterService createApplicationMasterService() {
        Configuration config = this.rmContext.getYarnConfiguration();
        if (isOpportunisticSchedulingEnabled(conf)) {
            if (YarnConfiguration.isDistSchedulingEnabled(config) &&
                    !YarnConfiguration
                            .isOpportunisticContainerAllocationEnabled(config)) {
                throw new YarnRuntimeException(
                        "Invalid parameters: opportunistic container allocation has to " +
                                "be enabled when distributed scheduling is enabled.");
            }
            OpportunisticContainerAllocatorAMService
                    oppContainerAllocatingAMService =
                    new OpportunisticContainerAllocatorAMService(this.rmContext,
                            scheduler);
            this.rmContext.setContainerQueueLimitCalculator(
                    oppContainerAllocatingAMService.getNodeManagerQueueLimitCalculator());
            return oppContainerAllocatingAMService;
        }
        // 一般情况下 创建 ApplicationMasterService
        return new ApplicationMasterService(this.rmContext, scheduler);
    }

    protected AdminService createAdminService() {
        // 创建 AdminService
        return new AdminService(this);
    }

    protected RMSecretManagerService createRMSecretManagerService() {
        // 创建 RMSecretManagerService
        return new RMSecretManagerService(conf, rmContext);
    }

    private boolean isOpportunisticSchedulingEnabled(Configuration conf) {
        return YarnConfiguration.isOpportunisticContainerAllocationEnabled(conf)
                || YarnConfiguration.isDistSchedulingEnabled(conf);
    }

    /**
     * Create RMDelegatedNodeLabelsUpdater based on configuration.
     */
    protected RMDelegatedNodeLabelsUpdater createRMDelegatedNodeLabelsUpdater() {
        if (conf.getBoolean(YarnConfiguration.NODE_LABELS_ENABLED,
                YarnConfiguration.DEFAULT_NODE_LABELS_ENABLED)
                && YarnConfiguration.isDelegatedCentralizedNodeLabelConfiguration(
                conf)) {
            return new RMDelegatedNodeLabelsUpdater(rmContext);
        } else {
            return null;
        }
    }

    @Private
    public ClientRMService getClientRMService() {
        return this.clientRM;
    }

    /**
     * return the scheduler.
     *
     * @return the scheduler for the Resource Manager.
     */
    @Private
    public ResourceScheduler getResourceScheduler() {
        return this.scheduler;
    }

    /**
     * return the resource tracking component.
     *
     * @return the resource tracking component.
     */
    @Private
    public ResourceTrackerService getResourceTrackerService() {
        return this.resourceTracker;
    }

    @Private
    public ApplicationMasterService getApplicationMasterService() {
        return this.masterService;
    }

    @Private
    public ApplicationACLsManager getApplicationACLsManager() {
        return this.applicationACLsManager;
    }

    @Private
    public QueueACLsManager getQueueACLsManager() {
        return this.queueACLsManager;
    }

    @Private
    @VisibleForTesting
    public FederationStateStoreService getFederationStateStoreService() {
        return this.federationStateStoreService;
    }

    @Private
    WebApp getWebapp() {
        return this.webApp;
    }

    @Override
    public void recover(RMState state) throws Exception {
        // recover RMdelegationTokenSecretManager
        rmContext.getRMDelegationTokenSecretManager().recover(state);

        // recover AMRMTokenSecretManager
        rmContext.getAMRMTokenSecretManager().recover(state);

        // recover reservations
        if (reservationSystem != null) {
            reservationSystem.recover(state);
        }
        // recover applications
        rmAppManager.recover(state);

        setSchedulerRecoveryStartAndWaitTime(state, conf);
    }

    public static void main(String[] argv) {
        Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
        StringUtils.startupShutdownMessage(ResourceManager.class, argv, LOG);
        try {

            // 创建 YarnConfiguration (与 HDFS 类似)
            Configuration conf = new YarnConfiguration();

            // 解析参数
            GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
            argv = hParser.getRemainingArgs();
            // If -format-state-store, then delete RMStateStore; else startup normally
            if (argv.length >= 1) {
                if (argv[0].equals("-format-state-store")) {
                    deleteRMStateStore(conf);
                } else if (argv[0].equals("-remove-application-from-state-store")
                        && argv.length == 2) {
                    removeApplication(conf, argv[1]);
                } else {
                    printUsage(System.err);
                }
            } else {
                // 创建 ResourceManager
                ResourceManager resourceManager = new ResourceManager();
                ShutdownHookManager.get().addShutdownHook(
                        new CompositeServiceShutdownHook(resourceManager),
                        SHUTDOWN_HOOK_PRIORITY);
                // 初始化 ResourceManager (直接调用其 serviceInit())
                /** 来自于 RMActiveServices.serviceInit() 并执行如下服务的 serviceInit()
                 * 1 RMSecretManagerService            (安全服务)
                 * 2 ContainerAllocationExpirer        (容器过期服务)
                 * 3 AMLivelinessMonitor               (ApplicationMaster 存活监控服务)
                 * 4 AMLivelinessMonitor               (ApplicationMaster 已完成监控服务)
                 * 5 RMAppLifetimeMonitor              (ApplicationMaster 生命周期监控服务)
                 * 6 RMNodeLabelsManager               (ResourceManager 节点标签管理服务)
                 * 7 MemoryPlacementConstraintManager  (基于内存的安置约束管理器服务)
                 * 8 NodesListManager                  (NodeManager 管理器服务)
                 * 9 CapacityScheduler                 (资源调度器服务)
                 * 10 EventDispatcher                  (资源调度事件分发器)
                 * 11 NMLivelinessMonitor              (NodeManager 存活监控服务)
                 * 12 ResourceTrackerService           (NodeManager 上报资源处理服务)
                 * 13 ApplicationMasterService         (管理 ApplicationMaster 服务)
                 * 14 ClientRMService                  (客户端[可以提交任务、停止任务等等]发送 RPC 请求处理服务)
                 * 15 ApplicationMasterLauncher        (负责启动 ApplicationMaster 服务)
                 */
                /** 来自于 ResourceManager.serviceInit() 并执行如下服务的 serviceInit()
                 * 1 AsyncDispatcher                   (异步中央事件分发器服务)
                 * 2 AdminService                      (ResourceManager 管理者服务)
                 * 3 ActiveStandbyElectorBasedElectorService (ResourceManager HA 选举服务)
                 * 4 RMApplicationHistoryWriter              (ResourceManager 历史服务器)
                 * 5 CombinedSystemMetricsPublisher          (监控发布服务)
                 */
                resourceManager.init(conf);
                // 启动 ResourceManager (直接调用其 serviceStart())
                resourceManager.start();
            }
        } catch (Throwable t) {
            LOG.fatal("Error starting ResourceManager", t);
            System.exit(-1);
        }
    }

    /**
     * Register the handlers for alwaysOn services
     */
    private Dispatcher setupDispatcher() {
        // 创建 AsyncDispatcher
        Dispatcher dispatcher = createDispatcher();
        // 往异步事件处理器注册事件类型为 RMFatalEventType, 事件处理函数为 RMFatalEventDispatcher
        dispatcher.register(
                RMFatalEventType.class,
                new ResourceManager.RMFatalEventDispatcher());
        // 返回 AsyncDispatcher
        return dispatcher;
    }

    private void resetRMContext() {
        RMContextImpl rmContextImpl = new RMContextImpl();
        // transfer service context to new RM service Context
        rmContextImpl.setServiceContext(rmContext.getServiceContext());

        // reset dispatcher
        Dispatcher dispatcher = setupDispatcher();
        ((Service) dispatcher).init(this.conf);
        ((Service) dispatcher).start();
        removeService((Service) rmDispatcher);
        // Need to stop previous rmDispatcher before assigning new dispatcher
        // otherwise causes "AsyncDispatcher event handler" thread leak
        ((Service) rmDispatcher).stop();
        rmDispatcher = dispatcher;
        addIfService(rmDispatcher);
        rmContextImpl.setDispatcher(dispatcher);

        rmContext = rmContextImpl;
    }

    private void setSchedulerRecoveryStartAndWaitTime(RMState state,
                                                      Configuration conf) {
        if (!state.getApplicationState().isEmpty()) {
            long waitTime =
                    conf.getLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
                            YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS);
            rmContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
        }
    }

    /**
     * Retrieve RM bind address from configuration
     *
     * @param conf
     * @return InetSocketAddress
     */
    public static InetSocketAddress getBindAddress(Configuration conf) {
        return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
    }

    /**
     * Deletes the RMStateStore
     *
     * @param conf
     * @throws Exception
     */
    @VisibleForTesting
    static void deleteRMStateStore(Configuration conf) throws Exception {
        RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
        rmStore.setResourceManager(new ResourceManager());
        rmStore.init(conf);
        rmStore.start();
        try {
            LOG.info("Deleting ResourceManager state store...");
            rmStore.deleteStore();
            LOG.info("State store deleted");
        } finally {
            rmStore.stop();
        }
    }

    @VisibleForTesting
    static void removeApplication(Configuration conf, String applicationId)
            throws Exception {
        RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
        rmStore.setResourceManager(new ResourceManager());
        rmStore.init(conf);
        rmStore.start();
        try {
            ApplicationId removeAppId = ApplicationId.fromString(applicationId);
            LOG.info("Deleting application " + removeAppId + " from state store");
            rmStore.removeApplication(removeAppId);
            LOG.info("Application is deleted from state store");
        } finally {
            rmStore.stop();
        }
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: yarn resourcemanager [-format-state-store]");
        out.println("                            "
                + "[-remove-application-from-state-store <appId>]" + "\n");
    }

    protected RMAppLifetimeMonitor createRMAppLifetimeMonitor() {
        // 创建 RMAppLifetimeMonitor
        return new RMAppLifetimeMonitor(this.rmContext);
    }

    /**
     * Register ResourceManagerMXBean.
     */
    private void registerMXBean() {
        MBeans.register("ResourceManager", "ResourceManager", this);
    }

    @Override
    public boolean isSecurityEnabled() {
        return UserGroupInformation.isSecurityEnabled();
    }
}
