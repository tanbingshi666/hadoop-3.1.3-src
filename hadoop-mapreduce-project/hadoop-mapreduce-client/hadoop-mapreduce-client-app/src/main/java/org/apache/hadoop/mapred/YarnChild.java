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

package org.apache.hadoop.mapred;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main() for MapReduce task processes.
 */
class YarnChild {

    private static final Logger LOG = LoggerFactory.getLogger(YarnChild.class);

    static volatile TaskAttemptID taskid = null;

    public static void main(String[] args) throws Throwable {
        Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
        LOG.debug("Child starting");

        // 获取任务配置文件并解析 也即 job.xml
        final JobConf job = new JobConf(MRJobConfig.JOB_CONF_FILE);
        // Initing with our JobConf allows us to avoid loading confs twice
        // 初始化配置避免加载两次
        Limits.init(job);

        UserGroupInformation.setConfiguration(job);
        // MAPREDUCE-6565: need to set configuration for SecurityUtil.
        SecurityUtil.setConfiguration(job);

        /**
         * exec /bin/bash -c
         * "$JAVA_HOME/bin/java
         * -Djava.net.preferIPv4Stack=true
         * -Dhadoop.metrics.log.level=WARN
         * -Xmx820m
         * -Djava.io.tmpdir=$PWD/tmp
         * -Dlog4j.configuration=container-log4j.properties
         * -Dyarn.app.container.log.dir=/opt/app/hadoop-3.1.3/logs/userlogs/application_1684656010852_0007/container_1684656010852_0007_01_000002
         * -Dyarn.app.container.log.filesize=0
         * -Dhadoop.root.logger=INFO,CLA
         * -Dhadoop.root.logfile=syslog
         * org.apache.hadoop.mapred.YarnChild
         * 192.168.6.102
         * 39956
         * attempt_1684656010852_0007_m_000000_0
         * 2
         * 1>/opt/app/hadoop-3.1.3/logs/userlogs/application_1684656010852_0007/container_1684656010852_0007_01_000002/stdout
         * 2>/opt/app/hadoop-3.1.3/logs/userlogs/application_1684656010852_0007/container_1684656010852_0007_01_000002/stderr
         */
        // AM 地址
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        final InetSocketAddress address =
                NetUtils.createSocketAddrForHost(host, port);
        final TaskAttemptID firstTaskid = TaskAttemptID.forName(args[2]);
        long jvmIdLong = Long.parseLong(args[3]);
        JVMId jvmId = new JVMId(firstTaskid.getJobID(),
                firstTaskid.getTaskType() == TaskType.MAP, jvmIdLong);

        CallerContext.setCurrent(
                new CallerContext.Builder("mr_" + firstTaskid.toString()).build());

        // initialize metrics
        DefaultMetricsSystem.initialize(
                StringUtils.camelize(firstTaskid.getTaskType().name()) + "Task");

        // Security framework already loaded the tokens into current ugi
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();

        // Executing with tokens:
        // [Kind: mapreduce.job, Service: job_1684656010852_0007,
        // Ident: (org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier@22555ebf)]
        LOG.info("Executing with tokens: {}", credentials.getAllTokens());

        // Create TaskUmbilicalProtocol as actual task owner.
        UserGroupInformation taskOwner =
                UserGroupInformation.createRemoteUser(firstTaskid.getJobID().toString());
        Token<JobTokenIdentifier> jt = TokenCache.getJobToken(credentials);
        SecurityUtil.setTokenService(jt, address);
        taskOwner.addToken(jt);
        // 获取 AM(MRAppMaster) 的 Task 监听服务 TaskAttemptListenerImpl
        // 该 TaskAttemptListenerImpl 内部启动了一个 RPC Server 通讯协议为 TaskUmbilicalProtocol
        // 这个获取 AM 的 RPC 服务 TaskAttemptListenerImpl 的 RPC 代理对象 (也即客户端)
        final TaskUmbilicalProtocol umbilical =
                taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
                    @Override
                    public TaskUmbilicalProtocol run() throws Exception {
                        return (TaskUmbilicalProtocol) RPC.getProxy(TaskUmbilicalProtocol.class,
                                TaskUmbilicalProtocol.versionID, address, job);
                    }
                });

        // report non-pid to application master
        JvmContext context = new JvmContext(jvmId, "-1000");
        LOG.debug("PID: " + System.getenv().get("JVM_PID"));
        Task task = null;
        UserGroupInformation childUGI = null;
        ScheduledExecutorService logSyncer = null;

        try {
            int idleLoopCount = 0;
            JvmTask myTask = null;
            // poll for new task
            for (int idle = 0; null == myTask; ++idle) {
                long sleepTimeMilliSecs = Math.min(idle * 500, 1500);
                // Sleeping for 0ms before retrying again. Got null now.
                LOG.info("Sleeping for " + sleepTimeMilliSecs
                        + "ms before retrying again. Got null now.");
                MILLISECONDS.sleep(sleepTimeMilliSecs);
                // 发送 RPC 请求给 AM 的 TaskAttemptListenerImpl RPC 服务获取当前 YarnChild 进程执行任务
                myTask = umbilical.getTask(context);
            }
            if (myTask.shouldDie()) {
                return;
            }

            // 获取执行任务 可能是 MapTask 也可能是 ReduceTask
            task = myTask.getTask();
            YarnChild.taskid = task.getTaskID();

            // Create the job-conf and set credentials
            // 配置执行 Task 的一些配置信息以及证书相关的
            configureTask(job, task, credentials, jt);

            // log the system properties
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
             * java.class.path: /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/tanbs/appcache/application_1684656010852_0007/container_1684656010852_0007_01_000002:/opt/app/hadoop-3.1.3/etc/hadoop:/opt/app/hadoop-3.1.3/share/hadoop/common/hadoop-common-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/hadoop-common-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/hadoop-kms-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/hadoop-nfs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-io-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/accessors-smart-1.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jcip-annotations-1.0-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/animal-sniffer-annotations-1.17.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/netty-3.10.5.Final.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/asm-5.0.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jersey-core-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/audience-annotations-0.5.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/nimbus-jose-jwt-4.41.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/avro-1.7.7.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-security-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/checker-qual-2.5.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-server-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-beanutils-1.9.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/paranamer-2.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-cli-1.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jsr311-api-1.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-codec-1.11.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jersey-json-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-collections-3.2.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jul-to-slf4j-1.7.25.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-compress-1.18.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jersey-servlet-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-configuration2-2.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-io-2.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/re2j-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-lang-2.6.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-admin-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-lang3-3.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-client-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-logging-1.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-common-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-math3-3.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/slf4j-api-1.7.25.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/commons-net-3.6.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-core-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/curator-client-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-crypto-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/curator-framework-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-identity-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/curator-recipes-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jersey-server-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/error_prone_annotations-2.2.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-server-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/failureaccess-1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/gson-2.2.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/snappy-java-1.0.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/guava-27.0-jre.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-simplekdc-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/hadoop-annotations-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerb-util-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/hadoop-auth-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jsch-0.1.54.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/htrace-core4-4.1.0-incubating.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/stax2-api-3.1.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/httpclient-4.5.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/token-provider-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/httpcore-4.4.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-asn1-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/j2objc-annotations-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jettison-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-annotations-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-config-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-core-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-pkix-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-util-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-databind-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/kerby-xdr-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-http-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/log4j-1.2.17.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/javax.servlet-api-3.1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/woodstox-core-5.0.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jaxb-api-2.2.11.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/metrics-core-3.2.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-servlet-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/json-smart-2.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-util-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jsp-api-2.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-webapp-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jsr305-3.0.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/jetty-xml-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/opt/app/hadoop-3.1.3/share/hadoop/common/lib/zookeeper-3.4.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-client-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-client-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-httpfs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-native-client-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-native-client-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-nfs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-rbf-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/hadoop-hdfs-rbf-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-http-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/accessors-smart-1.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jaxb-impl-2.2.3-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/animal-sniffer-annotations-1.17.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/log4j-1.2.17.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/asm-5.0.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jcip-annotations-1.0-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/audience-annotations-0.5.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/netty-3.10.5.Final.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/avro-1.7.7.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-io-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/checker-qual-2.5.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-security-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-beanutils-1.9.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/netty-all-4.0.52.Final.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/json-smart-2.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-codec-1.11.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jersey-core-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-collections-3.2.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jsr305-3.0.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-compress-1.18.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jersey-server-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-configuration2-2.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jsr311-api-1.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/nimbus-jose-jwt-4.41.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-io-2.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/okhttp-2.7.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-lang-2.6.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-admin-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-lang3-3.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-client-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-common-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-math3-3.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/okio-1.6.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/commons-net-3.6.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-core-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/curator-client-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-crypto-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/curator-framework-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-identity-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/curator-recipes-2.13.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jersey-json-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/error_prone_annotations-2.2.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-server-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/failureaccess-1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/paranamer-2.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/gson-2.2.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/guava-27.0-jre.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-simplekdc-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/hadoop-annotations-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerb-util-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/hadoop-auth-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-webapp-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/htrace-core4-4.1.0-incubating.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/re2j-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/httpclient-4.5.2.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/snappy-java-1.0.5.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/httpcore-4.4.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-asn1-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/j2objc-annotations-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jersey-servlet-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-annotations-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-config-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-core-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-pkix-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-util-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-databind-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/kerby-xdr-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-jaxrs-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jettison-1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jackson-xc-1.9.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/javax.servlet-api-3.1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/stax2-api-3.1.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jaxb-api-2.2.11.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-server-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-xml-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-servlet-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jsch-0.1.54.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-util-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/json-simple-1.1.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/jetty-util-ajax-9.3.24.v20180605.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/token-provider-1.0.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/woodstox-core-5.0.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/hdfs/lib/zookeeper-3.4.13.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-api-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-client-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-common-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-registry-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-common-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-nodemanager-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-router-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-tests-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-timeline-pluginstorage-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-server-web-proxy-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-services-api-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/hadoop-yarn-services-core-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/HikariCP-java7-2.4.12.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/aopalliance-1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/dnsjava-2.1.7.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/ehcache-3.3.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/fst-2.50.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/geronimo-jcache_1.0_spec-1.0-alpha-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/guice-4.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/guice-servlet-4.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jackson-jaxrs-base-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jackson-jaxrs-json-provider-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jackson-module-jaxb-annotations-2.7.8.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/java-util-1.9.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/javax.inject-1.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jersey-client-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/jersey-guice-1.19.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/json-io-2.5.1.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/metrics-core-3.2.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/mssql-jdbc-6.2.1.jre7.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/objenesis-1.0.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/snakeyaml-1.16.jar:/opt/app/hadoop-3.1.3/share/hadoop/yarn/lib/swagger-annotations-1.5.4.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-nativetask-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-uploader-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/opt/app/hadoop-3.1.3/share/hadoop/mapreduce/lib/junit-4.11.jar:job.jar/job.jar:job.jar/classes/:job.jar/lib/*:/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/tanbs/appcache/application_1684656010852_0007/container_1684656010852_0007_01_000002/job.jar
             * java.io.tmpdir: /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/tanbs/appcache/application_1684656010852_0007/container_1684656010852_0007_01_000002/tmp
             * user.dir: /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/tanbs/appcache/application_1684656010852_0007/container_1684656010852_0007_01_000002
             * user.name: tanbs
             * ************************************************************
             */
            String systemPropsToLog = MRApps.getSystemPropertiesToLog(job);
            if (systemPropsToLog != null) {
                LOG.info(systemPropsToLog);
            }

            // Initiate Java VM metrics
            JvmMetrics.initSingleton(jvmId.toString(), job.getSessionId());
            childUGI = UserGroupInformation.createRemoteUser(System
                    .getenv(ApplicationConstants.Environment.USER.toString()));
            // Add tokens to new user so that it may execute its task correctly.
            childUGI.addCredentials(credentials);

            // set job classloader if configured before invoking the task
            // 默认没有配置类加载器
            MRApps.setJobClassLoader(job);

            logSyncer = TaskLog.createLogSyncer();

            // Create a final reference to the task for the doAs block
            // 这里就是执行 MR 任务的最终 MapTask 或者 ReduceTask
            final Task taskFinal = task;
            childUGI.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    // use job-specified working directory
                    setEncryptedSpillKeyIfRequired(taskFinal);
                    FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
                    // 调用 MapTask.run() 或者 ReduceTask.run()
                    taskFinal.run(job, umbilical); // run the task
                    return null;
                }
            });
        } catch (FSError e) {
            LOG.error("FSError from child", e);
            if (!ShutdownHookManager.get().isShutdownInProgress()) {
                umbilical.fsError(taskid, e.getMessage());
            }
        } catch (Exception exception) {
            LOG.warn("Exception running child : "
                    + StringUtils.stringifyException(exception));
            try {
                if (task != null) {
                    // do cleanup for the task
                    if (childUGI == null) { // no need to job into doAs block
                        task.taskCleanup(umbilical);
                    } else {
                        final Task taskFinal = task;
                        childUGI.doAs(new PrivilegedExceptionAction<Object>() {
                            @Override
                            public Object run() throws Exception {
                                taskFinal.taskCleanup(umbilical);
                                return null;
                            }
                        });
                    }
                }
            } catch (Exception e) {
                LOG.info("Exception cleaning up: " + StringUtils.stringifyException(e));
            }
            // Report back any failures, for diagnostic purposes
            if (taskid != null) {
                if (!ShutdownHookManager.get().isShutdownInProgress()) {
                    umbilical.fatalError(taskid,
                            StringUtils.stringifyException(exception), false);
                }
            }
        } catch (Throwable throwable) {
            LOG.error("Error running child : "
                    + StringUtils.stringifyException(throwable));
            if (taskid != null) {
                if (!ShutdownHookManager.get().isShutdownInProgress()) {
                    Throwable tCause = throwable.getCause();
                    String cause =
                            tCause == null ? throwable.getMessage() : StringUtils
                                    .stringifyException(tCause);
                    umbilical.fatalError(taskid, cause, false);
                }
            }
        } finally {
            RPC.stopProxy(umbilical);
            DefaultMetricsSystem.shutdown();
            TaskLog.syncLogsShutdown(logSyncer);
        }
    }

    /**
     * Utility method to check if the Encrypted Spill Key needs to be set into the
     * user credentials of the user running the Map / Reduce Task
     *
     * @param task The Map / Reduce task to set the Encrypted Spill information in
     * @throws Exception
     */
    public static void setEncryptedSpillKeyIfRequired(Task task) throws
            Exception {
        if ((task != null) && (task.getEncryptedSpillKey() != null) && (task
                .getEncryptedSpillKey().length > 1)) {
            Credentials creds =
                    UserGroupInformation.getCurrentUser().getCredentials();
            TokenCache.setEncryptedSpillKey(task.getEncryptedSpillKey(), creds);
            UserGroupInformation.getCurrentUser().addCredentials(creds);
        }
    }

    /**
     * Configure mapred-local dirs. This config is used by the task for finding
     * out an output directory.
     *
     * @throws IOException
     */
    private static void configureLocalDirs(Task task, JobConf job) throws IOException {
        String[] localSysDirs = StringUtils.getTrimmedStrings(
                System.getenv(Environment.LOCAL_DIRS.name()));
        job.setStrings(MRConfig.LOCAL_DIR, localSysDirs);
        LOG.info(MRConfig.LOCAL_DIR + " for child: " + job.get(MRConfig.LOCAL_DIR));
        LocalDirAllocator lDirAlloc = new LocalDirAllocator(MRConfig.LOCAL_DIR);
        Path workDir = null;
        // First, try to find the JOB_LOCAL_DIR on this host.
        try {
            workDir = lDirAlloc.getLocalPathToRead("work", job);
        } catch (DiskErrorException e) {
            // DiskErrorException means dir not found. If not found, it will
            // be created below.
        }
        if (workDir == null) {
            // JOB_LOCAL_DIR doesn't exist on this host -- Create it.
            workDir = lDirAlloc.getLocalPathForWrite("work", job);
            FileSystem lfs = FileSystem.getLocal(job).getRaw();
            boolean madeDir = false;
            try {
                madeDir = lfs.mkdirs(workDir);
            } catch (FileAlreadyExistsException e) {
                // Since all tasks will be running in their own JVM, the race condition
                // exists where multiple tasks could be trying to create this directory
                // at the same time. If this task loses the race, it's okay because
                // the directory already exists.
                madeDir = true;
                workDir = lDirAlloc.getLocalPathToRead("work", job);
            }
            if (!madeDir) {
                throw new IOException("Mkdirs failed to create "
                        + workDir.toString());
            }
        }
        job.set(MRJobConfig.JOB_LOCAL_DIR, workDir.toString());
    }

    private static void configureTask(JobConf job, Task task,
                                      Credentials credentials, Token<JobTokenIdentifier> jt) throws IOException {
        job.setCredentials(credentials);

        ApplicationAttemptId appAttemptId = ContainerId.fromString(
                        System.getenv(Environment.CONTAINER_ID.name()))
                .getApplicationAttemptId();
        LOG.debug("APPLICATION_ATTEMPT_ID: " + appAttemptId);
        // Set it in conf, so as to be able to be used the the OutputCommitter.
        job.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
                appAttemptId.getAttemptId());

        // set tcp nodelay
        job.setBoolean("ipc.client.tcpnodelay", true);
        job.setClass(MRConfig.TASK_LOCAL_OUTPUT_CLASS,
                YarnOutputFiles.class, MapOutputFile.class);
        // set the jobToken and shuffle secrets into task
        task.setJobTokenSecret(
                JobTokenSecretManager.createSecretKey(jt.getPassword()));
        byte[] shuffleSecret = TokenCache.getShuffleSecretKey(credentials);
        if (shuffleSecret == null) {
            LOG.warn("Shuffle secret missing from task credentials."
                    + " Using job token secret as shuffle secret.");
            shuffleSecret = jt.getPassword();
        }
        task.setShuffleSecret(
                JobTokenSecretManager.createSecretKey(shuffleSecret));

        // setup the child's MRConfig.LOCAL_DIR.
        configureLocalDirs(task, job);

        // setup the child's attempt directories
        // Do the task-type specific localization
        task.localizeConfiguration(job);

        // Set up the DistributedCache related configs
        MRApps.setupDistributedCacheLocal(job);

        // Overwrite the localized task jobconf which is linked to in the current
        // work-dir.
        Path localTaskFile = new Path(MRJobConfig.JOB_CONF_FILE);
        writeLocalJobFile(localTaskFile, job);
        task.setJobFile(localTaskFile.toString());
        task.setConf(job);
    }

    private static final FsPermission urw_gr =
            FsPermission.createImmutable((short) 0640);

    /**
     * Write the task specific job-configuration file.
     *
     * @throws IOException
     */
    private static void writeLocalJobFile(Path jobFile, JobConf conf)
            throws IOException {
        FileSystem localFs = FileSystem.getLocal(conf);
        localFs.delete(jobFile);
        OutputStream out = null;
        try {
            out = FileSystem.create(localFs, jobFile, urw_gr);
            conf.writeXml(out);
        } finally {
            IOUtils.cleanupWithLogger(LOG, out);
        }
    }

}
