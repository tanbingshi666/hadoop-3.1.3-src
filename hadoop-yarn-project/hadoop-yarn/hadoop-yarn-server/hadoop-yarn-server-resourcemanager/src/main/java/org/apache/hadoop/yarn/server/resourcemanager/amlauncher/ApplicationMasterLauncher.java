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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;


public class ApplicationMasterLauncher extends AbstractService implements
        EventHandler<AMLauncherEvent> {
    private static final Log LOG = LogFactory.getLog(
            ApplicationMasterLauncher.class);
    private ThreadPoolExecutor launcherPool;
    private LauncherThread launcherHandlingThread;

    private final BlockingQueue<Runnable> masterEvents
            = new LinkedBlockingQueue<Runnable>();

    protected final RMContext context;

    public ApplicationMasterLauncher(RMContext context) {
        super(ApplicationMasterLauncher.class.getName());
        this.context = context;
        // 创建 LauncherThread
        this.launcherHandlingThread = new LauncherThread();
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        // 线程个数 默认 50
        int threadCount = conf.getInt(
                YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
                YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT);
        // 自定义线程池
        ThreadFactory tf = new ThreadFactoryBuilder()
                .setNameFormat("ApplicationMasterLauncher #%d")
                .build();
        launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1,
                TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
        launcherPool.setThreadFactory(tf);

        // 创建配置对象 YarnConfiguration
        Configuration newConf = new YarnConfiguration(conf);
        // 默认 10
        newConf.setInt(CommonConfigurationKeysPublic.
                        IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
                conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETRIES,
                        YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES));
        // 设置配置对象
        setConfig(newConf);

        super.serviceInit(newConf);
    }

    @Override
    protected void serviceStart() throws Exception {
        // 启动 LauncherThread 线程阻塞拉取需要启动 ApplicationMaster
        launcherHandlingThread.start();
        super.serviceStart();
    }

    protected Runnable createRunnableLauncher(RMAppAttempt application,
                                              AMLauncherEventType event) {
        // 创建 AMLauncher 并返回
        Runnable launcher =
                new AMLauncher(context, application, event, getConfig());
        return launcher;
    }

    private void launch(RMAppAttempt application) {

        // 创建 AMLauncher
        Runnable launcher = createRunnableLauncher(application,
                AMLauncherEventType.LAUNCH);
        // 将启动 AM 的 AMLauncher 任务存放到阻塞队列等待 LauncherThread 线程处理
        // 调用 LauncherThread.run()
        masterEvents.add(launcher);
    }


    @Override
    protected void serviceStop() throws Exception {
        launcherHandlingThread.interrupt();
        try {
            launcherHandlingThread.join();
        } catch (InterruptedException ie) {
            LOG.info(launcherHandlingThread.getName() + " interrupted during join ",
                    ie);
        }
        launcherPool.shutdown();
    }

    private class LauncherThread extends Thread {

        public LauncherThread() {
            super("ApplicationMaster Launcher");
        }

        @Override
        public void run() {
            while (!this.isInterrupted()) {
                Runnable toLaunch;
                try {
                    // 拉取需要启动 ApplicationMaster
                    toLaunch = masterEvents.take();
                    // 执行启动 AM 调用 AMLauncher.run()
                    launcherPool.execute(toLaunch);
                } catch (InterruptedException e) {
                    LOG.warn(this.getClass().getName() + " interrupted. Returning.");
                    return;
                }
            }
        }
    }

    private void cleanup(RMAppAttempt application) {
        Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.CLEANUP);
        masterEvents.add(launcher);
    }

    @Override
    public synchronized void handle(AMLauncherEvent appEvent) {

        // event = AMLauncherEvent
        // evenType = AMLauncherEventType.LAUNCH
        AMLauncherEventType event = appEvent.getType();
        RMAppAttempt application = appEvent.getAppAttempt();
        switch (event) {
            case LAUNCH:
                // 启动 AM
                launch(application);
                break;
            case CLEANUP:
                cleanup(application);
                break;
            default:
                break;
        }
    }
}
