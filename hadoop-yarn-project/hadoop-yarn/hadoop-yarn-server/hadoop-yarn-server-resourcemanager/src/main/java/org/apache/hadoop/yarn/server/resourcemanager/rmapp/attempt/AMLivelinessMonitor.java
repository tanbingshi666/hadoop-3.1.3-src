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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.Clock;

public class AMLivelinessMonitor extends AbstractLivelinessMonitor<ApplicationAttemptId> {

    private EventHandler<Event> dispatcher;

    public AMLivelinessMonitor(Dispatcher d) {
        // 往下追
        super("AMLivelinessMonitor");
        // 获取 ResourceManager 的 AsyncDispatcher 的通用事件处理器 GenericEventHandler
        // GenericEventHandler 主要将 Event 添加到阻塞队列 (put)
        this.dispatcher = d.getEventHandler();
    }

    public AMLivelinessMonitor(Dispatcher d, Clock clock) {
        super("AMLivelinessMonitor", clock);
        this.dispatcher = d.getEventHandler();
    }

    public void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        // ApplicationMaster 存活过期时间 默认 5 分钟 (ApplicationMaster 持续上报以便检查不过期)
        int expireIntvl = conf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
        setExpireInterval(expireIntvl);
        // 默认每隔 200s 检查
        setMonitorInterval(expireIntvl / 3);
    }

    @Override
    protected void expire(ApplicationAttemptId id) {
        dispatcher.handle(
                new RMAppAttemptEvent(id, RMAppAttemptEventType.EXPIRE));
    }
}