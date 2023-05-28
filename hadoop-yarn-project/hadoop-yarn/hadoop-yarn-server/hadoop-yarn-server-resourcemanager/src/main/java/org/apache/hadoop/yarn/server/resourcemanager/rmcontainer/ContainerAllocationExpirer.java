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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ContainerAllocationExpirer extends
        AbstractLivelinessMonitor<AllocationExpirationInfo> {

    private EventHandler dispatcher;

    public ContainerAllocationExpirer(Dispatcher d) {
        // 往下追
        super(ContainerAllocationExpirer.class.getName());
        // 获取 ResourceManager 的 AsyncDispatcher 的通用事件处理器 GenericEventHandler
        // GenericEventHandler 主要将 Event 添加到阻塞队列 (put)
        this.dispatcher = d.getEventHandler();
    }

    public void serviceInit(Configuration conf) throws Exception {
        // 容器过期时间 默认 600 * 1000L 也即 5 分钟
        int expireIntvl = conf.getInt(
                YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
        setExpireInterval(expireIntvl);
        // 默认 200s 检查
        setMonitorInterval(expireIntvl / 3);
        super.serviceInit(conf);
    }

    @Override
    protected void expire(AllocationExpirationInfo allocationExpirationInfo) {
        dispatcher.handle(new ContainerExpiredSchedulerEvent(
                allocationExpirationInfo.getContainerId(),
                allocationExpirationInfo.isIncrease()));
    }
}
