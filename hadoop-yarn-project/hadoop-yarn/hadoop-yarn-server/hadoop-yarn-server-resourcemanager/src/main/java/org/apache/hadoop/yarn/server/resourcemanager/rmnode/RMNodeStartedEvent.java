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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;

public class RMNodeStartedEvent extends RMNodeEvent {

    private List<NMContainerStatus> containerStatuses;
    private List<ApplicationId> runningApplications;
    private List<LogAggregationReport> logAggregationReportsForApps;

    public RMNodeStartedEvent(NodeId nodeId,
                              List<NMContainerStatus> containerReports,
                              List<ApplicationId> runningApplications) {
        // EventType = STARTED
        super(nodeId, RMNodeEventType.STARTED);
        // 针对 NM 向 RM 注册信息中 containerReports 为空
        this.containerStatuses = containerReports;
        // 针对 NM 向 RM 注册信息中 runningApplications 为空
        this.runningApplications = runningApplications;
    }

    public List<NMContainerStatus> getNMContainerStatuses() {
        return this.containerStatuses;
    }

    public List<ApplicationId> getRunningApplications() {
        return runningApplications;
    }

    public List<LogAggregationReport> getLogAggregationReportsForApps() {
        return this.logAggregationReportsForApps;
    }

    public void setLogAggregationReportsForApps(
            List<LogAggregationReport> logAggregationReportsForApps) {
        this.logAggregationReportsForApps = logAggregationReportsForApps;
    }
}
