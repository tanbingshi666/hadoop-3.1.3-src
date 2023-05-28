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
package org.apache.hadoop.hdfs.tools;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.BadFencingConfigurationException;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;

import com.google.common.base.Preconditions;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;

/**
 * One of the NN NameNodes acting as the target of an administrative command
 * (e.g. failover).
 */
@InterfaceAudience.Private
public class NNHAServiceTarget extends HAServiceTarget {

    // Keys added to the fencing script environment
    private static final String NAMESERVICE_ID_KEY = "nameserviceid";
    private static final String NAMENODE_ID_KEY = "namenodeid";

    private final InetSocketAddress addr;
    private final InetSocketAddress lifelineAddr;
    private InetSocketAddress zkfcAddr;
    private NodeFencer fencer;
    private BadFencingConfigurationException fenceConfigError;
    private final String nnId;
    private final String nsId;
    private final boolean autoFailoverEnabled;

    public NNHAServiceTarget(
            Configuration conf,
            String nsId,
            String nnId) {
        Preconditions.checkNotNull(nnId);

        if (nsId == null) {
            nsId = DFSUtil.getOnlyNameServiceIdOrNull(conf);
            if (nsId == null) {
                String errorString = "Unable to determine the name service ID.";
                String[] dfsNames = conf.getStrings(DFS_NAMESERVICES);
                if ((dfsNames != null) && (dfsNames.length > 1)) {
                    errorString = "Unable to determine the name service ID. " +
                            "This is an HA configuration with multiple name services " +
                            "configured. " + DFS_NAMESERVICES + " is set to " +
                            Arrays.toString(dfsNames) + ". Please re-run with the -ns option.";
                }
                throw new IllegalArgumentException(errorString);
            }
        }
        assert nsId != null;

        // Make a copy of the conf, and override configs based on the
        // target node -- not the node we happen to be running on.
        // 拷贝 Configuration
        HdfsConfiguration targetConf = new HdfsConfiguration(conf);
        NameNode.initializeGenericKeys(targetConf, nsId, nnId);

        // 获取 key = dfs.namenode.rpc-address.mycluster.nn1 对应的 value 比如 hj101:8020
        String serviceAddr =
                DFSUtil.getNamenodeServiceAddr(targetConf, nsId, nnId);
        if (serviceAddr == null) {
            throw new IllegalArgumentException(
                    "Unable to determine service address for namenode '" + nnId + "'");
        }
        // 创建 NameNode 的 InetSocketAddress(hj101,8020)
        this.addr = NetUtils.createSocketAddr(serviceAddr,
                HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);

        // 默认值 null
        String lifelineAddrStr =
                DFSUtil.getNamenodeLifelineAddr(targetConf, nsId, nnId);
        this.lifelineAddr = (lifelineAddrStr != null) ?
                NetUtils.createSocketAddr(lifelineAddrStr) : null;

        // 获取 key = dfs.ha.automatic-failover.enabled 对应的 value 默认值 false
        // 一般情况下 如果配置了 NameNode HA 在 hdfs-site.xml 需要配置为 true
        this.autoFailoverEnabled = targetConf.getBoolean(
                DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_KEY,
                DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT);
        if (autoFailoverEnabled) {
            // key = dfs.ha.zkfc.port value = 8019
            int port = DFSZKFailoverController.getZkfcPort(targetConf);
            if (port != 0) {
                // 创建 zkfcAddr = new InetSocketAddress(hj101,8019)
                setZkfcPort(port);
            }
        }

        try {
            // 创建 NodeFencer 对象
            this.fencer = NodeFencer.create(targetConf,
                    DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY);
        } catch (BadFencingConfigurationException e) {
            this.fenceConfigError = e;
        }

        // nn1 nn2
        this.nnId = nnId;
        // mycluster
        this.nsId = nsId;
    }

    /**
     * @return the NN's IPC address.
     */
    @Override
    public InetSocketAddress getAddress() {
        return addr;
    }

    @Override
    public InetSocketAddress getHealthMonitorAddress() {
        return lifelineAddr;
    }

    @Override
    public InetSocketAddress getZKFCAddress() {
        Preconditions.checkState(autoFailoverEnabled,
                "ZKFC address not relevant when auto failover is off");
        assert zkfcAddr != null;

        return zkfcAddr;
    }

    void setZkfcPort(int port) {
        assert autoFailoverEnabled;

        this.zkfcAddr = new InetSocketAddress(addr.getAddress(), port);
    }

    @Override
    public void checkFencingConfigured() throws BadFencingConfigurationException {
        if (fenceConfigError != null) {
            throw fenceConfigError;
        }
        if (fencer == null) {
            throw new BadFencingConfigurationException(
                    "No fencer configured for " + this);
        }
    }

    @Override
    public NodeFencer getFencer() {
        return fencer;
    }

    @Override
    public String toString() {
        return "NameNode at " + (lifelineAddr != null ? lifelineAddr : addr);
    }

    public String getNameServiceId() {
        return this.nsId;
    }

    public String getNameNodeId() {
        return this.nnId;
    }

    @Override
    protected void addFencingParameters(Map<String, String> ret) {
        super.addFencingParameters(ret);

        ret.put(NAMESERVICE_ID_KEY, getNameServiceId());
        ret.put(NAMENODE_ID_KEY, getNameNodeId());
    }

    @Override
    public boolean isAutoFailoverEnabled() {
        return autoFailoverEnabled;
    }

    @Override
    public boolean supportObserver() {
        return true;
    }
}
