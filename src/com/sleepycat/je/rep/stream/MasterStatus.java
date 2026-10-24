/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */
package com.sleepycat.je.rep.stream;

import java.net.InetSocketAddress;

import com.sleepycat.je.rep.impl.node.NameIdPair;

/**
 * Class used by a node to track changes in Master Status. It's updated by
 * the Listener. It represents the abstract notion that the notion of the
 * current Replica Group is definitive and is always in advance of the notion
 * of a master at each node. A node is typically playing catch up as it tries
 * to bring its view in line with that of the group.
 */
// MasterStatus 类是一个用于跟踪和管理复制组中主节点（Master）状态的核心工具类。
// 复制组层面（Group's perspective）对主的认知是权威的、超前的，而节点自身层面（Node's localized perspective）对主的认知可能会滞后。
// 节点在运行过程中，通常需要不断追赶（Catch up），把自己的本地主状态同步到与复制组一致的状态。
// 类的核心作用
// 双视角状态管理：同时维护两套主节点信息：
// 全局群组视角（Group Master）：通过选举通知确定的最新、最具权威的主节点信息。
// 本地节点视角（Node Master）：当前节点实际感知并正在交互的主节点信息（可能因网络延迟或未同步而滞后）。
// 状态同步与校验：提供了一系列方法来检查群组主与节点主是否一致（inSync）、执行同步（sync）、重置同步（unSync）以及断言同步状态（assertSync），从而保证复制流的正确消费。
// 线程安全：类中的关键状态修改和查询方法均加了 synchronized 关键字，确保在多线程并发（如心跳、选举通知、复制流重放）环境下的线程安全。
public class MasterStatus implements Cloneable {

    /* This node's identity */
    // 当前节点自身的身份标识（包含节点名称与内部数字 ID）。
    private final NameIdPair nameIdPair;

    /* The current master resulting from election notifications */
    // 从全局群组/选举通知中获悉的主节点主机名（Host Name）。
    private String groupMasterHostName = null;
    // 从全局群组/选举通知中获悉的主节点网络端口。
    private int groupMasterPort = 0;
    /* The node ID used to identify the master. */
    // 标识群组主的名称与 ID 对（初始为 NameIdPair.NULL）。
    private NameIdPair groupMasterNameId = NameIdPair.NULL;

    /*
     * The Master as implemented by the Node. It can lag the groupMaster
     * as the node tries to catch up.
     */
    // 当前节点本地所认为的主节点主机名（可能滞后于 groupMasterHostName）。
    private String nodeMasterHostName = null;
    // 当前节点本地所认为的主节点网络端口。
    private int nodeMasterPort = 0;
    // 当前节点本地所认为的主节点的名称与 ID 对。
    private NameIdPair nodeMasterNameId = NameIdPair.NULL;

    public MasterStatus(NameIdPair nameIdPair) {
        this.nameIdPair = nameIdPair;
    }

    /**
     * Returns a read-only snapshot of the object.
     */
    @Override
    public synchronized Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            assert(false);
        }
        return null;
    }

    /**
     * Returns true if it's the master from the Group's perspective
     */
    // 判断当前节点从群组的视角来看是否就是主节点。
    public synchronized boolean isGroupMaster() {
        final int id = nameIdPair.getId();
        return (id != NameIdPair.NULL_NODE_ID) &&
            (id == groupMasterNameId.getId());
    }

    /**
     * Returns true if it's the master from the node's localized perspective
     */
    // 判断当前节点从其本地局部的视角来看是否就是主节点。
    public synchronized boolean isNodeMaster() {
        final int id = nameIdPair.getId();
        return (id != NameIdPair.NULL_NODE_ID) &&
            (id == nodeMasterNameId.getId());
    }
    // 更新全局群组视角的主节点信息（通常由选举模块或监听器在选举产生新主时调用）。
    public synchronized void setGroupMaster(String hostname,
                                            int port,
                                            NameIdPair newGroupMasterNameId) {
        groupMasterHostName = hostname;
        groupMasterPort = port;
        groupMasterNameId = newGroupMasterNameId;
    }

    /**
     * Predicate to determine whether the group and node have a consistent
     * notion of the Master.
     *
     * @return false if the node does not know of a Master, or the group Master
     * is different from the node's notion the master.
     */
    // 判断群组主与节点主是否有一致的认知。
    public synchronized boolean inSync() {
        return !nodeMasterNameId.hasNullId() &&
               (groupMasterNameId.getId() == nodeMasterNameId.getId());
    }
    // 将本地节点的主状态清空或重置。
    public synchronized void unSync() {
        nodeMasterHostName = null;
        nodeMasterPort = 0;
        nodeMasterNameId = NameIdPair.NULL;
    }

    /**
     * An assertion form of the above. By combining the check and exception
     * generation in an atomic operation, it provides for an accurate exception
     * message.
     *
     * @throws MasterSyncException
     */
    // 以断言的形式检查主状态是否同步。
    public synchronized void assertSync()
        throws MasterSyncException {

        if (!inSync()) {
            throw new MasterSyncException();
        }
    }

    /**
     * Syncs to the group master
     */
    // 将本地节点的主状态对齐到群组主状态（Catch up）
    public synchronized void sync() {
        nodeMasterHostName = groupMasterHostName;
        nodeMasterPort = groupMasterPort;
        nodeMasterNameId = groupMasterNameId;
    }

    /**
     * Returns the Node's current idea of the Master. It may be "out of sync"
     * with the Group's notion of the Master
     */
    // 返回节点当前所认为的主节点的 Socket 地址。若本地主主机名为空则返回 null。
    public synchronized InetSocketAddress getNodeMaster() {
        if (nodeMasterHostName == null) {
            return null;
        }
        return new InetSocketAddress(nodeMasterHostName, nodeMasterPort);
    }
    // 返回本地节点主对应的 NameIdPair 标识对象。
    public synchronized NameIdPair getNodeMasterNameId() {
        return nodeMasterNameId;
    }

    /**
     * Returns a socket that can be used to communicate with the group master.
     * It can return null, if there is no current group master, that is,
     * groupMasterNameId is NULL.
     */
    // 返回从群组视角来看的主节点的 Socket 地址。若无当前群组主则返回 null
    public synchronized InetSocketAddress getGroupMaster() {
        if (groupMasterHostName == null) {
             return null;
        }
        return new InetSocketAddress(groupMasterHostName, groupMasterPort);
    }
    // 返回群组主对应的 NameIdPair 标识对象。
    public synchronized NameIdPair getGroupMasterNameId() {
        return groupMasterNameId;
    }

    @SuppressWarnings("serial")
    public class MasterSyncException extends Exception {
        private final NameIdPair savedGroupMasterId;
        private final NameIdPair savedNodeMasterId;

        MasterSyncException () {
            savedGroupMasterId = MasterStatus.this.getGroupMasterNameId();
            savedNodeMasterId = MasterStatus.this.getNodeMasterNameId();
        }

        @Override
        public String getMessage() {
            return "Master change. Node master id: " + savedNodeMasterId +
            " Group master id: " + savedGroupMasterId;
        }
    }
}
