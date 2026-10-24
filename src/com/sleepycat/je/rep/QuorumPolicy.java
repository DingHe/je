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

package com.sleepycat.je.rep;

import com.sleepycat.je.EnvironmentFailureException;

/**
 * The quorum policy determine the number of nodes that must participate to
 * pick the winner of an election, and therefore the master of the group.
 * The default quorum policy during the lifetime of the group is
 * QuorumPolicy.SIMPLE_MAJORITY. The only time that the application needs to
 * specify a specific quorum policy is at node startup time, by passing one
 * to the {@link ReplicatedEnvironment} constructor.
 *
 * <p>Note that {@link NodeType#SECONDARY} nodes are not counted as part of
 * master election quorums.
 */
// QuorumPolicy 是 Berkeley DB Java Edition (BDB JE) 高可用复制（Replication）模块中的核心枚举类。
// 决定选举仲裁策略（Quorum Policy）：QuorumPolicy 用于定义在复制组（Replication Group）进行主节点选举（Master Election）时，必须有多少个节点参与并投票才能选出赢家（即新的主节点）
// 防止脑裂（Split-Brain）：通过设定法定人数的阈值，确保在网络分区或节点故障时，只有获得足够多数派同意的子网才能选出主节点，从而保证数据一致性。
// 节点启动配置：复制组在生命周期中默认使用 QuorumPolicy.SIMPLE_MAJORITY。应用程序通常仅在节点启动时（通过 ReplicatedEnvironment 构造函数）需要显式指定特定的法定人数策略。
// 注意事项：NodeType#SECONDARY（辅助/次要节点）不计入主节点选举的法定人数中。
public enum QuorumPolicy {

    /**
     * All participants are required to vote.
     */
    // 全员投票策略。
    // 要求复制组中的所有投票参与者都必须参与投票并达成一致。这种策略对节点存活率要求极高（只要有一个节点挂掉就无法选出主节点），通常用于对可用性要求不高、但对强一致性或特定测试场景有要求的环境。
    ALL,

     /**
      *  A simple majority of participants is required to vote.
      */
    // 简单多数派投票策略（默认策略）
    // 要求复制组中必须有简单多数的参与者投票（即超过半数）。这是生产环境中推荐的经典高可用策略，能够容忍少数节点故障的同时保证系统继续提供服务。
    SIMPLE_MAJORITY;

    /**
     * Returns the minimum number of nodes to needed meet the quorum policy.
     *
     * @param groupSize the number of election participants in the replication
     *        group
     *
     * @return the number of nodes that are needed for a quorum for a group
     *         with {@code groupSize} number of election participants
     */
    public int quorumSize(int groupSize) {
        switch (this) {
            case ALL:
                return groupSize;

            case SIMPLE_MAJORITY:
                return (groupSize / 2 + 1);

            default:
                throw EnvironmentFailureException.unexpectedState
                    ("Unknown quorum:" + this);
        }
    }
}
