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

package com.sleepycat.je;

import java.util.StringTokenizer;

/**
 * Durability defines the overall durability characteristics associated with a
 * transaction. When operating on a local environment the durability of a
 * transaction is completely determined by the local {@link SyncPolicy} that is
 * in effect. When using replication, the overall durability is a function of
 * the local {@link SyncPolicy} plus the {@link ReplicaAckPolicy} used by the
 * master and the {@link SyncPolicy} in effect at each Replica.
 */
// Durability 是 Berkeley DB Java Edition (JE) 中定义事务持久性（Durability）特性的核心类。
// 在单机环境下，事务的持久性由本地的同步策略决定；
// 而在高可用（HA）复制环境中，整体持久性则由主节点的本地同步策略、副本节点的同步策略以及主节点所需的副本确认策略共同决定。
// 定义事务持久性级别：通过组合不同的本地日志刷盘策略（SyncPolicy）和网络确认策略（ReplicaAckPolicy），允许开发者在事务性能和数据安全性/持久性之间做出权衡。
// 支持复制环境确认：在分布式复制集群中，它决定了 Master 节点在提交事务时必须等待多少个 Replica 节点的确认（ACK），从而确保即使发生主备切换，已提交的数据也不会丢失。
public class Durability {

    /**
     * A convenience constant that defines a durability policy with COMMIT_SYNC
     * for local commit synchronization.
     *
     * The replicated environment policies default to COMMIT_NO_SYNC for
     * commits of replicated transactions that need acknowledgment and
     * SIMPLE_MAJORITY for the acknowledgment policy.
     */
    // 本地提交时强制同步刷盘（SYNC），并要求得到简单多数派（Simple Majority）副本的确认。
    public static final Durability COMMIT_SYNC =
        new Durability(SyncPolicy.SYNC,                // localSync
                       SyncPolicy.NO_SYNC,             // replicaSync
                       ReplicaAckPolicy.SIMPLE_MAJORITY);       // replicaAck

    /**
     * A convenience constant that defines a durability policy with
     * COMMIT_NO_SYNC for local commit synchronization.
     *
     * The replicated environment policies default to COMMIT_NO_SYNC for
     * commits of replicated transactions that need acknowledgment and
     * SIMPLE_MAJORITY for the acknowledgment policy.
     */
    // 本地提交时不进行同步刷盘（NO_SYNC，依赖后台异步刷盘或日志缓冲区），但仍要求多数派副本确认。这是复制环境的默认推荐策略，兼顾性能与高可用。
    public static final Durability COMMIT_NO_SYNC =
        new Durability(SyncPolicy.NO_SYNC,      // localSync
                       SyncPolicy.NO_SYNC,      // replicaSync
                       ReplicaAckPolicy.SIMPLE_MAJORITY);       // replicaAck

    /**
     * A convenience constant that defines a durability policy with
     * COMMIT_WRITE_NO_SYNC for local commit synchronization.
     *
     * The replicated environment policies default to COMMIT_NO_SYNC for
     * commits of replicated transactions that need acknowledgment and
     * SIMPLE_MAJORITY for the acknowledgment policy.
     */
    // 本地提交时将日志写入操作系统但不同步刷盘（WRITE_NO_SYNC），并要求多数派副本确认。
    public static final Durability COMMIT_WRITE_NO_SYNC =
        new Durability(SyncPolicy.WRITE_NO_SYNC,// localSync
                       SyncPolicy.NO_SYNC,      // replicaSync
                       ReplicaAckPolicy.SIMPLE_MAJORITY);       // replicaAck

    /**
     * A convenience constant that defines a durability policy, with
     * <code>ReplicaAckPolicy.NONE</code> for use with a read only transaction.
     * A read only transaction on a Master, using this Durability, will thus
     * not be held up, or throw <code>InsufficientReplicasException</code>, if
     * the Master is not in contact with a sufficient number of Replicas at the
     * time the transaction was initiated. </p>
     *
     * It's worth noting that since the transaction is read only, the sync
     * policies, although specified as <code>NO_SYNC</code>, do not really
     * matter.
     *
     * @deprecated use {@link TransactionConfig#setReadOnly} instead.
     */
    public static final Durability READ_ONLY_TXN =
        new Durability(SyncPolicy.NO_SYNC,      // localSync
                       SyncPolicy.NO_SYNC,      // replicaSync
                       ReplicaAckPolicy.NONE);  // replicaAck

    /**
     * Defines the synchronization policy to be used when committing a
     * transaction. High levels of synchronization offer a greater guarantee
     * that the transaction is persistent to disk, but trade that off for
     * lower performance.
     */
    // 事务提交同步策略
    // 定义了节点在提交事务时如何将日志写入磁盘：
    public enum SyncPolicy {

        /**
         *  Write and synchronously flush the log on transaction commit.
         *  Transactions exhibit all the ACID (atomicity, consistency,
         *  isolation, and durability) properties.
         *  <p>
         *  This is the default.
         */
        // 写入并同步刷盘（Fsync）。满足完整的 ACID 属性，数据最安全，但性能开销最大（默认值）。
        SYNC,

        /**
         * Do not write or synchronously flush the log on transaction commit.
         * Transactions exhibit the ACI (atomicity, consistency, and isolation)
         * properties, but not D (durability); that is, database integrity will
         * be maintained, but if the application or system fails, it is
         * possible some number of the most recently committed transactions may
         * be undone during recovery. The number of transactions at risk is
         * governed by how many log updates can fit into the log buffer, how
         * often the operating system flushes dirty buffers to disk, and how
         * often the log is checkpointed.
         */
        // 既不写入也不同步刷盘。满足 ACI，但不保证 D（持久性）。若系统崩溃，最近提交的事务可能会丢失。
        NO_SYNC,

        /**
         * Write but do not synchronously flush the log on transaction commit.
         * Transactions exhibit the ACI (atomicity, consistency, and isolation)
         * properties, but not D (durability); that is, database integrity will
         * be maintained, but if the operating system fails, it is possible
         * some number of the most recently committed transactions may be
         * undone during recovery. The number of transactions at risk is
         * governed by how often the operating system flushes dirty buffers to
         * disk, and how often the log is checkpointed.
         */
        // 写入操作系统缓冲区但不同步刷盘。若操作系统崩溃可能丢失数据，但能防范应用进程崩溃。
        WRITE_NO_SYNC
    };

    /**
     * A replicated environment makes it possible to increase an application's
     * transaction commit guarantees by committing changes to its replicas on
     * the network. ReplicaAckPolicy defines the policy for how such network
     * commits are handled.
     * <p>
     * The choice of a ReplicaAckPolicy must be consistent across all the
     * replicas in a replication group, to ensure that the policy is
     * consistently enforced in the event of an election.
     *
     * <p>Note that SECONDARY nodes are not included in the set of replicas
     * that must acknowledge transaction commits.
     */
    // 副本确认策略
    // 定义了 Master 节点提交事务时需要多少个可选举（ELECTABLE）Replica 的网络确认：
    public enum ReplicaAckPolicy {

        /**
         * All ELECTABLE replicas must acknowledge that they have committed the
         * transaction. This policy should be selected only if your replication
         * group has a small number of ELECTABLE replicas, and those replicas
         * are on extremely reliable networks and servers.
         */
        // 要求所有可选举 Replica 确认。适用于极小且网络极其可靠的集群。
        ALL,

        /**
         * No transaction commit acknowledgments are required and the master
         * will never wait for replica acknowledgments. In this case,
         * transaction durability is determined entirely by the type of commit
         * that is being performed on the master.
         */
        // 不需要任何确认，Master 提交不等待 Replica。持久性完全取决于 Master 本地。
        NONE,

        /**
         * A simple majority of ELECTABLE replicas must acknowledge that they
         * have committed the transaction. This acknowledgment policy, in
         * conjunction with an election policy which requires at least a simple
         * majority, ensures that the changes made by the transaction remains
         * durable if a new election is held.
         * <p>
         * This is the default.
         */
        // 要求简单多数派（$groupSize / 2 + 1$）的可选举 Replica 确认（默认值）。配合多数派选举策略，能保证故障转移后数据不丢失。
        SIMPLE_MAJORITY;

        /**
         * Returns the minimum number of ELECTABLE replicas required to
         * implement the ReplicaAckPolicy for a given replication group size.
         *
         * @param groupSize the number of ELECTABLE replicas in the replication
         * group
         *
         * @return the number of ELECTABLE replicas needed
         */
        // 根据当前复制组中可选举节点的总数（groupSize），计算并返回满足该确认策略所需的最少确认节点数。
        public int minAckNodes(int groupSize) {
            switch (this) {
            case ALL:
                return groupSize;
            case NONE:
                return 1;
            case SIMPLE_MAJORITY:
                return (groupSize / 2 + 1);
            default:
                throw EnvironmentFailureException.unexpectedState
                    ("Unknown ack policy: " + this);
            }
        }
    }

    /* The sync policy in effect on the local node. */
    private final SyncPolicy localSync;

    /* The sync policy in effect on a replica. */
    final private SyncPolicy replicaSync;

    /* The replica acknowledgment policy to be used. */
    final private ReplicaAckPolicy replicaAck;

    /**
     * Creates an instance of a Durability specification.
     *
     * @param localSync the SyncPolicy to be used when committing the
     * transaction locally.
     * @param replicaSync the SyncPolicy to be used remotely, as part of a
     * transaction acknowledgment, at a Replica node.
     * @param replicaAck the acknowledgment policy used when obtaining
     * transaction acknowledgments from Replicas.
     */
    public Durability(SyncPolicy localSync,
                      SyncPolicy replicaSync,
                      ReplicaAckPolicy replicaAck) {
        this.localSync = localSync;
        this.replicaSync = replicaSync;
        this.replicaAck = replicaAck;
    }

    /**
     * Parses the string and returns the durability it represents. The string
     * must have the following format:
     * <p>
     * <code>
     * <i>SyncPolicy</i>[,<i>SyncPolicy</i>[,<i>ReplicaAckPolicy</i>]]
     * </code>
     * <p>
     * The first SyncPolicy in the above format applies to the Master, and the
     * optional second SyncPolicy to the replica. Specific SyncPolicy or
     * ReplicaAckPolicy values are denoted by the name of the enumeration
     * value.
     * <p>
     * For example, the string:<i>sync,sync,quorum</i> describes a durability
     * policy where the master and replica both use {@link SyncPolicy#SYNC}
     * to commit transactions and {@link ReplicaAckPolicy#SIMPLE_MAJORITY} to
     * acknowledge a transaction commit.
     * <p>
     * {@link SyncPolicy#NO_SYNC}, is the default value for a node's
     * SyncPolicy.
     * <p>
     * {@link ReplicaAckPolicy#SIMPLE_MAJORITY} is the default for the
     * ReplicaAckPolicy.
     *
     * @param durabilityString the durability string in the above format
     *
     * @return the Durability resulting from the parse, or null if the
     * <code>durabilityString</code> argument was itself null.
     *
     * @throws IllegalArgumentException if the durabilityString is invalid.
     */
    public static Durability parse(String durabilityString) {
        if (durabilityString == null) {
            return null;
        }
        StringTokenizer tokenizer =
            new StringTokenizer(durabilityString.toUpperCase(), ",");

        if (!tokenizer.hasMoreTokens()) {
            throw new IllegalArgumentException
                ("Bad string format: " + '"' +  durabilityString + '"');
        }
        SyncPolicy localSync =
                SyncPolicy.valueOf(tokenizer.nextToken());
        SyncPolicy replicaSync = tokenizer.hasMoreTokens() ?
                SyncPolicy.valueOf(tokenizer.nextToken()) :
                SyncPolicy.NO_SYNC;
        ReplicaAckPolicy replicaAck = tokenizer.hasMoreTokens() ?
                ReplicaAckPolicy.valueOf(tokenizer.nextToken()) :
                ReplicaAckPolicy.SIMPLE_MAJORITY;
        return new Durability(localSync, replicaSync, replicaAck);
    }

    /**
     * Returns the string representation of durability in the format defined
     * by string form of the Durability constructor.
     *
     * @see #parse(String)
     */
    @Override
    public String toString() {
        return localSync.toString() + "," +
               replicaSync.toString() + "," +
               replicaAck.toString();
    }

    /**
     * Returns the transaction synchronization policy to be used locally when
     * committing a transaction.
     */
    public SyncPolicy getLocalSync() {
        return localSync;
    }

    /**
     * Returns the transaction synchronization policy to be used by the replica
     * as it replays a transaction that needs an acknowledgment.
     */
    public SyncPolicy getReplicaSync() {
        return replicaSync;
    }

    /**
     * Returns the replica acknowledgment policy used by the master when
     * committing changes to a replicated environment.
     */
    public ReplicaAckPolicy getReplicaAck() {
        return replicaAck;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((localSync == null) ? 0 : localSync.hashCode());
        result = prime * result
                + ((replicaAck == null) ? 0 : replicaAck.hashCode());
        result = prime * result
                + ((replicaSync == null) ? 0 : replicaSync.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Durability)) {
            return false;
        }
        Durability other = (Durability) obj;
        if (localSync == null) {
            if (other.localSync != null) {
                return false;
            }
        } else if (!localSync.equals(other.localSync)) {
            return false;
        }
        if (replicaAck == null) {
            if (other.replicaAck != null) {
                return false;
            }
        } else if (!replicaAck.equals(other.replicaAck)) {
            return false;
        }
        if (replicaSync == null) {
            if (other.replicaSync != null) {
                return false;
            }
        } else if (!replicaSync.equals(other.replicaSync)) {
            return false;
        }
        return true;
    }
}
