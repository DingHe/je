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

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * The interface for Consistency policies used to provide consistency
 * guarantees at a Replica. ReplicaConsistencyPolicies are only used by
 * Berkeley DB JE High Availability.
 * <p>
 * A transaction initiated at a Replica will wait in
 * the {@link com.sleepycat.je.Environment#beginTransaction} method until the
 * consistency policy is satisfied.
 * Consistency policies are specified at either a per-transaction level through
 * {@link TransactionConfig#setConsistencyPolicy} or as an replication node
 * wide default through {@link
 * com.sleepycat.je.rep.ReplicationConfig#setConsistencyPolicy}
 *
 * @see <a href="{@docRoot}/../ReplicationGuide/consistency.html"
 * target="_top">Managing Consistency</a>
 */
// ReplicaConsistencyPolicy 是 Berkeley DB Java Edition (JE) 高可用（HA）复制模块中副本一致性策略的顶层接口。
// 核心作用体现在：
// 定义副本（Replica）读取的一致性保证：在高可用分布式环境中，由于主备网络传输存在延迟，副本节点的数据可能落后于主节点。该接口用于为在副本节点上发起的事务（特别是只读事务）设定一致性约束。
// 控制事务开启的阻塞行为：当客户端在副本节点调用 Environment.beginTransaction() 开启事务时，JE 会执行该策略。如果当前副本的数据落后于策略要求的界限，事务会在 beginTransaction() 方法中阻塞等待，直到副本追赶上主节点的进度并满足一致性条件为止。
// 策略配置的统一抽象：可以通过事务级别的 TransactionConfig.setConsistencyPolicy() 针对特定事务进行定制，也可以通过复制节点级别的 ReplicationConfig.setConsistencyPolicy() 设置全局默认策略。常见的具体实现类包括 TimeConsistencyPolicy（基于时间滞后的一致性）和 CommitPointConsistencyPolicy（基于提交点的一致性）等。
public interface ReplicaConsistencyPolicy {

    /**
     * @hidden
     * For internal use only.
     *
     * Ensures that the replica is within the constraints specified by this
     * policy. If it isn't the method waits until the constraint is satisfied
     * by the replica.
     *
     * @param repInstance identifies the replicated environment that must meet
     * this consistency requirement.
     */
    // 用于确保当前的副本环境满足该策略指定的一致性约束。
    public void ensureConsistency(EnvironmentImpl repInstance)
        throws InterruptedException;

    /**
     * Returns the name used to identify the policy. The name is used when
     * constructing policy property values for use in je.properties files.
     */
    // 返回用于标识该策略的名称字符串。
    public String getName();

    /**
     * The timeout associated with the consistency policy. If consistency
     * cannot be established by the Replica within the timeout period, a {@link
     * com.sleepycat.je.rep.ReplicaConsistencyException} is thrown by {@link
     * com.sleepycat.je.Environment#beginTransaction}.
     *
     * @return the timeout associated with the policy
     */
    // 获取与当前一致性策略关联的超时时间
    public long getTimeout(TimeUnit unit);
}
