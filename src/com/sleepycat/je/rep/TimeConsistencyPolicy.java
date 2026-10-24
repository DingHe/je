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

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.Replica;
import com.sleepycat.je.utilint.PropUtil;

/**
 * A consistency policy which describes the amount of time the Replica is
 * allowed to lag the Master. The application can use this policy to ensure
 * that this node sees all transactions that were committed on the Master
 * before the lag interval.
 * <p>
 * Effective use of this policy requires that the clocks on the Master and
 * Replica are synchronized by using a protocol like NTP
 * <p>
 * Consistency policies are specified at either a per-transaction level through
 * {@link com.sleepycat.je.TransactionConfig#setConsistencyPolicy} or as an replication node
 * wide default through {@link
 * com.sleepycat.je.rep.ReplicationConfig#setConsistencyPolicy}
 *
 * @see <a href="{@docRoot}/../ReplicationGuide/consistency.html"
 * target="_top">Managing Consistency</a>
 */
// 高可用（HA）复制模块中基于时间滞后（Time Lag）的一致性策略实现类
// 核心作用体现在：
// 基于时间约束副本的一致性：它允许应用程序为副本（Replica）节点上的读写事务设定一个最大允许的时间滞后值（Permissible Lag）。
// 保证读取到指定时间点前提交的数据：当在副本节点上发起事务（如 Environment.beginTransaction）时，该策略确保副本节点已经重放（Replay）并同步了主节点（Master）在此滞后时间间隔之前提交的所有事务。
// 依赖时钟同步：为了让基于时间的滞后计算准确，主节点和副本节点的系统时钟必须通过 NTP 等协议保持合理同步。
public class TimeConsistencyPolicy implements ReplicaConsistencyPolicy {

    /**
     * The name:{@value} associated with this policy. The name can be used when
     * constructing policy property values for use in je.properties files.
     */
    // 定义该策略的唯一名称常量（值为 "TimeConsistencyPolicy"）。
    // 主要用于在属性文件（如 je.properties）中配置和解析策略。
    public static final String NAME = "TimeConsistencyPolicy";
    // 保存副本允许落后于主节点的最大时间滞后值（在内部统一转换为毫秒数存储）。
    private final int permissibleLag;

    /* Amount of time to wait (in ms) for the consistency to be reached. */
    // 保存等待达到一致性状态的超时时间
    // 如果在该时间内副本未能赶上进度，则会抛出 ReplicaConsistencyException。
    private final int timeout;

    /**
     * Specifies the amount of time by which the Replica is allowed to lag the
     * master when initiating a transaction. The Replica ensures that all
     * transactions that were committed on the Master before this lag interval
     * are available at the Replica before allowing a transaction to proceed
     * with Environment.beginTransaction.
     *
     * Effective use of this policy requires that the clocks on the Master and
     * Replica are synchronized by using a protocol like NTP.
     *
     * @param permissibleLag the time interval by which the Replica may be out
     * of date with respect to the Master when a transaction is initiated on
     * the Replica.
     *
     * @param permissibleLagUnit the {@code TimeUnit} for the permissibleLag
     * parameter.
     *
     * @param timeout the amount of time to wait for the consistency to be
     * reached.
     *
     * @param timeoutUnit the {@code TimeUnit} for the timeout parameter.
     *
     * @throws IllegalArgumentException if the permissibleLagUnit or
     * timeoutUnit is null.
     */
    // 接收允许的滞后时间数值及单位、超时时间数值及单位。
    public TimeConsistencyPolicy(long permissibleLag,
                                 TimeUnit permissibleLagUnit,
                                 long timeout,
                                 TimeUnit timeoutUnit) {
        this.permissibleLag = PropUtil.durationToMillis(permissibleLag,
                                                        permissibleLagUnit);
        this.timeout = PropUtil.durationToMillis(timeout, timeoutUnit);
    }

    /**
     * Returns the name:{@value #NAME}, associated with this policy.
     * @see #NAME
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Returns the allowed time lag associated with this policy.
     *
     * @param unit the {@code TimeUnit} of the returned value.
     *
     * @return the permissible lag time in the specified unit.
     */
    public long getPermissibleLag(TimeUnit unit) {
        return PropUtil.millisToDuration(permissibleLag, unit);
    }

    /**
     * Returns the consistency timeout associated with this policy.
     *
     * @param unit the {@code TimeUnit} of the returned value.
     *
     * @return the consistency timeout in the specified unit.
     */
    @Override
    public long getTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(timeout, unit);
    }

    /**
     * @hidden
     * For internal use only.
     * Ensures that the replica has replayed the replication stream to the
     * point identified by the lag period. If it isn't the method waits until
     * the constraint is satisfied by the replica.
     */
    // 内部核心一致性保障方法
    @Override
    public void ensureConsistency(EnvironmentImpl replicatorImpl)
        throws InterruptedException,
               ReplicaConsistencyException{

        /*
         * Cast is done to preserve replication/non replication code
         * boundaries.
         */
        // 将通用的环境实现转为复制专用的 RepImpl，获取副本节点对象 replica()
        RepImpl repImpl = (RepImpl) replicatorImpl;
        Replica replica = repImpl.getRepNode().replica();
        // 让当前线程进入阻塞等待，直到副本的复制流重放进度满足该时间滞后（permissibleLag）约束或超时
        replica.getConsistencyTracker().lagAwait(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + permissibleLag;
        result = prime * result + timeout;
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        TimeConsistencyPolicy other =
            (TimeConsistencyPolicy) obj;
        if (permissibleLag != other.permissibleLag) {
            return false;
        }
        if (timeout != other.timeout) {
            return false;
        }
        return true;
    }

    @Override
    public String toString(){
        return getName() + " permissibleLag=" + permissibleLag;
    }
}
