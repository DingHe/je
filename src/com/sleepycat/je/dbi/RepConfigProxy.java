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

package com.sleepycat.je.dbi;

import com.sleepycat.je.ReplicaConsistencyPolicy;

/**
 * Used to pass a replication configuration instance through the non-HA code.
 */
// 用于获取当前复制环境或事务所配置的副本一致性策略。
// 在分布式复制集群中，当客户端在 Replica（副本）节点上发起读操作时，一致性策略决定了该 Replica 必须追赶主节点（Master）到什么程度（例如：允许的最大时间滞后、事务提交序号差距等），当前读请求才被允许执行。
public interface RepConfigProxy {
    public ReplicaConsistencyPolicy getConsistencyPolicy();
}
