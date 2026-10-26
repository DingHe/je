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

package com.sleepycat.je.latch;

import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Provides information about a latch, to avoid requiring this information to
 * be stored with every latch object.  This is implemented by the IN class to
 * reduce memory usage.  LatchFactory provides a default implementation for
 * cases where creating an extra object is not an issue.
 */
// 其核心作用是为轻量级锁（Latch）提供元数据和上下文信息。
// 核心设计动机——节省内存：在数据库引擎中，通常需要创建大量的锁对象。如果在每一个 Latch 对象内部都存储它的名称、超时时间、调试跟踪表、环境引用等元数据，将会消耗可观的内存。JE 通过将这些信息剥离到 LatchContext 接口中，让具体的业务对象（如 B 树的内部节点 IN 类）来实现该接口，从而避免在每个 Latch 实例中重复存储这些字段，极大地优化了内存占用。
// 默认实现：对于那些创建额外对象成本不高、不需要绑定到复杂业务节点的场景，JE 提供了 LatchFactory 来提供默认的 LatchContext 实现。
public interface LatchContext {

    /** Returns EnvironmentParams.ENV_LATCH_TIMEOUT */
    // 获取该 Latch 的超时时间（以毫秒为单位）
    int getLatchTimeoutMs();

    /** Returns the latch name for debugging. */
    // 获取 Latch 的名称，主要用于调试（Debugging）和日志记录。
    String getLatchName();

    /** Returns LatchTable for debug/test tracking. */
    // 获取用于调试和测试跟踪的 LatchTable 实例。
    LatchTable getLatchTable();

    /** Returns envImpl, or may throw another exception in unit tests. */
    // 获取与当前 Latch 上下文关联的数据库环境实现实例（EnvironmentImpl），或在单元测试中抛出特定异常。
    EnvironmentImpl getEnvImplForFatalException();
}
