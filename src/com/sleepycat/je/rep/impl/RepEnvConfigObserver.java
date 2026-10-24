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

package com.sleepycat.je.rep.impl;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.rep.ReplicationMutableConfig;

/**
 * Implemented by observers of mutable rep config changes.
 */
// 高可用复制模块内部（rep.impl）的一个可变复制环境配置变更观察者接口。
// 实现高可用（HA）复制配置的动态热更新监听：类似于基础存储引擎中的 EnvConfigObserver，该接口专门用于复制环境（Replication Environment）。
// 解耦组件与配置变更：允许 JE 内部的复制节点、网络通信层、状态机或心跳服务注册成为观察者。当管理员在运行时动态调整了复制相关的可变属性（例如节点优先级、超时时间、复制组参数等通过 ReplicationMutableConfig 修改的属性）时，系统能够通过该接口实时通知所有观察者，从而使相关底层组件动态刷新内部状态，而无需重启高可用复制节点。
public interface RepEnvConfigObserver {

    /**
     * Notifies the observer that one or more mutable rep properties have been
     * changed.
     */
    // 通知观察者：一个或多个可变的高可用复制配置属性已经被成功修改。
    void repEnvConfigUpdate(RepConfigManager configMgr,
                            ReplicationMutableConfig newConfig)
        throws DatabaseException;
}
