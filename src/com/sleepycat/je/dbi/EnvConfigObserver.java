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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentMutableConfig;

/**
 * Implemented by observers of mutable config changes.
 */
// 存储引擎包（dbi）中的一个环境配置变更观察者接口
// 核心作用体现在：
// 实现配置动态修改的通知机制（Observer Pattern）：在 Berkeley DB 中，某些数据库环境配置属性是支持在运行时动态修改的（通过 EnvironmentMutableConfig）。该接口用于允许 JE 内部的各个子系统、缓存组件或功能模块注册成为观察者。
// 实时响应配置参数变更：当管理员或应用程序在运行时动态调整了环境配置（例如修改了缓存大小、日志参数等可变属性）时，配置管理器会触发通知，所有实现了该接口的观察者能够立刻收到回调，从而动态调整其内部行为或内存结构，实现“热更新”而无需重启数据库。
public interface EnvConfigObserver {

    /**
     * Notifies the observer that one or more mutable properties have been
     * changed.
     */
    // 通知观察者：一个或多个可变的环境配置属性已经被成功修改。
    // DbConfigManager configMgr：当前的数据库配置管理器实例。观察者可以通过它查询或获取底层的最新配置详情。
    // EnvironmentMutableConfig newConfig：包含了最新修改后的可变环境配置对象。观察者可以检查其中的具体属性值，以便应用相应的调整。
    void envConfigUpdate(DbConfigManager configMgr,
                         EnvironmentMutableConfig newConfig)
        throws DatabaseException;
}
