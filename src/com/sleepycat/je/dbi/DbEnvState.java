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


/**
 * DbEnvState implements a typesafe enumeration of environment states
 * and does state change validation.
 */
// 定义环境状态：通过常量规范数据库环境可能处于的各种生命周期阶段（初始化、已打开、已关闭、已失效）。
// 状态变迁控制（State Validation）：通过 checkState 方法，确保环境状态的改变符合预定的业务逻辑，防止非法状态跃迁（例如：从一个不合法的状态直接执行关闭操作）。
class DbEnvState {
    private static final boolean DEBUG = false;
    // 存储当前状态的字符串描述（如 "initialized"、"open" 等）。它主要用于日志记录、调试输出以及抛出异常时的错误提示信息。
    private String name;

    /* Valid environment states. */
    // 表示环境的初始化状态。当 Environment 对象刚被创建、尚未正式打开时所处的状态。
    public static final DbEnvState INIT = new DbEnvState("initialized");
    // 表示环境的已打开状态。此时环境已经成功加载、配置并可以正常处理读写请求。
    public static final DbEnvState OPEN = new DbEnvState("open");
    // 表示环境的已关闭状态。此时环境资源已被释放，无法再执行任何数据库操作。
    public static final DbEnvState CLOSED = new DbEnvState("closed");
    // 表示环境的已失效/损坏状态。通常是因为发生了严重的内部错误，导致该环境实例无法继续安全使用。
    public static final DbEnvState INVALID = new DbEnvState("invalid");

    /* Valid previous states, for state transition checking. */
    public static final DbEnvState[] VALID_FOR_CLOSE =  {INIT, OPEN, INVALID};
    /* Not currently used:
    public static final DbEnvState[] VALID_FOR_OPEN =   {INIT, CLOSED};
    public static final DbEnvState[] VALID_FOR_REMOVE = {INIT, CLOSED};
    */

    DbEnvState(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /* Check for valid state transitions. */
    void checkState(DbEnvState[] validPrevStates, DbEnvState newState) {
        if (DEBUG) {
            System.out.println("newState = " + newState +
                               " currentState = " + name);
        }
        boolean transitionOk = false;
        for (int i = 0; i < validPrevStates.length; i++) {
            if (this == validPrevStates[i]) {
                transitionOk = true;
                break;
            }
        }
        if (!transitionOk) {
            throw new IllegalStateException
                ("Can't go from environment state " + toString() +
                 " to " + newState.toString());
        }
    }
}
