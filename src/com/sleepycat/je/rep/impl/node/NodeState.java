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
package com.sleepycat.je.rep.impl.node;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * NodeState encapsulates the current replicator state, and the ability to wait
 * for state transition and fire state change notifications.
 */
// 封装复制节点状态：NodeState 负责封装和维护复制节点当前的生命周期状态（如 MASTER、REPLICA、DETACHED、UNKNOWN）。
// 状态变更与通知管理：它提供了线程安全的状态切换机制（changeAndNotify），能够在节点状态发生跃迁时，记录日志、更新当前线程名称，并触发应用程序注册的监听器（StateChangeListener），通知上层业务节点角色的转变。
public class NodeState {
    /* The rep impl whose state is being tracked. */
    // 被跟踪状态的底层复制实现对象（Replication Implementation），提供核心复制控制逻辑和环境上下文。
    private final RepImpl repImpl;

    /* The application registered state change listener for this node. */
    // 应用程序注册的监听器，用于监听并接收该节点的状态变更事件。
    private StateChangeListener stateChangeListener = null;

    /* The state change event that resulted in the current state. */
    // 记录最近一次导致当前状态变更的事件对象（包含新状态和对应的主节点信息）。
    private StateChangeEvent stateChangeEvent = null;
    // 使用原子引用封装的当前复制环境状态（ReplicatedEnvironment.State），初始值默认为 DETACHED（未连接/游离状态）。
    private final AtomicReference<ReplicatedEnvironment.State> currentState;
    private final Logger logger;
    // 用于记录节点状态转换日志的标准日志记录器。
    private final NameIdPair nameIdPair;

    public NodeState(NameIdPair nameIdPair,
                     RepImpl repImpl) {

        currentState = new AtomicReference<ReplicatedEnvironment.State>
            (ReplicatedEnvironment.State.DETACHED);
        this.nameIdPair = nameIdPair;
        this.repImpl = repImpl;
        logger = LoggerUtils.getLogger(getClass());
    }
    // 设置（注册）状态变更监听器。
    // 线程安全地为当前节点指定一个应用层的 StateChangeListener。
    synchronized public
        void setChangeListener(StateChangeListener stateChangeListener){
        this.stateChangeListener = stateChangeListener;
    }

    synchronized public StateChangeListener getChangeListener() {
        return stateChangeListener;
    }

    /**
     * Change to a new node state and release any threads waiting for a state
     * transition.
     */
    // 将节点切换到新状态，并触发应用层通知及线程名称更新。
    synchronized public void changeAndNotify(ReplicatedEnvironment.State state,
                                             NameIdPair masterNameId) {

        ReplicatedEnvironment.State newState = state;
        ReplicatedEnvironment.State oldState = currentState.getAndSet(state);
        stateChangeEvent = new StateChangeEvent(state, masterNameId);

        LoggerUtils.info(logger, repImpl,
                         "node:" + masterNameId +
                         " state change from " + oldState + " to " + newState);

        if (stateChangeListener != null) {
            try {
                stateChangeListener.stateChange(stateChangeEvent);
            } catch (Exception e) {
                LoggerUtils.severe(logger, repImpl,
                                   "State Change listener exception" +
                                   e.getMessage());
                throw new EnvironmentFailureException
                    (repImpl, EnvironmentFailureReason.LISTENER_EXCEPTION, e);
            }
        }

        /* Make things obvious in thread dumps */
        Thread.currentThread().setName(currentState + " " + nameIdPair);
    }
    // 获取当前的复制环境状态。
    synchronized public ReplicatedEnvironment.State getRepEnvState() {
        return currentState.get();
    }

    // 获取最近一次的状态变更事件对象。
    synchronized public StateChangeEvent getStateChangeEvent() {
        return stateChangeEvent;
    }
}
