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

import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_BYTES_READ;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_READ;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.utilint.StatGroup;

// ReplicaFactory 是一个用于创建从节点（Replica）实例的工厂类。
// 通过工厂模式统一管理 Replica 对象的创建逻辑，并支持在测试或特殊场景下创建用于压测网络带宽的“空从节点”（Null Replica）
// 类的核心作用
// 对象创建管理：封装了 Replica 实例的创建过程，使上层调用者无需关心具体实例的初始化细节。
// 支持网络性能测试（Null Replica）：除了标准的默认从节点（DEFAULT）外，它还支持通过配置创建一种特殊的轻量级从节点。
// 这种特殊从节点不执行实际的数据库重放（Replay）逻辑，而是专门用来“吃掉”（吞噬）从主节点（Feeder）发来的网络消息，用于纯粹测试网络带宽和吞吐性能。
public class ReplicaFactory {
    // 用于指定当前工厂要创建的从节点类型。默认值为 ReplicaType.DEFAULT。
    private static ReplicaType type = ReplicaType.DEFAULT;
    // 用于控制 NULL_REPLICA 模式下打印网络统计信息的频率（即每隔多少条消息或操作输出一次带宽统计）。如果该值为 0，则不主动打印统计信息。
    private static long statsInterval = 0;

    public enum ReplicaType {
        // 标准的默认从节点类型，负责完整的复制流接收与数据重放（Replay）。
        DEFAULT,
        // 空从节点类型，专为网络带宽基准测试设计。
        NULL_REPLICA
    }

    public static void setReplicaType(ReplicaType t) {
        type = t;
    }

    public static void setStatsInterval(long interval) {
        statsInterval = interval;
    }

    public static Replica create(RepNode repNode, Replay replay) {
        switch (type) {
        case DEFAULT:
            // 如果当前类型是默认类型，直接返回标准的新建 Replica 实例（new Replica(repNode, replay)）。
            return new Replica(repNode, replay);

        case NULL_REPLICA:
        // 如果当前类型是测试用的空从节点，它会返回一个匿名的 Replica 子类实例。
            /**
             * Create a replica which just eats messages.  Used for testing
             * network bandwidth.
             */
            return
                new Replica(repNode, replay) {
                    @Override
                    protected void doRunReplicaLoopInternalWork()
                        throws Exception {

                        long ctime = System.currentTimeMillis();
                        long opCount = 0;
                        while (true) {
                            opCount++;
                            @SuppressWarnings("unused")
                            Message message =
                                getProtocol().read(getReplicaFeederChannel());
                            if (statsInterval > 0 &&
                                (opCount % statsInterval) == 0) {
                                StatGroup stats = getProtocol().
                                    getStats(StatsConfig.DEFAULT);
                                long bytesRead = 
                                    stats.getLong(N_BYTES_READ);
                                long messagesRead =
                                    stats.getLong(N_MESSAGES_READ);
                                long elapsedTime =
                                    System.currentTimeMillis() - ctime;
                                long bytesPerMilliSecond =
                                    bytesRead / elapsedTime;
                                System.out.println
                                    (" Bytes Read: " + bytesRead +
                                     " Messages Read: " + messagesRead +
                                     " BytesPerMSec: " + bytesPerMilliSecond +
                                     " MS: " + elapsedTime);
                                ctime = System.currentTimeMillis();
                            }
                        }

                    }
                };

        default:
            throw EnvironmentFailureException.unexpectedState
                ("unknown type passed to makeReplica: " + type);
        }
    }
}
