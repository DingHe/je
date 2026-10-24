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

package com.sleepycat.je.rep.net;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SocketChannel;

/**
 * @hidden
 * An interface that associates a delegate socketChannel for network I/O, which
 * provides ByteChannel, GatheringByteChannel, and ScatteringByteChannel,
 * interfaces for callers.
 */
// 统一的网络数据通道抽象：DataChannel 接口将底层的 SocketChannel 与上层应用逻辑进行了解耦和包装。它继承了 Java NIO 的 ByteChannel、GatheringByteChannel 和 ScatteringByteChannel 接口，支持标准的字节读写以及分散/聚集（Scatter/Gather）I/O 操作。
// 安全与信任机制支持：除了基础的字节传输能力外，它还提供了判断通道是否加密（isSecure）、是否具备信任判断能力（isTrustCapable）以及对端是否受信任（isTrusted）的方法，非常适合用于实现支持 SSL/TLS 等安全加密传输的复制通道。
// 支持缓冲区冲刷控制：针对可能存在的非阻塞加密通道或缓存写入场景，提供了 flush() 方法及 FlushStatus 状态机，用于精确控制挂起数据的冲刷。
public interface DataChannel extends ByteChannel,
                                     GatheringByteChannel,
                                     ScatteringByteChannel {

    /**
     * Accessor for the underlying SocketChannel.
     * Callers may used the returned SocketChannel in order to query/modify
     * connections attributes, but may not directly close, read from or write
     * to the SocketChannel.
     *
     * @return the socket channel underlying this data channel instance
     */
    // 获取底层的 SocketChannel 代理。
    public SocketChannel getSocketChannel();

    /**
     * Checks whether the channel encrypted.
     *
     * @return true if the data channel provides network privacy
     */
    // 检查通道是否经过加密。
    public boolean isSecure();

    /**
     * Checks whether  the channel capable of determining peer trust.
     *
     * @return true if the data channel implementation has the capability
     * to determine trust.
     */
    // 检查通道是否具备确定对端信任（Peer Trust）的能力。
    // 如果当前数据通道的实现支持证书校验或身份认证机制来判断对端是否可信，则返回 true。
    public boolean isTrustCapable();

    /**
     * Checks whether the channel peer is trusted.
     *
     * @return true if the channel has determined that the peer is trusted.
     */
    // 检查通道的对端是否已经被证实为受信任的。
    public boolean isTrusted();

    /**
     * The status of the flush method.
     */
    public enum FlushStatus {

        /** Flushes are not being used. */
        // 未启用冲刷机制（通常在普通明文通道中不需要特殊的冲刷步骤）
        DISABLED,

        /** Nothing needs to be flushed. */
        // 冲刷完成（没有任何需要被冲刷的数据）。
        DONE,

        /** Flush not complete because there is something left to flush. */
        // 冲刷未完成，因为还有剩余的数据需要继续冲刷。
        AGAIN,

        /** Flush not complete because socket is busy for write. */
        // 冲刷未完成，因为底层 Socket 当前处于写忙碌状态。
        WRITE_BUSY,

        /** Flush not complete due to read data dependency. */
        // 冲刷未完成，由于依赖于先读取更多数据（常见于 SSL 握手或双向交互协议中）。
        NEED_READ,

        /** Flush not complete due to task execution dependency. */
        // 冲刷未完成，由于依赖于某些后台任务的执行。
        NEED_TASK,
    }

    /**
     * Attempt to flush any pending writes to the underlying socket buffer.
     * The caller should ensure that it is the only thread accessing the
     * DataChannel in order that the return value be meaningful.
     *
     * @return the flush status
     */
    // 尝试将任何挂起的写入内容冲刷（Flush）到底层 Socket 缓冲区中。
    public FlushStatus flush() throws IOException;
}

