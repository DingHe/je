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
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * @hidden
 * Interface for creating DataChannel instances.
 */
// 数据通道工厂接口：DataChannelFactory 负责创建和管理 DataChannel 实例。DataChannel 是 BDB JE 用于节点间加密、安全传输或普通网络通信的抽象数据通道。
// 统一连接管理与配置：它定义了两种核心能力：一是主动发起连接（通过指定目标地址和连接选项连接到远程节点），二是被动接受连接（将新接收到的底层 SocketChannel 包装并适配为 BDB JE 的 DataChannel）。
// 支持定制化网络行为：通过内部类 ConnectOptions，它允许开发者对底层 Socket 链接的各项参数（如超时时间、缓冲区大小、阻塞模式、Nagle 算法等）进行精细化配置。
public interface DataChannelFactory {
    /**
     * Creates a DataChannel from an newly accepted socketChannel
     *
     * @param socketChannel the newly accepted SocketChannel
     * @return an implementation of DataChannel that wraps the
     *         input SocketChannel
     */
    // 接受并包装一个新连接的套接字通道。
    // SocketChannel socketChannel：服务端新接收到的底层标准 SocketChannel。
    DataChannel acceptChannel(SocketChannel socketChannel);

    /**
     * A collection of options to apply to a connection.
     */
    // 用于封装连接选项的静态内部类，所有的 set* 方法都返回当前对象本身（支持链式调用）。
    public class ConnectOptions {
        // 是否启用 TCP_NODELAY 选项。默认值为 false。
        private boolean tcpNoDelay = false;
        // 套接字接收缓冲区大小（字节）。默认值为 0（表示使用系统默认值）。
        private int receiveBufferSize = 0;
        // 建立连接时的超时时间（毫秒）。默认值为 0（无自定义超时/系统默认）。
        private int openTimeout = 0;
        private int readTimeout = 0;
        // 是否使用阻塞式 I/O 模式。默认值为 true。
        private boolean blocking = true;
        // 是否启用 SO_RESUEADDR（地址复用）选项。默认值为 false。
        private boolean reuseAddr = false;

        /**
         * Creates a base set of connection options. The default values
         * for the options are:
         * <pre>
         *   tcpNoDelay = false
         *   receiveBufferSize = 0
         *   openTimeout = 0
         *   readTimeout = 0
         *   blocking = true
         *   reuseAddr = false
         * </pre>
         */
        public ConnectOptions() {
        }

        /**
         * Sets the tcpNoDelay option for the connection.
         *
         * @param tcpNoDelay if true, disable the Nagle algorithm for delayed
         * transmissions on connection
         * @return this instance
         */
        public final ConnectOptions setTcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        /**
         * Gets the tcpNoDelay option for the connection.
         *
         * @return true if the tcpNoDelay option is enabled
         */
        public final boolean getTcpNoDelay() {
            return this.tcpNoDelay;
        }

        /**
         * Sets the connection receive buffer size for the connection.
         *
         * @param rcvBufferSize the desired size of the receive buffer, or
         * 0 to use system defaults.
         * @return this instance
         */
        public final ConnectOptions setReceiveBufferSize(int rcvBufferSize) {
            this.receiveBufferSize = rcvBufferSize;
            return this;
        }

        /**
         * Gets the connection receive buffer size for the connection.
         *
         * @return the configured receive buffer size option
         */
        public final int getReceiveBufferSize() {
            return this.receiveBufferSize;
        }

        /**
         * Sets the connection open timeout value for the connection.
         *
         * @param timeout the desired timeout value for connection initiation
         * in milliseconds, or 0 if system defaults should be used
         * @return this instance
         */
        public final ConnectOptions setOpenTimeout(int timeout) {
            this.openTimeout = timeout;
            return this;
        }

        /**
         * Gets the connection open timeout value for the connection.
         *
         * @return the configured timeout value
         */
        public final int getOpenTimeout() {
            return this.openTimeout;
        }

        /**
         * Sets the read timeout value for the connection.
         *
         * @param timeout the desired timeout value for read operations in
         * milliseconds, or 0 if system defaults should be used
         * @return this instance
         */
        public final ConnectOptions setReadTimeout(int timeout) {
            this.readTimeout = timeout;
            return this;
        }

        /**
         * Gets the read timeout value for the connection.
         *
         * @return the configured timeout value
         */
        public final int getReadTimeout() {
            return this.readTimeout;
        }

        /**
         * Sets the blocking mode option for the connection.
         *
         * @param blocking if true, the connection will use blocking mode IO
         * @return this instance
         */
        public final ConnectOptions setBlocking(boolean blocking) {
            this.blocking = blocking;
            return this;
        }

        /**
         * Gets the blocking mode option for the connection.
         *
         * @return  the blockingMode configuration setting
         */
        public final boolean getBlocking() {
            return this.blocking;
        }

        /**
         * Sets the reuseAddr option for the connection.
         *
         * @param reuseAddr if true, enable the SO_REUSEADDR option on the
         * underlying socket
         * @return this instance
         */
        public final ConnectOptions setReuseAddr(boolean reuseAddr) {
            this.reuseAddr = reuseAddr;
            return this;
        }

        /**
         * Gets the reuseAddr option for the connection.
         *
         * @return the setting of the reuseAddr option
         */
        public final boolean getReuseAddr() {
            return this.reuseAddr;
        }

        /**
         * Generates a String representation of the object.
         */
        @Override
        public String toString() {
            return "ConnectOptions[" +
                "tcpNoDelay = " + tcpNoDelay +
                ", receiveBufferSize = " + receiveBufferSize +
                ", openTimeout = " + openTimeout +
                ", readTimeout = " + readTimeout +
                ", blocking = " + blocking +
                ", reuseAddr = " + reuseAddr +
                "]";
        }
    };

    /**
     * Creates a DataChannel that connects to the specified address,
     * with the specified options.
     *
     * @param addr The address to which the connection should be made.
     *        It is possible for a DataChannelFactory implementation to
     *        proxy this connection through an intermediary.
     * @param connectOptions the collection of connection options to be
     *        applied to the connection.
     * @return A DataChannel connected to the the specified address.
     */
    // 连接到指定的网络地址，并应用相应的连接配置选项。
    // InetSocketAddress addr：目标节点的网络地址（IP 和端口）。工厂的实现也可以通过中间代理来转发该连接。
    DataChannel connect(InetSocketAddress addr,
                        ConnectOptions connectOptions)
        throws IOException;
}
