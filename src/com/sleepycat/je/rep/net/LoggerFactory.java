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

/**
 * The LoggerFactory interface provides a mechanism for obtaining logging
 * objects for use with dynamically constructed network objects.  Instances
 * of this interface are provided during object instantiation.
 */
// 网络组件的日志抽象工厂：LoggerFactory 接口的主要作用是为动态构建的网络对象（Dynamically constructed network objects）提供获取日志记录器（Logging objects）的机制。
// 解耦与插件化支持：在 BDB JE 的复制网络层中（例如自定义的 Socket 工厂、连接通道或安全传输层），网络对象往往是通过配置动态实例化的。通过引入 LoggerFactory 接口，这些动态网络组件能够在实例化时获得一个统一的日志记录方式，而无需硬编码依赖底层的日志实现，从而增强了网络层的灵活性、可测试性和可扩展性。
public interface LoggerFactory {

    /**
     * Obtains an InstanceLogger for the specified class.
     *
     * @param clazz the class for which a logger instance is to be obtained.
     * @return a logging object
     */
    public InstanceLogger getLogger(Class<?> clazz);
}
