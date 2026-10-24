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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;

/**
 * A class that implements Loggable knows how to read and write itself into
 * a ByteBuffer in a format suitable for the JE log or JE replication
 * messages.
 *
 * <p>Classes that implement {@code Loggable} and are included in replication
 * data should implement {@code VersionedWriteLoggable}.
 */
// 为所有需要持久化到 JE 物理日志文件（Log）或作为网络复制消息（Replication Message）进行传输的对象，定义统一的二进制序列化与反序列化契约。
// 任何实现了 Loggable 接口的类，都具备了“自我读写二进制缓冲区”的能力。
// 统一的二进制序列化/反序列化标准：JE 的底层日志系统（Log System）需要将各种各样的数据库操作、事务记录、节点状态等持久化到磁盘日志中。Loggable 规定了这些对象如何高效地写入字节缓冲区（Buffer）以及如何从缓冲区中还原。
// 支持复制数据流传输：在分布式复制组中，许多需要在节点间同步的数据结构也通过实现该接口，从而能够方便地打包进网络传输层
// 日志审计与调试支持：提供了转储（Dump）能力，支持将二进制日志内容转换为人类可读或机器可解析的 XML 格式，方便进行底层排查和测试。
public interface Loggable {

    /*
     * Writing to a byte buffer
     */

    /**
     * @return number of bytes used to store this object.
     */
    // 计算并返回当前对象在序列化后所需的字节总数（Byte Size）
    // 在将对象写入缓冲区之前，JE 的日志系统通常需要预先知道该对象会占用多大的空间，以便分配足够的缓冲区容量或在日志文件中精准规划偏移量。
    public int getLogSize();

    /**
     * Serialize this object into the buffer.
     * @param logBuffer is the destination buffer
     */
    // 将当前对象的内部状态序列化（Serialize）并写入到指定的字节缓冲区（ByteBuffer）中。
    // 对象需要按照既定的二进制格式，把自己的各个字段（如整型、长整型、字符串、字节数组等）逐一写入 logBuffer，通常与 getLogSize() 的计算结果严格对应。
    public void writeToLog(ByteBuffer logBuffer);

    /*
     *  Reading from a byte buffer
     */

    /**
     * Initialize this object from the data in itemBuf.
     * @param itemBuffer the source buffer
     * @param entryVersion the log version of the data
     */
    // 从给定的源字节缓冲区（itemBuffer）中读取数据，并反序列化（Deserialize）初始化当前对象。
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion);

    /**
     * Write the object into the string buffer for log dumping. Each object
     * should be dumped without indentation or new lines and should be valid
     * XML.
     * @param sb destination string buffer
     * @param verbose if true, dump the full, verbose version
     */
    // 将对象的内部状态导出为字符串形式，用于日志转储（Log Dumping）或调试分析。
    public void dumpLog(StringBuilder sb, boolean verbose);

    /**
     * @return the transaction id embedded within this loggable object. Objects
     * that have no transaction id should return 0.
     */
    // 返回嵌入在该 Loggable 对象内部的事务 ID（Transaction ID）
    // 如果该对象与某个特定的数据库事务紧密关联（例如事务提交、锁记录等），则返回对应的事务 ID；对于与事务无关的普通日志条目，该方法应当返回 0。
    public long getTransactionId();

    /**
     * @return true if these two loggable items are logically the same.
     * Used for replication testing.
     */
    // 判断当前对象与另一个 Loggable 对象在业务逻辑上是否完全相同。
    public boolean logicalEquals(Loggable other);
}
