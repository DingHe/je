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

package com.sleepycat.je.log.entry;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.VLSN;

/**
 * A Log entry allows you to read, write and dump a database log entry.  Each
 * entry may be made up of one or more loggable items.
 *
 * <p>The log entry on disk consists of a log header defined by LogManager and
 * the specific contents of the log entry.
 *
 * <p>Log entries that support replication are required to implement {@link
 * ReplicableLogEntry}.
 */
// LogEntry 是 JE 日志子系统中的一个核心接口（Interface）
// 核心定位：它定义了在 JE 数据库日志（Log）中读取、写入、解析和转储（Dump）一条日志记录的标准行为与生命周期契约。
// 主要职责：
// 编组与解组：控制数据在内存对象与磁盘字节缓冲区（ByteBuffer）之间的相互转化。
// 多项组合：一条日志条目（Log Entry）可能由一个或多个可记录项（Loggable items）组成，该接口提供了统一的访问和解析入口。
// 复制与调试支持：提供逻辑相等性判断、复制流内容转储、事务关联、数据库 ID 获取以及日志落盘后的后续处理能力。
public interface LogEntry extends Cloneable {

    /**
     * Inform a LogEntry instance of its corresponding LogEntryType.
     */
    // 为当前的 LogEntry 实例关联并告知其对应的 LogEntryType（日志条目类型）。
    public void setLogType(LogEntryType entryType);

    /**
     * @return the type of log entry
     */
    // 获取当前日志条目对应的日志类型。
    public LogEntryType getLogType();

    /**
     * Read in a log entry.
     */
    // 从磁盘日志缓冲区中读取并反序列化一条日志条目。
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer);

    /**
     * Print out the contents of an entry.
     */
    // 将日志条目的内容格式化并输出到字符串构建器中，用于调试和日志查看（Dump）。
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose);

    /**
     * @return the first item of the log entry
     */
    // 获取该日志条目的核心主对象（Main Item）。
    // Object - 日志中封装的核心业务实体（例如树节点、LN 记录或事务条目）
    public Object getMainItem();

    /**
     * Construct a complete item from a item entry, fetching additional log
     * entries as needed to ensure that a usable main object is available.
     *
     * For an OldBINDeltaLogEntry, fetches the full BIN and merges the delta
     * information.  This is necessary to return a Node main object.
     * However, for the new BINDeltaLogEntry, the full BIN is not fetched,
     * since the partial BIN (the delta) is usable as a Node.
     */
    // 从条目构造一个完整且可用的主对象，必要时会从磁盘或缓存中抓取关联的附加日志条目。
    public Object getResolvedItem(DatabaseImpl dbImpl);

    /**
     * @return the ID of the database containing this entry, or null if this
     * entry type is not part of a database.
     */
    // 获取包含该日志条目的数据库 ID。
    public DatabaseId getDbId();

    /**
     * @return return the transaction id if this log entry is transactional,
     * 0 otherwise.
     */
    // 获取与该日志条目关联的事务 ID。
    public long getTransactionId();

    /**
     * @return size of byte buffer needed to store this entry.
     */
    // 计算并返回将该条目序列化到磁盘所需的字节缓冲区大小。
    public int getSize();

    /**
     * Serialize this object into the buffer.
     * @param logBuffer is the destination buffer
     */
    // 将当前日志条目对象序列化（Marshalling）写入到指定的字节缓冲区中。
    public void writeEntry(ByteBuffer logBuffer);

    /**
     * Returns true if this item should be counted as obsolete when logged.
     */
    // 判断当前日志项在被写入日志时，是否应当被直接视为过时（Obsolete）。
    public boolean isImmediatelyObsolete(DatabaseImpl dbImpl);

    /**
     * Returns whether this is a deleted LN.
     */
    // 判断该日志条目是否代表一条已被删除的记录（Deleted LN）。
    public boolean isDeleted();

    /**
     * Do any processing we need to do after logging, while under the logging
     * latch.
     */
    // 在日志成功落盘后、且处于日志写锁（Logging Latch）保护内部时，执行后续的收尾或处理工作。
    public void postLogWork(LogEntryHeader header,
                            long justLoggedLsn,
                            VLSN vlsn);

    /**
     * @return a shallow clone.
     */
    public LogEntry clone();

    /**
     * @return true if these two log entries are logically the same.
     * Used for replication.
     */
    public boolean logicalEquals(LogEntry other);

    /**
     * Dump the contents of the log entry that are interesting for
     * replication.
     */
    // 转储对复制（Replication）过程有意义的关键日志内容。
    public void dumpRep(StringBuilder sb);
}
