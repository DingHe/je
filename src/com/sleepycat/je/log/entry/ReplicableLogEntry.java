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
import java.util.Collection;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.VersionedWriteLoggable;

/**
 * A sub-interface of {@link LogEntry} that must be implemented by all log
 * entries that can be replicated.  Replicable log entries are all those
 * entries for which the associated {@link LogEntryType}'s {@link
 * LogEntryType#isReplicationPossible} method returns {@code true}.  These are
 * the log entries that can be included in the replication stream distributed
 * from feeders to replicas during replication.  See [#22336].
 *
 * <p>Starting with the release using log version 9, as specified by {@link
 * LogEntryType#LOG_VERSION_REPLICATE_OLDER}, all replicable log entries
 * need to support writing themselves in earlier log formats, to support
 * replication during an upgrade when the master is replicated first.  Any
 * loggable objects that they reference should also implement {@link
 * com.sleepycat.je.log.VersionedWriteLoggable} for the same reason.
 *
 * <p>The {@link #getLastFormatChange} method identifies the log version for
 * which the entry's log format has most recently changed.  This information is
 * used to determine if the current log format is compatible with a
 * non-upgraded replica.
 *
 * <p>The {@link #getSize(int, boolean)} method overloading is used when
 * creating the buffer that will be used to transmit the log entry data in the
 * earlier format.
 *
 * <p>The {@link #writeEntry(ByteBuffer, int, boolean)} method overloading is
 * used to convert the in-memory format of the log entry into the log data in
 * the earlier format.
 *
 * <p>To simplify the implementation of writing log entries in multiple log
 * version formats, a log entry that needs to be written in a previous format
 * will first be read into its in-memory format in the current version, and
 * then written from there to the previous format.
 */
// 必须由所有可以被复制（Replicated）的日志条目实现。换句话说，当且仅当某条日志对应的 LogEntryType 允许复制时，该日志条目才需要实现此接口。
// 主要职责：
// 网络传输与滚动升级兼容：支持在主从节点版本不一致（例如主节点已升级、而副本节点尚未升级）的滚动升级（Rolling Upgrade）期间，将日志条目序列化并降级写入到较早的日志版本格式中，以便在网络中传输。
// 版本跟踪与多版本序列化：通过记录格式变更的版本号、管理嵌入式的可写对象，以及提供带版本参数的 getSize 和 writeEntry 重载方法，确保复制流在不同版本节点间能够被正确理解和重放。
public interface ReplicableLogEntry extends LogEntry {

    /**
     * Returns the log version of the most recent format change for this log
     * entry.
     *
     * @return the log version of the most recent format change
     */
    // 获取该日志条目最近一次格式发生改变时的日志版本号。
    int getLastFormatChange();

    /**
     * Returns all possible {@link VersionedWriteLoggable} objects that may be
     * embedded in the binary data of this log entry.
     *
     * <p>This is used by tests to ensure that for each X:Y pair, where X is a
     * ReplicableLogEntry and Y is a VersionedWriteLoggable, and X embeds Y
     * either directly or indirectly, X.getLastFormatChange is greater than or
     * equal to Y.getLastFormatChange.
     *
     * <p>Each ReplicableLogEntry and VersionedWriteLoggable class typically
     * has a LAST_FORMAT_CHANGE constant that is returned by its
     * getLastFormatChange method. When bumping this constant for an object X
     * embedded by an log entry Y, Y.LAST_FORMAT_CHANGE should also be set to
     * the minimum of its current value and X.LAST_FORMAT_CHANGE.
     *
     * <p>Enforcing this rule in a general way is made possible by the
     * getEmbeddedLoggables method of each ReplicableLogEntry and
     * VersionedWriteLoggable. Note that this method is not intended to be
     * called outside of tests.
     */
    // 返回该日志条目二进制数据中可能嵌入的所有版本化可写对象集合。
    Collection<VersionedWriteLoggable> getEmbeddedLoggables();

    /**
     * Returns the number of bytes needed to store this entry in the format for
     * the specified log version.  Earlier log versions only need to be
     * supported for log entries with format changes made in {@link
     * LogEntryType#LOG_VERSION_REPLICATE_OLDER} or greater.
     *
     * @param logVersion the log version
     * @param forReplication whether the entry will be sent over the wire,
     * and not written to the log.
     * @return the number of bytes to store this entry for the log version
     */
    // 计算并返回在指定的日志版本和传输模式下，存储该条目所需的字节数大小。
    int getSize(int logVersion, boolean forReplication);

    /**
     * Serializes this object into the specified buffer in the format for the
     * the specified log version.  Earlier log versions only need to be
     * supported for log entries with format changes made in {@link
     * LogEntryType#LOG_VERSION_REPLICATE_OLDER} or greater.
     *
     * @param logBuffer the destination buffer
     * @param forReplication whether the entry will be sent over the wire,
     * and not written to the log.
     * @param logVersion the log version
     */
    // 将当前对象序列化到指定的缓冲区中，但使用指定的早期日志版本格式。
    void writeEntry(ByteBuffer logBuffer,
                    int logVersion,
                    boolean forReplication);

    /**
     * Returns whether this format has a variant that is optimized for
     * replication.
     */
    // 判断当前日志格式是否存在一个专为复制进行了优化的变体格式。
    boolean hasReplicationFormat();

    /**
     * Returns whether it is worthwhile to materialize and then re-serialize a
     * log entry in a format optimized for replication. Implementations should
     * attempt to check efficiently, without instantiating the log entry
     * object. Some implementations will simply return false.
     *
     * <p>WARNING: The logBuffer position must not be changed by this method.
     *
     * <p>WARNING: The shared LogEntry object is used for calling this method,
     * and this method must not change any of the fields in the object.
     *
     * @param logBuffer contains the entry that would be re-serialized.
     * @param srcVersion the log version of entry in logBuffer.
     * @param destVersion the version that would be used for re-serialization.
     */
    // 评估将现有的日志条目反序列化并重新序列化为复制优化格式是否值得（即是否有性能收益）。
    boolean isReplicationFormatWorthwhile(ByteBuffer logBuffer,
                                          int srcVersion,
                                          int destVersion);
}
