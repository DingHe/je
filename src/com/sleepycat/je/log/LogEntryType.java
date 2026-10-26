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

import java.util.HashSet;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.log.entry.AbortLogEntry;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.CommitLogEntry;
import com.sleepycat.je.log.entry.DeletedDupLNLogEntry;
import com.sleepycat.je.log.entry.FileHeaderEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.MatchpointLogEntry;
import com.sleepycat.je.log.entry.NameLNLogEntry;
import com.sleepycat.je.log.entry.OldBINDeltaLogEntry;
import com.sleepycat.je.log.entry.ReplicableLogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.log.entry.TraceLogEntry;

/**
 * LogEntryType is an enumeration of all log entry types.
 *
 * <p>Log entries are versioned. When changing the persistent form of a log
 * entry in any way that is incompatible with prior releases, make sure the
 * LogEntry instance is capable of reading in older versions from the log and
 * be sure to increment LOG_VERSION.  The LogEntry.readEntry and
 * Loggable.readFromLog methods should check the actual version of the entry.
 * If it is less than LOG_VERSION, the old version should be converted to the
 * current version.
 *
 * <p>Prior to LOG_VERSION 6, each log entry type had a separate version number
 * that was incremented only when that log version changed.  From LOG_VERSION 6
 * onward, all types use the same version, the LOG_VERSION constant.  For
 * versions prior to 6, the readEntry and readFromLog methods will be checking
 * the old per-type version.  There is no overlap between the old per-type
 * versions and the LOG_VERSION values, because the per-type values are all
 * below 6. [#15365]</p>

 * <p>The LogEntry instance must be sure that older versions are converted in
 * memory into a correct instance of the newest version, so when that LogEntry
 * object is written again as the result of migration, eviction, the resulting
 * new log entry conforms to the requirements of the new version.  If context
 * objects are required for data conversion, the conversion can be done in the
 * Node.postFetchInit method.</p>
 *
 * <p>Starting with LOG_VERSION 9, log entries that can be included in the
 * replication stream must be able to write themselves in the format for the
 * immediately previous log version, to allow replication during an upgrade
 * when the master has been upgraded and a replica has not.  Starting with
 * LOG_VERSION 8, log entries that support replication must implement {@link
 * ReplicableLogEntry}.  When changes are made to replicable log entries for
 * LOG_VERSION 9 and later, those entries need to support writing in the
 * previous version's format.</p>
 */
// LogEntryType 是 JE 日志子系统（Log Subsystem）中的核心管理类，它定义并枚举了 JE 数据库日志文件中所有可能的日志条目类型（Log Entry Types）。
// 核心定位：它统一管理 JE 写入磁盘日志的各种数据结构类型（如叶子节点 LN、内部节点 IN、事务提交/回滚、检查点、文件头等）。
// 版本兼容与演进：类中定义了详细的日志版本迭代历史（从 LOG_VERSION 1 到 15），支持从旧版本日志中读取、转换和向后兼容，确保数据库在升级或恢复时能够正确解析历史数据。
// 复制（Replication）支持：支持区分哪些日志条目可以进入复制流（Replication Stream），并定义了主从节点升级过程中的版本兼容性策略。
public class LogEntryType {

    /**
     * Version of the file header, which identifies the version of all entries
     * in that file.
     *
     * Changes to log entries for each version are:
     *
     * Version 3
     * ---------
     * [12328] Add main and dupe tree fanout values for DatabaseImpl.
     * [12557] Add IN LSN array compression.
     * [11597] Add a change to FileSummaryLNs: obsolete offset tracking was
     * added and multiple records are stored for a single file rather than a
     * single record.  Each record contains the offsets that were tracked since
     * the last record was written.
     * [11597] Add the full obsolete LSN in LNLogEntry.
     *
     * Version 4
     * ---------
     * [#14422] Bump MapLN version from 1 to 2.  Instead of a String for the
     * comparator class name, store either a serialized string or Comparator.
     *
     * Version 5
     * ---------
     * [#15195] FileSummaryLN version 3.  Add FileSummary.obsoleteLNSize and
     * obsoleteLNSizeCounted fields.
     *
     * Version 6 (in JE 3.3.X)
     * ---------
     * [#15365] From this point onward, all log entries have the same version,
     * LOG_VERSION, rather than using per-type versions.
     * [#15365] DatabaseImpl stores a map of DbFileSummaries.
     *
     * [#13467] Convert duplicatesAllowed boolean to DUPS_ALLOWED_BIT flag in
     * DatabaseImpl. Add REPLICATED_BIT flag to DatabaseImpl.
     * [#13467] Add REPLICATED_BIT to DbTree.
     * [#13467] Add ReplicatedDatabaseConfig to NameLN_TX to support
     * replication of database operations.
     *
     * [#15581] Add lastAllocateReplicatedDbId to DbTree
     * [#16083] Add replication master node ID to txn commit/abort
     *
     * Version 7 (in JE 4.0)
     * ---------------------
     * Add the invisible bit in the entry header version field.
     * Add the RollbackStart log entry type
     * Add the RollbackEnd log entry type
     * Add the Matchpoint log entry type.
     *
     * Version 8 (in JE 5.0)
     * ---------------------
     * Made provisions for storing Triggers in a DatabaseImpl.
     *
     * Database IDs enlarged from int or packed int, to long or packed long
     * (note that packed int and packed long are compatible).  [#18540]
     *
     * Add new log entry types for LN delete, insert and update. [#18055]
     *
     * Apply optimization to omit key size for some internal LNs, in addition
     * to user LNs. [#18055]
     *
     * LN no longer has node ID. [#18633]
     *
     * Add FileSummary.maxLNSize. [#18633]
     *
     * VLSN is optionally maintained in LogEntryHeader for cleaner migrated LNs
     * and a new VLSN_PRESENT entry header flag is used to signify the presence
     * of the VLSN.  PRESERVE_VLSN_BIT was added to DbTree to correspond to the
     * je.env.preserveRecordVersion environment config param. [#19476]
     *
     * Dup tree representation changed to use two-part keys. Deprecated: DIN,
     * DBIN, DupCountLN, INDeleteInfo, INDupDeleteInfo.  Removed
     * DatabaseImpl.maxDupTreeEntriesPerNode.  [#19165]
     *
     * Version 9 (in JE 6.0)
     * ---------------------
     * See comment above about ReplicableLogEntry.
     *
     * BIN-deltas are now represented as BINs using the new BINDeltaLogEntry
     * (log entry type NewBINDelta).
     *
     * Version 10 (in JE 6.2)
     * ----------------------
     * Each BIN-delta stores the total and max number of entries in the
     * previous full version of the same BIN. A BIN-delta may also store a
     * bloom filter for the keys in the full BIN.
     *
     * Version 11 (in JE 6.3)
     * ----------------------
     * LN log records have additional info to handle embedded records. See
     * LNLogEntry for details. Also, BIN log records include the VLSNs of
     * embedded records.
     *
     * Added LOG_IMMUTABLE_FILE entry type.
     *
     * Version 12 (in JE 7.0)
     * ----------------------
     * Added expiration info to LNs and BIN slots. The LN's expiration time is
     * replicated, so this changes the replication format. Also added the "have
     * abort LSN" flag, to avoid writing a byte for a null LSN, and moved the
     * flags to the front to support the new forReplication format.
     *
     * For ReplicableLogEntry and VersionedWriteLoggable, added the
     * forReplication parameter to write methods to support a replication
     * format variation. This allows omitting some fields from an entry when
     * they are not needed by replication.
     *
     * LNLogEntry in forReplication mode no longer includes the abortLSN or the
     * abortKnownDeleted flag. This is a format change because these were
     * used by Replay in earlier versions.
     *
     * Txn and VersionedWriteTxnEnd (Commit and Abort) in forReplication mode
     * now always includes a null prevLsn field, which only occupies one byte.
     * However, this is not strictly a format change, because this field has
     * never been used by Replay.
     *
     * Version 13 (in JE 7.1)
     * ----------------------
     *
     * Added dtvlsn field to LOG_TXN_END entry to support efficient persistent
     * tracking of the DTVLN (Durable Transaction VLSN).
     *
     * Version 14 (in JE 7.3)
     * ----------------------
     *
     * Added LOG_RESTORE_REQUIRED to indicate that the environment's log is no
     * longer consistent, and some curative action must happen before the
     * environment can be recovered.
     *
     * Version 15 (in JE 7.5)
     * ----------------------
     *
     * Fixed a bug in mutation of a BIN-delta to a full BIN where the
     * identifierKey was not set correctly. The identifierKey can only be
     * checked by the BtreeVerifier when the log was initially created using
     * log version 15 or greater. Added new field DbTree.initialLogVersion
     * to support this.
     *
     * The GlobalCBVLSN is no longer updated in the rep group DB when all
     * nodes in a rep group have been updated to 7.5 or later. The network
     * restore protocol no longer relies on the GlobalCBVLSN.
     *
     * The _jeReservedFilesDb internal DB (DbType.RESERVED_FILES) was added.
     * Uses a new LN log entry type: LOG_RESERVED_FILE_LN. Contains metadata
     * used to manage reserved files.
     */
    // 当前最新的日志版本号（代码中为 15）。自版本 6 开始，所有日志条目统一使用此全局版本号，而不是各自维护独立的版本。
    public static final int LOG_VERSION = 15;

    /**
     * The latest log version for which the replicated log format of any
     * replicable log entry class changed.  Replication uses this value to
     * determine if the latest version of the replication stream can be
     * understood by an earlier software version.  This field is needed to
     * account for cases where log entry format changes only apply to
     * non-replicable entries, or only to the local, not replicated, form of
     * replicable entries, the as was the case for log versions 9, 10, and 11.
     */
    // 最高可复制日志条目格式发生改变的版本号（13）。用于判断复制流是否能被早期软件版本理解。
    public static final int LOG_VERSION_HIGHEST_REPLICABLE = 13;
    // 最早支持的日志版本号（1）
    public static final int FIRST_LOG_VERSION = 1;

    /**
     * The earliest log version for which replicable log entries support
     * writing themselves in older versions, to support replication to
     * older nodes during upgrades.
     */
    // 支持向旧节点复制的最低日志版本（9）
    public static final int LOG_VERSION_REPLICATE_OLDER = 9;

    /*
     * The log version that added expiration info to LNs and BIN slots for JE
     * 7.0.
     */
    // 在 LN 和 BIN 插槽中引入过期信息（Expiration Info）的日志版本（12）。
    public static final int LOG_VERSION_EXPIRE_INFO = 12;

    /*
     * The log version that introduced the dtvlsn field in commit log entries.
     */
    // 在提交日志条目中引入持久化 VLSN（dtvlsn）字段的版本（13）。
    public static final int LOG_VERSION_DURABLE_VLSN = 13;

    /**
     * Should be used for reading the entry header of the file header, since
     * the actual version is not known until the FileHeader item is read.
     */
    // 未知的文件头版本（-1）。在读取文件头之前由于版本未知而使用。
    public static final int UNKNOWN_FILE_HEADER_VERSION = -1;

    /*
     * Collection of log entry type classes, used to read the log.  Note that
     * this must be declared before any instances of LogEntryType, since the
     * constructor uses this map. Each statically defined LogEntryType should
     * register itself with this collection.
     */
    private static final int MAX_TYPE_NUM = 40;
    private static LogEntryType[] LOG_TYPES = new LogEntryType[MAX_TYPE_NUM];

    /*
     * Enumeration of log entry types. The log entry type represents the 2 byte
     * field that starts every log entry. The top byte is the log type, the
     * bottom byte holds the version value, provisional bit, replicated bit,
     * and invisible bit.
     *
     * Log type(8 bits)
     * Provisional(2 bits) Replicated(1 bit) Invisible(1 bit) Version(5 bits)
     *
     * The top byte (log type) identifies the type and can be used to lookup
     * the LogEntryType object, while the bottom byte has information about the
     * entry (instance) of this type.  The bottom byte is effectively entry
     * header information that is common to all types and is managed by methods
     * in LogEntryHeader. See LogEntryHeader.java
     */

    /*  Node types */

    /*
     * Deprecated transactional LN entry type, use LOG_DEL_LN_TRANSACTIONAL,
     * LOG_INS_LN_TRANSACTIONAL, LOG_UPD_LN_TRANSACTIONAL instead according to
     * the operation type.
     */
    // 旧版的事务性用户 LN 日志类型
    // 现已被废弃。在现代版本的 JE 中，为了更精确地追踪和处理不同类型的数据库操作，
    // 被细分为三个更具体的类型：LOG_DEL_LN_TRANSACTIONAL（删除）、LOG_INS_LN_TRANSACTIONAL（插入）和 LOG_UPD_LN_TRANSACTIONAL（更新）。
    public static final LogEntryType LOG_OLD_LN_TRANSACTIONAL =
        createReplicableLogEntryType(
            (byte) 1, "LN_TX",
            LNLogEntry.create(com.sleepycat.je.tree.LN.class),
            Txnal.TXNAL,
            Marshall.OUTSIDE_LATCH,
            Replicable.REPLICABLE_NO_MATCH,
            NodeType.LN_USER);

    /*
     * Deprecated LN entry type, use LOG_DEL_LN, LOG_INS_LN, LOG_UPD_LN instead
     * according to the operation type.
     */
    // 旧版的非事务性用户 LN 日志类型。
    // 同样已在后续版本中被废弃，由操作细分后的 LOG_DEL_LN、LOG_INS_LN 和 LOG_UPD_LN 取代。
    public static final LogEntryType LOG_OLD_LN =
        createReplicableLogEntryType(
            (byte) 2, "LN",
            LNLogEntry.create(com.sleepycat.je.tree.LN.class),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            Replicable.REPLICABLE_NO_MATCH,
            NodeType.LN_USER);

    /*
     * Never used
     */
    // 注释明确标注了 "Never used"（从未被使用）
    public static final LogEntryType LOG_MAPLN_TRANSACTIONAL =
        new LogEntryType
        ((byte) 3, "MapLN_TX",
         LNLogEntry.create(com.sleepycat.je.tree.MapLN.class),
         Txnal.TXNAL,
         Marshall.INSIDE_LATCH, /* Logging changes DB utilization. */
         NodeType.LN_INTERNAL);
    // MapLN（Map Leaf Node）用于存储 JE 数据库的核心元数据映射（例如将内部 DatabaseId 映射到具体的 B 树根节点指针等）。
    // 尽管它本身是非事务性的，但由于它的写入会直接影响底层存储文件的空间利用率统计（Utilization），因此必须在全局日志写锁内部进行编组，以确保元数据状态与空间统计强一致。
    public static final LogEntryType LOG_MAPLN =
        new LogEntryType
        ((byte) 4, "MapLN",
         LNLogEntry.create(com.sleepycat.je.tree.MapLN.class),
         Txnal.NON_TXNAL,
         Marshall.INSIDE_LATCH, /* Logging changes DB utilization. */
         NodeType.LN_INTERNAL);
    // NameLN 用于管理数据库的命名空间（例如创建、删除或重命名数据库时的目录映射）。
    // 代表在一个事务上下文中执行的数据库创建或修改操作（带有事务属性），并且它支持复制，允许在主从高可用集群中同步数据库的创建与删除。
    public static final LogEntryType LOG_NAMELN_TRANSACTIONAL =
        createReplicableLogEntryType(
            (byte) 5, "NameLN_TX",
            new NameLNLogEntry(),
            Txnal.TXNAL,
            Marshall.OUTSIDE_LATCH,
            Replicable.REPLICABLE_NO_MATCH,
            NodeType.LN_INTERNAL);

    public static final LogEntryType LOG_NAMELN =
        createReplicableLogEntryType(
            (byte) 6, "NameLN",
            new NameLNLogEntry(),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            Replicable.REPLICABLE_NO_MATCH,
            NodeType.LN_INTERNAL);

    /* Obsolete in version 8, only used by some log readers. */
    public static final LogEntryType LOG_DEL_DUPLN_TRANSACTIONAL =
        createReplicableLogEntryType(
            (byte) 7, "DelDupLN_TX",
            new DeletedDupLNLogEntry(),
            Txnal.TXNAL,
            Marshall.OUTSIDE_LATCH,
            Replicable.REPLICABLE_NO_MATCH,
            NodeType.LN_USER);

    /* Obsolete in version 8, only used by some log readers. */
    public static final LogEntryType LOG_DEL_DUPLN =
        createReplicableLogEntryType(
            (byte) 8, "DelDupLN",
            new DeletedDupLNLogEntry(),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            Replicable.REPLICABLE_NO_MATCH,
            NodeType.LN_USER);

    /* Obsolete in version 8, only used by DupConvert and some log readers. */
    public static final LogEntryType LOG_DUPCOUNTLN_TRANSACTIONAL =
        new LogEntryType
        ((byte) 9, "DupCountLN_TX",
         LNLogEntry.create(com.sleepycat.je.tree.dupConvert.DupCountLN.class),
         Txnal.TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.OLD_DUP);

    /* Obsolete in version 8, only used by DupConvert and some log readers. */
    public static final LogEntryType LOG_DUPCOUNTLN =
        new LogEntryType
        ((byte) 10, "DupCountLN",
         LNLogEntry.create(com.sleepycat.je.tree.dupConvert.DupCountLN.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.OLD_DUP);

    public static final LogEntryType LOG_FILESUMMARYLN =
        new LogEntryType
        ((byte) 11, "FileSummaryLN",
         LNLogEntry.create(com.sleepycat.je.tree.FileSummaryLN.class),
         Txnal.NON_TXNAL,
         Marshall.INSIDE_LATCH, /* Logging changes file utilization. */
         NodeType.LN_INTERNAL);

    public static final LogEntryType LOG_IN =
        new LogEntryType
        ((byte) 12, "IN",
         INLogEntry.create(com.sleepycat.je.tree.IN.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.IN);

    public static final LogEntryType LOG_BIN =
        new LogEntryType
        ((byte) 13, "BIN",
         INLogEntry.create(com.sleepycat.je.tree.BIN.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.IN);

    /* Obsolete in version 8, only used by DupConvert and some log readers. */
    public static final LogEntryType LOG_DIN =
        new LogEntryType
        ((byte) 14, "DIN",
         INLogEntry.create(com.sleepycat.je.tree.dupConvert.DIN.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.OLD_DUP);

    /* Obsolete in version 8, only used by DupConvert and some log readers. */
    public static final LogEntryType LOG_DBIN =
        new LogEntryType
        ((byte) 15, "DBIN",
         INLogEntry.create(com.sleepycat.je.tree.dupConvert.DBIN.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.OLD_DUP);

    /*
     * The root entry of the DbTree, it saves the root information for name
     * and id database.
     */
    public static final LogEntryType LOG_DBTREE =
        new LogEntryType
        ((byte) 16, "DbTree",
         SingleItemEntry.create(
             com.sleepycat.je.dbi.DbTree.class),
         Txnal.NON_TXNAL,
         Marshall.INSIDE_LATCH, /* Logging changes DB utilization. */
         NodeType.NONE);

    /* Transactional entries */
    public static final LogEntryType LOG_TXN_COMMIT =
        createReplicableLogEntryType(
            (byte) 17, "Commit",
            new CommitLogEntry(),
            Txnal.TXNAL,
            Marshall.INSIDE_LATCH, /* To ensure DTVLSN is in sync with VLSN */
            Replicable.REPLICABLE_MATCH,
            NodeType.NONE);

    public static final LogEntryType LOG_TXN_ABORT =
        createReplicableLogEntryType(
            (byte) 18, "Abort",
            new AbortLogEntry(),
            Txnal.TXNAL,
            Marshall.INSIDE_LATCH, /* To ensure DTVLSN is in sync with VLSN */
            Replicable.REPLICABLE_MATCH,
            NodeType.NONE);

    public static final LogEntryType LOG_CKPT_START =
        new LogEntryType
        ((byte) 19, "CkptStart",
         SingleItemEntry.create(
             com.sleepycat.je.recovery.CheckpointStart.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    public static final LogEntryType LOG_CKPT_END =
        new LogEntryType
        ((byte) 20, "CkptEnd",
         SingleItemEntry.create(
             com.sleepycat.je.recovery.CheckpointEnd.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    /* Obsolete in version 8, only used by some log readers. */
    public static final LogEntryType LOG_IN_DELETE_INFO =
        new LogEntryType
        ((byte) 21, "INDelete",
         SingleItemEntry.create(
             com.sleepycat.je.tree.dupConvert.INDeleteInfo.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    /* Obsolete in version 9, replaced by "live" LOG_BIN_DELTA. */
    public static final LogEntryType LOG_OLD_BIN_DELTA =
        new LogEntryType
        ((byte) 22, "BINDelta",
         new OldBINDeltaLogEntry
         (com.sleepycat.je.tree.OldBINDelta.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    /* Obsolete in version 8, only used by some log readers. */
    public static final LogEntryType LOG_OLD_DUP_BIN_DELTA =
        new LogEntryType
        ((byte) 23, "DupBINDelta",
         new OldBINDeltaLogEntry
         (com.sleepycat.je.tree.OldBINDelta.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    /* Although this is replicable, it is never replicated except in tests. */
    public static final LogEntryType LOG_TRACE =
        createReplicableLogEntryType(
            (byte) 24, "Trace",
            new TraceLogEntry(),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            Replicable.REPLICABLE_NO_MATCH,
            NodeType.NONE);

    /* File header */
    public static final LogEntryType LOG_FILE_HEADER =
        new LogEntryType
        ((byte) 25, "FileHeader",
         new FileHeaderEntry
         (com.sleepycat.je.log.FileHeader.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    /* Obsolete in version 8, only used by some log readers. */
    public static final LogEntryType LOG_IN_DUPDELETE_INFO =
        new LogEntryType
        ((byte) 26, "INDupDelete",
         SingleItemEntry.create(
             com.sleepycat.je.tree.dupConvert.INDupDeleteInfo.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    public static final LogEntryType LOG_TXN_PREPARE =
        new LogEntryType
        ((byte) 27, "Prepare",
         SingleItemEntry.create(
             com.sleepycat.je.txn.TxnPrepare.class),
         Txnal.TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    public static final LogEntryType LOG_ROLLBACK_START =
        new LogEntryType
        ((byte) 28, "RollbackStart",
         SingleItemEntry.create(
             com.sleepycat.je.txn.RollbackStart.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    public static final LogEntryType LOG_ROLLBACK_END =
        new LogEntryType
        ((byte) 29, "RollbackEnd",
         SingleItemEntry.create(
             com.sleepycat.je.txn.RollbackEnd.class),
         Txnal.NON_TXNAL,
         Marshall.OUTSIDE_LATCH,
         NodeType.NONE);

    public static final LogEntryType LOG_MATCHPOINT =
        createReplicableLogEntryType(
            (byte) 30, "Matchpoint",
            new MatchpointLogEntry(),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            Replicable.REPLICABLE_MATCH,
            NodeType.NONE);

    public static final LogEntryType LOG_DEL_LN_TRANSACTIONAL =
        new UserLNLogEntryType((byte) 31, "DEL_LN_TX", Txnal.TXNAL);

    public static final LogEntryType LOG_DEL_LN =
        new UserLNLogEntryType((byte) 32, "DEL_LN", Txnal.NON_TXNAL);

    public static final LogEntryType LOG_INS_LN_TRANSACTIONAL =
        new UserLNLogEntryType((byte) 33, "INS_LN_TX", Txnal.TXNAL);

    public static final LogEntryType LOG_INS_LN =
        new UserLNLogEntryType((byte) 34, "INS_LN", Txnal.NON_TXNAL);

    public static final LogEntryType LOG_UPD_LN_TRANSACTIONAL =
        new UserLNLogEntryType((byte) 35, "UPD_LN_TX", Txnal.TXNAL);

    public static final LogEntryType LOG_UPD_LN =
        new UserLNLogEntryType((byte) 36, "UPD_LN", Txnal.NON_TXNAL);

    public static final LogEntryType LOG_BIN_DELTA =
        new LogEntryType(
            (byte) 37, "NewBINDelta",
            new BINDeltaLogEntry(com.sleepycat.je.tree.BIN.class),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            NodeType.IN);

    public static final LogEntryType LOG_IMMUTABLE_FILE =
        new LogEntryType(
            (byte) 38, "ImmutableFile",
            SingleItemEntry.create(
                com.sleepycat.je.log.entry.EmptyLogEntry.class),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            NodeType.NONE);

    public static final LogEntryType LOG_RESTORE_REQUIRED =
        new LogEntryType(
            (byte) 39, "RestoreRequired",
            SingleItemEntry.create(
                com.sleepycat.je.log.entry.RestoreRequired.class),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            NodeType.NONE);

    public static final LogEntryType LOG_RESERVED_FILE_LN =
        new LogEntryType
            ((byte) 40, "ReservedFileLN",
            LNLogEntry.create(com.sleepycat.je.tree.LN.class),
            Txnal.NON_TXNAL,
            Marshall.OUTSIDE_LATCH,
            NodeType.LN_INTERNAL);

    /*** If you add new types, be sure to update MAX_TYPE_NUM at the top.***/

    /* Persistent fields */
    // 该日志条目类型的唯一数值标识（编号从 1 到 40）。在日志文件中，每个日志条目的头部都会记录这个编号。
    private final byte typeNum; // persistent value for this entry type
    // 该日志类型的可读字符串名称（例如 "LN_TX"、"Commit"、"BIN"），主要用于调试、报告和日志打印。
    /* Transient fields */
    private final String displayName;
    // 与该类型关联的 LogEntry 实例模版，负责具体的数据编组（Marshalling）、解组（Unmarshalling）以及读写操作。
    private final LogEntry logEntry;

    /*
     * Attributes
     */

    /* Whether the log entry holds a transactional information. */
    // 标识该日志条目是否包含事务相关信息（如 Commit、Abort 或事务性 LN）。
    private Txnal isTransactional;

    /*
     * Does this log entry be marshalled outside or inside the log write
     * latch.
     */
    // 标识该日志条目序列化时是在日志写锁内部（INSIDE_LATCH）还是在日志写锁外部（OUTSIDE_LATCH）进行。例如，FileSummaryLN 和事务提交/回滚需要内部加锁以保证串行语义。
    private Marshall marshallBehavior;

    /* Can this log entry be put in the replication stream? */
    // 标识该日志条目是否可以分发到复制组中。
    private Replicable replicationPossible;
    // 关联的 B 树节点类型（如用户 LN、内部节点、非节点等）。
    private NodeType nodeType;

    /*
     * Constructors
     */

    /**
     * For base class support.
     */

    /*
     * This constructor only used when the LogEntryType is being used as a key
     * for a map. No log types can be defined outside this package.
     */
    LogEntryType(byte typeNum) {
        this.typeNum = typeNum;
        displayName = null;
        logEntry = null;
    }

    /**
     * Used to create a map key for reporting that is not a real type.
     */
    LogEntryType(byte typeNum, String displayName) {
        assert typeNum > MAX_TYPE_NUM;
        this.typeNum = typeNum;
        this.displayName = displayName;
        logEntry = null;
    }

    /**
     * Create a non-replicable log type.
     *
     * @param isTransactional whether this type of log entry holds data
     * involved in a transaction. For example, transaction commit and LN data
     * records are transactional, but INs are not.
     * @param marshallBehavior whether this type of log entry may be serialized
     * outside the log write latch. This is true of the majority of
     * types. Certain types like the FileSummaryLN rely on the log write latch
     * to enforce serial semantics.
     */
    private LogEntryType(final byte typeNum,
                         final String displayName,
                         final LogEntry logEntry,
                         final Txnal isTransactional,
                         final Marshall marshallBehavior,
                         final NodeType nodeType) {

        this(typeNum, displayName, logEntry, isTransactional, marshallBehavior,
             Replicable.LOCAL, nodeType);
    }

    /**
     * Create a replicable log type.
     *
     * @param isTransactional whether this type of log entry holds data
     * involved in a transaction
     * @param marshallBehavior whether this type of log entry may be serialized
     * outside the log write latch
     * @param replicationPossible whether this type of log entry can be shared
     * with a replication group
     */
    // 创建和注册“可复制（Replicable）”的日志条目类型。
    // 在 Berkeley DB JE 的高可用复制架构（Replication Group）中，主节点（Master）产生的某些日志（如事务提交、特定数据变更、元数据操作等）需要通过网络同步并重放到副本节点（Replica）。
    // 这个工厂方法为这些需要参与复制的日志类型统一设定了标准化的创建流程，并配合构造函数中的断言机制，确保所有可复制的日志条目都正确实现了复制接口。
    private static LogEntryType createReplicableLogEntryType(
        // 该日志条目类型的唯一数值编号（例如 1、5、17 等）。
        // 写入磁盘日志时，日志头会记录这个字节编号，用于后续恢复或读取时识别日志类型。
        final byte typeNum,
        // 该日志类型的可读字符串名称（例如 "Commit"、"LN_TX"、"NameLN_TX"）。
        final String displayName,
        // 与该日志类型关联的日志条目模版实例。
        // 该对象必须实现 ReplicableLogEntry 接口，负责具体数据的序列化（Marshalling）、反序列化（Unmarshalling）以及针对复制流的特殊格式处理。
        final ReplicableLogEntry logEntry,
        // 标识该日志条目是否包含事务信息
        final Txnal isTransactional,
        // 标识该日志条目的序列化锁行为。
        // 决定序列化操作是在 JE 的日志写锁（Log Write Latch）内部（INSIDE_LATCH）还是外部（OUTSIDE_LATCH）进行。例如，事务提交记录需要 INSIDE_LATCH 以确保与底层 VLSN（Virtual LSN）保持严格同步。
        final Marshall marshallBehavior,
        // 标识该日志条目在复制流中的匹配或共享策略。
        // 例如 Replicable.REPLICABLE_MATCH 或 REPLICABLE_NO_MATCH，用于细粒度控制哪些日志可以安全地分发给复制组中的其他节点。
        final Replicable replicationPossible,
        // 关联的 B 树节点类型分类。
        final NodeType nodeType) {

        return new LogEntryType(typeNum, displayName, logEntry,
                                isTransactional, marshallBehavior,
                                replicationPossible, nodeType);
    }

    /**
     * Internal constructor for all log types.  Don't create instances using
     * this directly, to improve error checking.
     */
    private LogEntryType(final byte typeNum,
                         final String displayName,
                         final LogEntry logEntry,
                         final Txnal isTransactional,
                         final Marshall marshallBehavior,
                         final Replicable replicationPossible,
                         final NodeType nodeType) {

        this.typeNum = typeNum;
        this.displayName = displayName;
        this.logEntry = logEntry;
        this.isTransactional = isTransactional;
        this.marshallBehavior = marshallBehavior;
        this.replicationPossible = replicationPossible;
        this.nodeType = nodeType;
        logEntry.setLogType(this);
        LOG_TYPES[typeNum - 1] = this;

        assert logEntry != null && replicationPossible != null;
        assert !replicationPossible.isReplicable() ||
            logEntry instanceof ReplicableLogEntry
            : "Replicable log types must have replicable log entries";
    }

    /**
     * @return the static version of this type
     */
    public static LogEntryType findType(byte typeNum) {
        if (typeNum <= 0 || typeNum > MAX_TYPE_NUM) {
            return null;
        }
        return LOG_TYPES[typeNum - 1];
    }

    /**
     * Get a copy of all types for unit testing.
     */
    public static Set<LogEntryType> getAllTypes() {
        HashSet<LogEntryType> ret = new HashSet<LogEntryType>();

        for (int i = 0; i < MAX_TYPE_NUM; i++) {
            ret.add(LOG_TYPES[i]);
        }
        return ret;
    }

    /**
     * @return the log entry type owned by the shared, static version
     */
    public LogEntry getSharedLogEntry() {
        return logEntry;
    }

    /**
     * @return a clone of the log entry type for a given log type.
     */
    public LogEntry getNewLogEntry()
        throws DatabaseException {

        return logEntry.clone();
    }

    public byte getTypeNum() {
        return typeNum;
    }

    /**
     * @return true if type number is valid.
     */
    static boolean isValidType(byte typeNum) {
        return typeNum > 0 && typeNum <= MAX_TYPE_NUM;
    }

    public String toStringNoVersion() {
        return displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }

    /**
     * Check for equality without making a new object.
     */
    public boolean equalsType(byte type) {
        return (this.typeNum == type);
    }

    /*
     * Override Object.equals. Ignore provisional bit when checking for
     * equality.
     */
    @Override
    public boolean equals(Object obj) {
        /* Same instance? */
        if (this == obj) {
            return true;
        }

        /* Is it the right type of object? */
        if (!(obj instanceof LogEntryType)) {
            return false;
        }

        return typeNum == ((LogEntryType) obj).typeNum;
    }

    /**
     * This is used as a hash key.
     */
    @Override
    public int hashCode() {
        return typeNum;
    }
    // 为日志条目（Log Entry）提供事务属性的二元标识。
    // 明确界定了某一种日志条目究竟属于事务性（Transactional）操作，还是非事务性（Non-transactional）操作。
    // 主要用途：在数据库进行崩溃恢复（Recovery）、事务回滚、重放（Replay）以及日志管理时，引擎需要根据这个标记来决定如何处理这些日志（例如，事务性日志需要参与 ACID 事务生命周期和锁管理，而非事务性日志如内部节点或检查点则不需要）。
    static enum Txnal {
        // 事务性。
        // 用于标识那些由用户事务触发、或者包含事务上下文信息的日志条目。例如：事务提交记录（Commit）、事务中止记录（Abort）、带有事务属性的用户记录增删改（如 INS_LN_TX、DEL_LN_TX 等）。
        TXNAL(true),
        // 非事务性。
        // 用于标识系统内部维护的、与特定业务事务无直接绑定关系的日志条目。例如：B树内部节点（IN/BIN）、检查点开始/结束记录（CkptStart/CkptEnd）、文件头（FileHeader）以及内部元数据变更等。
        NON_TXNAL(false);

        private final boolean isTxnal;

        Txnal(boolean isTxnal) {
            this.isTxnal = isTxnal;
        }

        boolean isTransactional() {
            return isTxnal;
        }
    }

    /**
     * Return true if this log entry has transactional information in it,
     * like a commit or abort record, or a transactional LN.
     */
    public boolean isTransactional() {
        return isTransactional.isTransactional();
    }
    // Marshall 枚举的核心作用是控制日志条目在序列化（Marshalling / 编组）时与日志写锁（Log Write Latch）的并发同步关系。
    // 决定了某个日志条目在转化为磁盘字节流时，是否可以在全局日志写锁的外部并发执行，还是必须严格在日志写锁的内部串行执行。
    // 主要用途：这是 JE 优化高并发日志写入性能的重要设计。绝大多数日志条目允许在锁外序列化以降低锁竞争，而少数对状态强一致性有要求的特殊日志则必须在加锁状态下序列化。
    static enum Marshall {
        // 允许在日志写锁外部序列化。
        // 适用于绝大多数标准的日志条目（如普通的 LN、IN 等）。在写入日志文件前，线程可以先把这些对象在内存中序列化好，然后再去获取全局的日志写锁完成最终写入，从而最大程度减少锁持有时间，提升系统的并发吞吐量。
        OUTSIDE_LATCH(true),
        // 必须在日志写锁内部序列化。
        // 适用于那些序列化过程强依赖于临界区内实时状态、或者需要强制串行语义的特殊日志。
        // 例如：文件摘要信息（FileSummaryLN）、映射数据库记录（MapLN）、以及包含 DTVLSN（Durable Transaction VLSN）的事务提交（Commit）和中止（Abort）日志。
        // 这些条目必须在获取日志写锁后才能进行编组，以防并发修改导致状态不一致。
        INSIDE_LATCH(false);

        private final boolean marshallOutsideLatch;

        Marshall(boolean marshallOutsideLatch) {
            this.marshallOutsideLatch = marshallOutsideLatch;
        }

        boolean marshallOutsideLatch() {
            return marshallOutsideLatch;
        }
    }

    /**
     * Return true if this log entry should be marshalled into a buffer outside
     * the log write latch. Currently, the FileSummaryLN and MapLN (which
     * contains DbFileSummary objects) and the commit and abort log entries
     * (due to their DTVLSN fields) need to be logged inside the log write
     * latch.
     */
    public boolean marshallOutsideLatch() {
        return marshallBehavior.marshallOutsideLatch();
    }

    /**
     * Return true if the type of this LogEntryType is equivalent to typeB.
     * Version is used as a factor in the comparison when new log entry types
     * are introduced in one release, which supercede existing types.
     */
    public boolean compareTypeAndVersion(int versionA,
                                         int versionB,
                                         final byte typeB) {
        return typeNum == typeB;
    }

    /*
     * Indicates whether this type of log entry is shared in a replicated
     * environment or not, and whether it can be used as a replication
     * matchpoint.
     */
    // 定义日志条目在主从复制（Replication）环境中的分发属性与对齐能力。
    // 核心定位：它明确标示了某类日志条目是仅属于本地（Local），还是可以分发到复制组（Replication Group）；同时，它还区分了这些可复制的日志中，哪些可以作为主从节点流同步的匹配点（Matchpoint）。
    // 主要用途：在高可用复制架构中，当副本节点（Replica）与主节点（Master）建立连接或进行日志流对齐、故障恢复时，系统需要借助这些标记来识别哪些日志记录具有全局一致的对齐参考价值。
    static enum Replicable {
        // 可复制，且可作为匹配点。
        // 既能在复制组中跨节点共享传输，又具备充当复制流匹配点（Matchpoint）的资格。通常用于如事务提交（Commit）、中止（Abort）或专门的 Matchpoint 日志，这些日志在主从同步时能够帮助确认双方的日志位置是否处于一致状态。
        REPLICABLE_MATCH(true, true),
        // 可复制，但不能作为匹配点。
        // 这些日志条目（例如某些用户数据变更的 LN 日志、NameLN 等）会随复制流分发给副本节点并在从库重放，但它们本身不适合用来作为主从流对齐的基准锚点。
        REPLICABLE_NO_MATCH(true, false),
        // 仅本地有效，不可复制。
        LOCAL(false, false);

        private final boolean isReplicable;
        private final boolean isMatchable;

        Replicable(boolean isReplicable, boolean isMatchable) {
            this.isReplicable = isReplicable;
            this.isMatchable = isMatchable;
        }

        boolean isReplicable() {
            return isReplicable;
        }

        boolean isMatchable() {
            return isMatchable;
        }
    }

    /**
     * Return true if this type of log entry can be part of the replication
     * stream. For example, INs can never be replicated, while LNs are
     * replicated only if their owning database is replicated.
     */
    public boolean isReplicationPossible() {
        return replicationPossible.isReplicable();
    }

    /**
     * Return true if this type of log entry can serve as the synchronization
     * matchpoint for the replication stream. That generally means that this
     * log entry contains an replication node ID.
     */
    public boolean isSyncPoint() {
        return replicationPossible.isMatchable();
    }

    /**
     * Return true if this type of log entry can serve as the synchronization
     * matchpoint for the replication stream.
     */
    public static boolean isSyncPoint(byte entryType) {
        return findType(entryType).isSyncPoint();
    }

    /* Type of Btree node. */
    // NodeType 枚举的主要作用是在日志子系统和 B 树结构之间建立粗粒度的节点类别映射。
    // 职责：它帮助 JE 区分某个日志条目到底对应普通用户数据、内部管理数据、B 树内部索引节点、还是已被废弃的旧版重复键节点。
    // 应用场景：在执行崩溃恢复、内存统计、树结构验证或日志项过滤时，系统可以根据这个类型快速判断该日志条目在 B 树体系中的角色。
    static enum NodeType {

        /* Not a Btree node. */
        // 非 B 树节点。
        // 用于那些不属于 B 树节点或叶子记录的系统级日志条目，例如事务提交/回滚记录（Commit/Abort）、检查点记录（CkptStart/CkptEnd）、文件头（FileHeader）以及元数据管理树（DbTree）等。
        NONE,

        /* Internal node. Does not include old-format DIN/DBIN. */
        // 代表 B 树的内部索引节点（包括 IN、BIN 以及新版 BIN-delta）。注释特别说明，它不包括旧格式的 DIN/DBIN。
        IN,

        /* DIN/DBIN/DupCountLN in old-format duplicates database. */
        // 旧版重复键数据库中的特殊节点。
        // 代表旧格式支持重复键（Duplicates）数据库时所使用的特殊树节点，包括 DIN（Duplicate Internal Node）、DBIN（Duplicate Bottom Internal Node）以及 DupCountLN。这些节点在现代 JE 版本中已被新的两部分键（Two-part keys）机制取代，目前仅用于旧日志读取和转换（DupConvert）。
        OLD_DUP,

        /* LNs representing records in internal databases. */
        // 内部数据库中的叶子记录（LN）。
        // 代表用于存储 JE 系统内部元数据的叶子节点，例如映射数据库记录（MapLN）、数据库名称记录（NameLN）、文件摘要信息（FileSummaryLN）以及保留文件管理记录等。这些 LN 是 JE 引擎维持自身运转所必需的。
        LN_INTERNAL,

        /* LNs representing ordinary user records.  */
        // 普通用户记录的叶子节点（LN）。
        // 代表存储用户实际业务键值对数据的叶子节点（包括各种插入、更新、删除操作产生的用户 LN）。这是应用程序最常交互的数据形态。
        LN_USER;
    }

    public boolean isNodeType() {
        return nodeType != NodeType.NONE;
    }

    public boolean isUserLNType() {
        return nodeType == NodeType.LN_USER;
    }

    public boolean isLNType() {
        return nodeType == NodeType.LN_INTERNAL || isUserLNType();
    }

    public boolean isINType() {
        return nodeType == NodeType.IN;
    }

    public boolean isOldDupType() {
        return nodeType == NodeType.OLD_DUP;
    }

    /**
     * Return true if the two types are equal. Handles the situation where new
     * log entry types were introduced in one release, that are actually
     * equivalent to old, deprecated types.
     */
    public static boolean compareTypeAndVersion(int versionA,
                                                byte typeA,
                                                int versionB,
                                                byte typeB) {
        LogEntryType entryA = findType(typeA);
        return entryA.compareTypeAndVersion(versionA, versionB, typeB);
    }

    private static class UserLNLogEntryType extends LogEntryType {
        public UserLNLogEntryType(byte typeNum,
                                  String displayName,
                                  Txnal txnal) {
            super(typeNum, displayName,
                  LNLogEntry.create(com.sleepycat.je.tree.LN.class),
                  txnal, Marshall.OUTSIDE_LATCH,
                  Replicable.REPLICABLE_NO_MATCH, NodeType.LN_USER);
        }

        @Override
        public boolean compareTypeAndVersion(int versionA,
                                             int versionB,
                                             byte typeB) {
            /* If the other entry is newer, the types should match. */
            if (versionA <= versionB && getTypeNum() == typeB) {
                return true;
            }

            /*
             * If the other entry is older, the type might be an old,
             * deprecated, equivalent type.
             */
            if ((versionA > versionB) && isEquivalentOldType(typeB)) {
                return true;
            }

            /*
             * In this case, the other entry's version is older or newer but it
             * doesn't matter -- it's completely the wrong type.
             */
            return false;
        }

        private boolean isEquivalentOldType(byte typeB) {
            if ((!isTransactional() && (typeB == LOG_OLD_LN.getTypeNum())) ||
                (isTransactional() &&
                 (typeB == LOG_OLD_LN_TRANSACTIONAL.getTypeNum()))) {
                return true;
            }

            return false;
        }
    }
}
