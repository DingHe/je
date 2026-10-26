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

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogParams;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.VLSN;

/**
 * An LN represents a Leaf Node in the JE tree.
 */
// 真实数据的绝对载体：存储用户写入数据库的实际数据字节（data）。它的父节点（BIN）维护键（Key）和物理/内存位置（LSN/Node Reference），而 LN 承载实际的值（Value）。
// 记录状态与删除标记：当 data == null 时，该 LN 代表一个逻辑已删除（Tombstone / Deleted）的记录。
// 日志序列化与事务联动：实现把节点数据打包成日志（LOG_INS_LN、LOG_UPD_LN、LOG_DEL_LN 等）写入磁盘日志管理器（LogManager），并在分布式/事务模式下配合写锁（WriteLockInfo）生成 Undo/Redo 恢复数据。
// 内存与冷热状态追踪：通过非持久化的标志位（flags）追踪当前节点是否为内存“脏（Dirty）”数据，以及在内存驱逐（Eviction）策略中是否属于冷数据（FETCHED_COLD_BIT）。
public class LN extends Node implements VersionedWriteLoggable {
    // XML 格式化转储（Dump）调试时，标志 LN 节点开始的标签。
    private static final String BEGIN_TAG = "<ln>";
    private static final String END_TAG = "</ln>";

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    // 记录该类最后一次发生日志格式变更时的日志版本号（Version 8）。
    private static final int LAST_FORMAT_CHANGE = 8;
    // 存储实际用户数据的字节数组。
    // 如果为 null 代表当前记录已被删除；如果为 ZERO_LENGTH_BYTE_ARRAY 代表长度为 0 的空数据。
    private byte[] data;

    /*
     * Flags: bit fields
     *
     * -Dirty means that the in-memory version is not present on disk.
     */
    // 标志位掩码，第 31 位代表当前内存 LN 相比磁盘镜像已被修改（Dirty）。
    private static final int DIRTY_BIT = 0x80000000;
    // 用于按位与清除脏标志的掩码。
    private static final int CLEAR_DIRTY_BIT = ~DIRTY_BIT;
    // 用于按位与清除脏标志的掩码。
    private static final int FETCHED_COLD_BIT = 0x40000000;
    // 组合非持久化标志位字段（包含 DIRTY_BIT 和 FETCHED_COLD_BIT）。
    private int flags; // not persistent

    /**
     * Create an empty LN, to be filled in from the log.  If VLSNs are
     * preserved for this environment, a VersionedLN will be created instead.
     */
    public LN() {
        this.data = null;
    }

    /**
     * Create a new LN from a byte array.  Pass a null byte array to create a
     * deleted LN.
     *
     * Does NOT copy the byte array, so after calling this method the array is
     * "owned" by the Btree and should not be modified.
     */
    public static LN makeLN(EnvironmentImpl envImpl, byte[] dataParam) {
        if (envImpl.getPreserveVLSN()) {
            return new VersionedLN(dataParam);
        }
        return new LN(dataParam);
    }

    /**
     * Create a new LN from a DatabaseEntry. Makes a copy of the byte array.
     */
    public static LN makeLN(EnvironmentImpl envImpl, DatabaseEntry dbt) {
        if (envImpl.getPreserveVLSN()) {
            return new VersionedLN(dbt);
        }
        return new LN(dbt);
    }

    /**
     * Does NOT copy the byte array, so after calling this method the array is
     * "owned" by the Btree and should not be modified.
     */
    LN(final byte[] data) {

        if (data == null) {
            this.data = null;
        } else if (data.length == 0) {
            this.data = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        } else {
            this.data = data;
        }

        setDirty();
    }

    /**
     * Makes a copy of the byte array.
     */
    LN(DatabaseEntry dbt) {
        byte[] dat = dbt.getData();
        if (dat == null) {
            data = null;
        } else if (dbt.getPartial()) {
            init(dat,
                 dbt.getOffset(),
                 dbt.getPartialOffset() + dbt.getSize(),
                 dbt.getPartialOffset(),
                 dbt.getSize());
        } else {
            init(dat, dbt.getOffset(), dbt.getSize());
        }
        setDirty();
    }

    /** For Sizeof. */
    public LN(SizeofMarker marker, DatabaseEntry dbt) {
        this(dbt);
    }

    private void init(byte[] data, int off, int len, int doff, int dlen) {
        if (len == 0) {
            this.data = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        } else {
            this.data = new byte[len];
            System.arraycopy(data, off, this.data, doff, dlen);
        }
    }

    private void init(byte[] data, int off, int len) {
        init(data, off, len, 0, len);
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDeleted() {
        return (data == null);
    }

    @Override
    public boolean isLN() {
        return true;
    }
    // 删除数据
    void makeDeleted() {
        data = null;
    }

    public boolean isDirty() {
        return ((flags & DIRTY_BIT) != 0);
    }

    public void setDirty() {
        flags |= DIRTY_BIT;
    }

    public void clearDirty() { // TODO make private
        flags &= CLEAR_DIRTY_BIT;
    }

    public boolean getFetchedCold() {
        return ((flags & FETCHED_COLD_BIT) != 0);
    }

    public void setFetchedCold(boolean val) {
        if (val) {
            flags |= FETCHED_COLD_BIT;
        } else {
            flags &= ~FETCHED_COLD_BIT;
        }
    }

    @Override
    public void postFetchInit(DatabaseImpl db, long sourceLsn) {
        super.postFetchInit(db, sourceLsn);

        /*
         * This flag is initially true for a fetched LN, and will be set to
         * false if the LN is accessed with any CacheMode other than UNCHANGED.
         */
        setFetchedCold(true);
    }

    /**
     * Called by CursorImpl to get the record version.
     *
     * If VLSNs are not preserved for this environment, returns -1 which is the
     * sequence for VLSN.NULL_VLSN.
     *
     * If VLSNs are preserved for this environment, this method is overridden
     * by VersionedLN which returns the VLSN sequence.
     */
    public long getVLSNSequence() {
        return VLSN.NULL_VLSN_SEQUENCE;
    }

    /**
     * Called by LogManager after writing an LN with a newly assigned VLSN, and
     * called by LNLogEntry after reading the LN with the VLSN from the log
     * entry header.
     *
     * If VLSNs are not preserved for this environment, does nothing.
     *
     * If VLSNs are preserved for this environment, this method is overridden
     * by VersionedLN which stores the VLSN sequence.
     */
    public void setVLSNSequence(long seq) {
        /* Do nothing. */
    }

    /*
     * If you get to an LN, this subtree isn't valid for delete. True, the LN
     * may have been deleted, but you can't be sure without taking a lock, and
     * the validate -subtree-for-delete process assumes that bin compressing
     * has happened and there are no committed, deleted LNS hanging off the
     * BIN.
     */
    @Override
    boolean isValidForDelete() {
        return false;
    }

    /**
     * Returns true by default, but is overridden by MapLN to prevent eviction
     * of open databases.  This method is meant to be a guaranteed check and is
     * used after a BIN has been selected for LN stripping but before actually
     * stripping an LN. [#13415]
     * @throws DatabaseException from subclasses.
     */
    boolean isEvictable(long lsn)
        throws DatabaseException {

        return true;
    }
    // 将当前 LN 标记为逻辑删除态（将底层 data 设为 null），并将其设为脏数据（setDirty()），以便后续写入删除日志。
    public void delete() {
        makeDeleted();
        setDirty();
    }
    // 用新的字节数组替换旧的记录内容，并更新脏状态。
    public void modify(byte[] newData) {
        data = newData;
        setDirty();
    }

    /**
     * Sets data to empty and returns old data.  Called when converting an old
     * format LN in a duplicates DB.
     */
    public byte[] setEmpty() {
        final byte[] retVal = data;
        data = Key.EMPTY_KEY;
        return retVal;
    }

    /**
     * Add yourself to the in memory list if you're a type of node that should
     * belong.
     */
    @Override
    void rebuildINList(INList inList) {
        /*
         * Don't add, LNs don't belong on the list.
         */
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes.
     */
    @Override
    public long getMemorySizeIncludedByParent() {
        int size = MemoryBudget.LN_OVERHEAD;
        if (data != null) {
            size += MemoryBudget.byteArraySize(data.length);
        }
        return size;
    }

    /**
     * Release the memory budget for any objects referenced by this
     * LN. For now, only release treeAdmin memory, because treeMemory
     * is handled in aggregate at the IN level. Over time, transition
     * all of the LN's memory budget to this, so we update the memory
     * budget counters more locally. Called when we are releasing a LN
     * for garbage collection.
     */
    public void releaseMemoryBudget() {
        // nothing to do for now, no treeAdmin memory
    }

    public long getTreeAdminMemory() {
        return 0;
    }

    /*
     * Dumping
     */

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder self = new StringBuilder();
        if (dumpTags) {
            self.append(TreeUtils.indent(nSpaces));
            self.append(beginTag());
            self.append('\n');
        }

        self.append(super.dumpString(nSpaces + 2, true));
        self.append('\n');
        if (data != null) {
            self.append(TreeUtils.indent(nSpaces+2));
            self.append("<data>");
            self.append(Key.DUMP_TYPE.dumpByteArray(data));
            self.append("</data>");
            self.append('\n');
        }
        if (dumpTags) {
            self.append(TreeUtils.indent(nSpaces));
            self.append(endTag());
        }
        return self.toString();
    }

    /*
     * Logging Support
     */

    /**
     * Convenience logging method.  See logInternal.
     *
     * For a deferred-write database, the logging will not actually occur and
     * a transient LSN will be created and returned if the currLsn is NULL;
     * otherwise the currLsn is returned. 
     *
     * However, if the embeddedness of the LN changes, we must create a real
     * logrec for the op. The following scenario is an example of what can
     * go wrong if we don't log embeddedness changes:
     *
     * - R1 exists before DB is opened in DW mode.
     * - R1 is embedded, so its on-disk image has been counted obsolete and
     *   might have been deleted already.
     * - R1 is updated in DW mode, and its new version, R2, is not embedded
     *   (and not logged). Furthermore, the slot LSN points to R1 still.
     * - R2 gets evicted and logged. As a result, R1 will be counted as
     *   obsolete again, because the slot points to R1 and says R1 is not
     *   embedded. 
     *
     * If we do log embeddedness changes, then in the above scenario, R2 will
     * be logged and this logging will not count R1 as obsolete, because the
     * embedded flag in the slot still says that R1 is embedded. After R2 is
     * logged, the slot is updated (both the slot LSN and the embedded flag).
     *
     * In general, the embedded flag in the slot must be in accord with the
     * embedded flag in the last logged version of a record, even if multiple
     * updates were done on the record since the last time it was logged and
     * the current logging event. 
     */
    public LogItem optionalLog(
        EnvironmentImpl envImpl,
        DatabaseImpl dbImpl,
        Locker locker,
        WriteLockInfo writeLockInfo,
        boolean newEmbeddedLN,
        byte[] newKey,
        int newExpiration,
        boolean newExpirationInHours,
        boolean currEmbeddedLN,
        long currLsn,
        int currSize,
        boolean isInsertion,
        ReplicationContext repContext)
        throws DatabaseException {

        if (dbImpl.isDeferredWriteMode() && currEmbeddedLN == newEmbeddedLN) {
            final LogItem item = new LogItem();
            item.lsn = assignTransientLsn(envImpl, dbImpl, currLsn, locker);
            item.size = -1;
            return item;
        } else {
            return logInternal(
                envImpl, dbImpl, locker, writeLockInfo,
                newEmbeddedLN, newKey, newExpiration, newExpirationInHours,
                currEmbeddedLN, currLsn, currSize,
                isInsertion, false /*backgroundIO*/, repContext);
        }
    }

    /**
     * Convenience logging method, used to migrate an LN during cleaning.
     * See logInternal.
     */
    public LogItem log(
        EnvironmentImpl envImpl,
        DatabaseImpl dbImpl,
        Locker locker,
        WriteLockInfo writeLockInfo,
        boolean newEmbeddedLN,
        byte[] newKey,
        int newExpiration,
        boolean newExpirationInHours,
        boolean currEmbeddedLN,
        long currLsn,
        int currSize,
        boolean isInsertion,
        boolean backgroundIO,
        ReplicationContext repContext)
        throws DatabaseException {

        return logInternal(
            envImpl, dbImpl, locker, writeLockInfo,
            newEmbeddedLN, newKey, newExpiration, newExpirationInHours,
            currEmbeddedLN, currLsn, currSize,
            isInsertion, backgroundIO, repContext);
    }

    /**
     * Generate and write to the log a logrec describing an operation O that
     * is being performed on a record R with key K. O may be an insertion,
     * update, deletion, migration, or in the case of a DW DB, an eviction
     * or checkpoint of a dirty LN.
     *
     * Let T be the locker performing O. T is null in case of DW eviction/ckpt.
     * Otherwise, T holds a lock on R and it will keep that lock until it
     * terminates. In case of a CUD op, the lock is an exclusive one; in 
     * case of LN migration, it's a shared one (and T is non-transactional).
     *
     * - Let Rc be the current version of R (before O). The absence of R from
     *   the DB is considered as a special "deleted" version. Rc may be the
     *   deleted version.
     * - If T is a Txn, let Ra be the version of R before T write-locked R. Ra
     *   may be the deleted version. Ra and Rc will be the same if O is the
     *   very 1st op on R by T.
     * - Let Rn be R's new version (after O). Rc and Rn will be the same if O
     *   is migration or DW eviction/ckpt.
     *
     * - Let Ln be the LSN of the logrec that will be generated here to
     *   describe O.
     * - Let Lc be the current LSN value in R's slot, or NULL if no such slot
     *   exists currently. If an R slot exists, then for a non-DW DB, Lc points
     *   to Rc, or may be NULL if Rc is the deleted version. But for a DW DB,
     *   Lc may point to an older version than Rc, or it may be transient.
     * - If T is a Txn, let La be the LSN value in R's slot at the time T 
     *   write-locked R, or NULL if no such slot existed at that time. 
     *
     * @param isInsertion Whether this CUDop is an insertion (possibly with
     * slot reuse.
     *
     * @param locker The locker T. If non-null, a write lock will be acquired
     * by T on Ln's LSN.
     *
     * WARNING: Be sure to pass null for the locker param if the new LSN should
     * not be locked.
     *
     * @param writeLockInfo It is non-null if and only if T is a Txn. It
     * contains info that must be included in Ln to make it undoable if T
     * aborts. Specifically, it contains:
     *
     * - abortKD   : True if Ra is the deleted version; false otherwise.
     * - abortLSN  : The La LSN as defined above.
     * - abortKey  : The key of Ra, if Ra was embedded in the parent BIN and
     *               the containing DB allows key updates.
     * - abortData : The data of Ra, if Ra was embedded in the parent BIN.
     *
     * When the new LSN is write-locked, a new WriteLockInfo is created and
     * the above info is copied into it. Normally this parameter should be
     * obtained from the prepareForInsert or prepareForUpdate method of
     * CursorImpl.LockStanding.
     *
     * @param newEmbeddedLN Whether Rn will be embedded into the parent BIN.
     * If true, Ln will be counted as an "immediately obsolete" logrec.
     *
     * @param newKey Rn's key. Note: Rn's data is not passed as a parameter to
     * this method because it is stored in this LN. Rn (key and data) will be
     * stored in Ln. Rn's key will also be stored in the parent BIN, and if
     * newEmbeddedLN is true, Rn's data too will be stored there.
     *
     * @param newExpiration the new expiration time in days or hours.
     *
     * @param newExpirationInHours whether the new expiration time is in hours.
     *
     * @param currEmbeddedLN Whether Rc's data is embedded into the parent
     * BIN. If true, Lc has already been counted obsolete.
     *
     * @param currLsn The Lc LSN as defined above. Is given as a param to this
     * method to count the associated logrec as obsolete (which must done under
     * the LWL), if it has not been counted already.
     *
     * @param currSize The size of Lc (needed for obsolete counting).
     *
     * @param isInsertion True if the operation is an insertion (including
     * slot reuse). False otherwise.
     */
    // 主要作用是将对某条记录（Record）进行的各种操作（如插入、更新、删除、迁移、后台检查点落盘等）生成对应的日志条目（Log Entry），
    // 并将其写入磁盘日志管理器（LogManager），
    // 同时在事务环境下处理好 Undo/Redo 恢复元数据、废弃空间回收（Obsolete Counting）以及新生成 LSN 的写锁绑定。
    private LogItem logInternal(
        // JE 运行时环境实例。
        final EnvironmentImpl envImpl,
        // 当前操作所属的数据库实例。
        final DatabaseImpl dbImpl,
        // 执行当前操作的事务或锁定代理（如果是后台 Cleaner 迁移或 DW 异步落盘，此参数可为 null）。
        final Locker locker,
        // 事务写锁信息对象。如果是由事务执行，它包含了用于事务中止（Abort/Rollback）时进行数据回滚所需的 Undo 元数据（如 abortLsn、abortKey、abortData 等）。
        final WriteLockInfo writeLockInfo,
        // 新版本记录是否要作为“嵌入式/内联 LN”（直接存放在父 BIN 槽位中）。
        final boolean newEmbeddedLN,
        // 新版本的键（Key）
        final byte[] newKey,
        // 新的 TTL 过期时间（以天或小时为单位）。
        final int newExpiration,
        // 过期时间单位是否为小时。
        final boolean newExpirationInHours,
        // 当前旧版本记录是否是内联存储的。
        final boolean currEmbeddedLN,
        // 当前槽位中记录的旧 LSN 值（用于计算废弃空间）。
        final long currLsn,
        // 旧 LSN 对应日志记录的大小（用于废弃空间回收）。
        final int currSize,
        // 指示本次操作是否为插入（或槽位复用）。
        final boolean isInsertion,
        // 是否为后台 I/O 操作（如 Cleaner 触发的迁移）。
        final boolean backgroundIO,
        // 高可用复制（HA）上下文信息。
        final ReplicationContext repContext)
        throws DatabaseException {
        // 确保当前对象只能是标准 LN 或高可用 VersionedLN，且非这两个类时不能使用内联特性。
        assert(getClass() == LN.class ||
               getClass() == VersionedLN.class ||
               !newEmbeddedLN);
        // 如果当前环境处于只读模式（Read-Only），严禁写入日志，直接抛出状态异常。
        if (envImpl.isReadOnly()) {
            /* Returning a NULL_LSN will not allow locking. */
            throw EnvironmentFailureException.unexpectedState(
                "Cannot log LNs in read-only env.");
        }

        /*
         * Check that a replicated txn is used for writing to a replicated DB,
         * and a non-replicated locker is used for writing to a
         * non-replicated DB. This is critical for avoiding corruption when HA
         * failover occurs [#23234] [#23330].
         *
         * Two cases are exempt from this rule:
         *
         *  - The locker is null only when performing internal logging (not a
         *    user operation), such as cleaner migration and deferred-write
         *    logging.  This is always non-transactional and non-replicated, so
         *    we can skip this check.  Note that the cleaner may migrate an LN
         *    in a replicated DB, but this is not part ot the rep stream.
         *
         *  - Only NameLNs that identify replicated DBs are replicated, not
         *    all NameLNs in the naming DB, so the naming DB is exempt.
         *
         * This guard should never fire because of two checks made prior to
         * logging:
         *
         *  - When a user txn in a replicated environment is not configured for
         *    local-write and a write operation is attempted (or when the
         *    opposite is true), the Cursor class will throw
         *    UnsupportedOperationException. See Locker.isLocalWrite.
         *
         *  - On a replica, writes to replicated DBs are disallowed even when
         *    local-write is false.  This is enforced by the ReadonlyTxn class
         *    which throws ReplicaWriteException in this case.
         */
        // 复制一致性安全守卫（HA 隔离校验）
        // 在启用了高可用复制（HA）的环境中，严格校验“复制事务只能写复制数据库，非复制事务只能写非复制数据库”，防止主从切换或故障转移时产生数据错乱。
        // DbTree.NAME_DB_ID：JE 内部用于存储所有用户数据库元数据（如数据库名称、ID映射等）的系统命名数据库（NameDB）。
        // 检查当前正在写入的目标数据库是不是 NameDB。如果是，则将其标记为 isNamingDB = true。
        // 因为 NameDB 具有特殊性（它自身可能伴随复制环境存在，但内部的具体记录并不完全遵循普通用户库的隔离规则），所以后续的严格校验会对其进行豁免。
        final boolean isNamingDB = dbImpl.getId().equals(DbTree.NAME_DB_ID);
        // 当前操作的目标不是内部的命名数据库（即普通的业务用户数据库）。
        if (!isNamingDB &&
            // 当前整个 JE 运行环境处于高可用复制（HA）模式下。
            envImpl.isReplicated() &&
            // 当前有明确的事务或锁定代理（Locker）正在执行写入操作（内部无锁迁移等内部操作暂不在此列）。
            locker != null &&
            // 检查“目标数据库是否是复制库（dbImpl.isReplicated()）”与“执行写入的事务是否是复制事务（locker.isReplicated()）”两者是否发生矛盾：
            dbImpl.isReplicated() != locker.isReplicated()) {

            throw EnvironmentFailureException.unexpectedState(
                (locker.isReplicated() ?
                    "Rep txn used to write to non-rep DB" :
                    "Non-rep txn used to write to rep DB") +
                ", class = " + locker.getClass().getName() +
                ", txnId = " + locker.getId() +
                ", dbName = " + dbImpl.getDebugName());
        }

        /*
         * As an additional safeguard, check that a replicated txn is used when
         * the operation is part of the rep stream, and that the inverse is
         * also true. The naming DB is exempt for the same reason as above.
         */
        if (!isNamingDB) {
            // 判定当前事务是否为复制事务
            boolean isRepLocker = (locker != null) && locker.isReplicated();
            // repContext.inReplicationStream()：当前操作是否正在复制流（Replication Stream）中执行。
            // 例如，当从节点（Replica）收到主节点（Master）发来的日志并进行重放（Replay）时，该方法会返回 true；而如果是本地客户端直接发起的普通写入，则返回 false。
            if (repContext.inReplicationStream() != isRepLocker) {
                throw EnvironmentFailureException.unexpectedState(
                    (isRepLocker ?
                        "Rep txn used to write outside of rep stream" :
                        "Non-rep txn used to write in rep stream") +
                    ((locker != null) ?
                        (", class = " + locker.getClass().getName() +
                         ", txnId = " + locker.getId()) :
                        ", null locker") +
                    ", dbName = " + dbImpl.getDebugName());
            }
        }
        // 初始化事务与 Undo 回滚元数据
        // 声明日志条目类型（例如是插入、更新还是删除，是否带有事务）。
        LogEntryType entryType;
        // 声明底层的事务对象，默认设为 null（适用于非事务操作或后台内部日志写入）。
        Txn txn = null;
        // 事务中止时需要回滚到的目标 LSN（即该事务修改该记录前的原始 LSN，称为 $La$）。默认无。
        long abortLsn = DbLsn.NULL_LSN;
        // Abort Known Deleted 标记。指示在事务加锁前，该记录是否处于“已知删除”状态。
        boolean abortKD = false;
        // 中止时的键（用于内联存储在父 BIN 中的记录键回滚）。
        byte[] abortKey = null;
        // 中止时的数据（用于内联存储在父 BIN 中的记录值回滚）。
        byte[] abortData = null;
        // 中止时的复制版本序列号。
        long abortVLSN = VLSN.NULL_VLSN_SEQUENCE;
        // 中止时的 TTL 过期时间及单位。
        int abortExpiration = 0;
        boolean abortExpirationInHours = false;
        // 实例化一个日志参数容器对象，后续所有传递给日志管理器（LogManager）的参数都会封装在这个对象中。
        LogParams params = new LogParams();
        // 事务分支处理
        if (locker != null && locker.isTransactional()) {
            // 如果当前在事务中，调用 getLogType 并传入 isTransactional = true，获取带事务特性的日志类型（例如 LOG_INS_LN_TRANSACTIONAL、LOG_UPD_LN_TRANSACTIONAL 等）。
            entryType = getLogType(isInsertion, true, dbImpl);
            // 从当前的锁定代理（locker）中安全地提取出真正的事务对象 Txn，并通过断言确保其不为空。
            txn = locker.getTxnLocker();
            assert(txn != null);
            // 提取 Undo 元数据
            // 通过游标加写锁时生成的 writeLockInfo，一口气拉取了事务开始修改该记录前的所有“快照状态”（即 Abort LSN、是否删除、键、数据、VLSN、过期时间等）。
            abortLsn = writeLockInfo.getAbortLsn();
            abortKD = writeLockInfo.getAbortKnownDeleted();
            abortKey = writeLockInfo.getAbortKey();
            abortData = writeLockInfo.getAbortData();
            abortVLSN = writeLockInfo.getAbortVLSN();
            abortExpiration = writeLockInfo.getAbortExpiration();
            abortExpirationInHours = writeLockInfo.isAbortExpirationInHours();
            // 如果该事务曾经发生过回滚重试，允许处理重复的废弃空间计数。
            params.obsoleteDupsAllowed = locker.isRolledBack();

        } else {
            entryType = getLogType(isInsertion, false, dbImpl);
        }
        // 创建日志条目与设置 Provisional 属性
        params.entry = createLogEntry(
            entryType, dbImpl, txn,
            abortLsn, abortKD, abortKey, abortData, abortVLSN,
            abortExpiration, abortExpirationInHours,
            newKey, newEmbeddedLN, newExpiration, newExpirationInHours,
            repContext);

        /*
         * Always log temporary DB LNs as provisional.  This prevents the
         * possibility of a FileNotFoundException during recovery, since
         * temporary DBs are not checkpointed.  And it speeds recovery --
         * temporary DBs are removed during recovery anyway.
         */
        params.provisional =
            (dbImpl.isTemporary() ? Provisional.YES : Provisional.NO);

        /*
         * Dedice whether to count the current record version as obsolete.
         * Rc should not be counted as obsolete if:
         * (a) Rc == Ra; Ra (i.e. abortLsn) will be counted obsolete during
         * commit, or
         * (b) Rc was counted earlier as an "immediately obsolete" logrec.
         * This includes the cases where the DB is a dups DB, or the current
         * op is an insertion (which implies Rc is a deletion and as such has
         * been counted already) or Rc is embedded.
         */
        // 废弃空间计算（Obsolete Counting）
        if (currLsn != abortLsn &&
            !dbImpl.isLNImmediatelyObsolete() &&
            !isInsertion &&
            !currEmbeddedLN) {

            params.oldLsn = currLsn;
            params.oldSize = currSize;
        }

        params.repContext = repContext;
        params.backgroundIO = backgroundIO;
        params.nodeDb = dbImpl;

        /* Save obsolete size information to be used during commit. */
        if (txn != null && currLsn == abortLsn) {
            writeLockInfo.setAbortLogSize(currSize);
        }
        // 执行真实的磁盘日志写入
        LogItem item;
        try {
            if (txn != null) {

                /*
                 * Writing an LN_TX entry requires looking at the Txn's
                 * lastLoggedTxn.  The Txn may be used by multiple threads so
                 * ensure that the view we get is consistent. [#17204]
                 */
                synchronized (txn) {
                    item = envImpl.getLogManager().log(params);
                }
            } else {
                item = envImpl.getLogManager().log(params);
            }
        } catch (Throwable e) {
            /*
             * If any exception occurs while logging an LN, ensure that the
             * environment is invalidated. This will also ensure that the txn
             * cannot be committed.
             */
            if (envImpl.isValid()) {
                throw new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_INCOMPLETE,
                    "LN could not be logged", e);
            } else {
                throw e;
            }
        } finally {

            /*
             * Guarantee that if logging fails, we won't have a dirty LN in
             * the Btree.  This avoids incorrect assertions in other threads.
             */
            clearDirty();
        }

        /**
         * Lock the new LSN immediately after logging, with the BIN latched.
         * Lock non-blocking, since no contention is possible on the new LSN.
         * If the locker is transactional, a new WriteLockInfo is created for
         * the new LSN and stored in the locker. lockResult points to that
         * WriteLockInfo. Since this new WriteLockInfo and the WriteLockInfo
         * given as input to this method refer to the same logical record,
         * the info from the given WriteLockInfo is copied to the new one.
         */
        // 为新生成的 LSN 自动加写锁
        if (locker != null) {
            final long newLsn = item.lsn;

            final LockResult lockResult = locker.nonBlockingLock(
                newLsn, LockType.WRITE, false /*jumpAheadOfWaiters*/, dbImpl);

            assert lockResult.getLockGrant() != LockGrantType.DENIED :
                   DbLsn.getNoFormatString(newLsn);

            lockResult.copyWriteLockInfo(writeLockInfo);
        }

        /* In a dup DB, do not expect embedded LNs or non-empty data. */
        // 重复键数据库防错校验与返回
        if (dbImpl.getSortedDuplicates() &&
            (newEmbeddedLN || (data != null && data.length > 0))) {

            throw EnvironmentFailureException.unexpectedState(
                envImpl,
                "[#25288] emb=" + newEmbeddedLN +
                " key=" + Key.getNoFormatString(newKey) +
                " data=" + Key.getNoFormatString(data) +
                " vlsn=" + item.header.getVLSN() +
                " lsn=" + DbLsn.getNoFormatString(currLsn));
        }

        return item;
    }

    /*
     * Each LN knows what kind of log entry it uses to log itself. Overridden
     * by subclasses.
     */
    LNLogEntry<?> createLogEntry(
        LogEntryType entryType,
        DatabaseImpl dbImpl,
        Txn txn,
        long abortLsn,
        boolean abortKD,
        byte[] abortKey,
        byte[] abortData,
        long abortVLSN,
        int abortExpiration,
        boolean abortExpirationInHours,
        byte[] newKey,
        boolean newEmbeddedLN,
        int newExpiration,
        boolean newExpirationInHours,
        ReplicationContext repContext) {

        return new LNLogEntry<LN>(
            entryType, dbImpl.getId(), txn,
            abortLsn, abortKD, abortKey, abortData, abortVLSN,
            abortExpiration, abortExpirationInHours,
            newKey, this, newEmbeddedLN, newExpiration, newExpirationInHours);
    }

    /**
     * @see Node#incFetchStats
     */
    @Override
    void incFetchStats(EnvironmentImpl envImpl, boolean isMiss) {
        envImpl.getEvictor().incLNFetchStats(isMiss);
    }

    /**
     * @see Node#getGenericLogType
     */
    @Override
    public LogEntryType getGenericLogType() {
        return getLogType(true, false, null);
    }

    protected LogEntryType getLogType(
        boolean isInsert,
        boolean isTransactional,
        DatabaseImpl db) {

        if (db != null) {
            LogEntryType type = db.getDbType().getLogType();
            if (type != null) {
                return type;
            }
        }

        if (isDeleted()) {
            assert !isInsert;
            return isTransactional ?
                   LogEntryType.LOG_DEL_LN_TRANSACTIONAL :
                   LogEntryType.LOG_DEL_LN;
        }

        if (isInsert) {
            return isTransactional ?
                LogEntryType.LOG_INS_LN_TRANSACTIONAL :
                LogEntryType.LOG_INS_LN;
        }

        return isTransactional ?
            LogEntryType.LOG_UPD_LN_TRANSACTIONAL :
            LogEntryType.LOG_UPD_LN;
    }

    /**
     * The first time we optionally-log an LN in a DeferredWrite database,
     * oldLsn will be NULL_LSN and we'll assign a new transient LSN.  When we
     * do subsequent optional-log operations, the old LSN will be non-null and
     * to conserve transient LSNs we'll continue to use the previously assigned
     * LSN rather than assigning a new one.  And of course, when old LSN is
     * persistent we'll continue to use it.
     *
     * If locker is non-null, this method write-locks the new LSN, whether it
     * has been assigned by this method or not.
     */
    private long assignTransientLsn(EnvironmentImpl envImpl,
                                    DatabaseImpl dbImpl,
                                    long oldLsn,
                                    Locker locker) {
        final long newLsn;
        if (oldLsn != DbLsn.NULL_LSN) {
            newLsn = oldLsn;
        } else {
            newLsn = envImpl.getNodeSequence().getNextTransientLsn();
        }

        /**
         * Lock immediately after assigning a new LSN, with the BIN latched.
         * Lock non-blocking, since no contention is possible on the new LSN.
         */
        if (locker != null) {
            final LockResult lockResult = locker.nonBlockingLock(
                newLsn, LockType.WRITE, false /*jumpAheadOfWaiters*/, dbImpl);

            assert lockResult.getLockGrant() != LockGrantType.DENIED :
                   DbLsn.getNoFormatString(newLsn);
        }

        return newLsn;
    }

    /**
     * @see VersionedWriteLoggable#getLastFormatChange
     */
    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        return Collections.emptyList();
    }

    @Override
    public int getLogSize() {
        return getLogSize(LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer) {
        writeToLog(
            logBuffer, LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public int getLogSize(final int logVersion, final boolean forReplication) {
        return calcLogSize(isDeleted() ? -1 : data.length);
    }

    /**
     * Calculates log size based on given dataLen, which is negative to
     * calculate the size of a deleted LN.
     */
    private int calcLogSize(int dataLen) {

        int size = 0;

        if (dataLen < 0) {
            size += LogUtils.getPackedIntLogSize(-1);
        } else {
            size += LogUtils.getPackedIntLogSize(dataLen);
            size += dataLen;
        }

        return size;
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {

        if (isDeleted()) {
            LogUtils.writePackedInt(logBuffer, -1);
        } else {
            LogUtils.writePackedInt(logBuffer, data.length);
            LogUtils.writeBytesNoLength(logBuffer, data);
        }
    }

    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        if (entryVersion < 8) {
            /* Discard node ID from older version entry. */
            LogUtils.readLong(itemBuffer, entryVersion < 6 /*unpacked*/);
        }

        if (entryVersion < 6) {
            boolean dataExists = LogUtils.readBoolean(itemBuffer);
            if (dataExists) {
                data = LogUtils.readByteArray(itemBuffer, true/*unpacked*/);
            }
        } else {
            int size = LogUtils.readInt(itemBuffer, false/*unpacked*/);
            if (size >= 0) {
                data = LogUtils.readBytesNoLength(itemBuffer, size);
            }
        }
    }

    @Override
    public boolean hasReplicationFormat() {
        return false;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {
        return false;
    }

    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof LN)) {
            return false;
        }

        LN otherLN = (LN) other;

        if (!Arrays.equals(getData(), otherLN.getData())) {
            return false;
        }

        return true;
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append(beginTag());

        if (data != null) {
            sb.append("<data>");
            if (verbose) {
                sb.append(Key.DUMP_TYPE.dumpByteArray(data));
            } else {
                sb.append("hidden");
            }
            sb.append("</data>");
        }

        dumpLogAdditional(sb, verbose);

        sb.append(endTag());
    }

    public void dumpKey(StringBuilder sb, byte[] key) {
        sb.append(Key.dumpString(key, 0));
    }

    /*
     * Allows subclasses to add additional fields before the end tag.
     */
    protected void dumpLogAdditional(StringBuilder sb,
                                     @SuppressWarnings("unused")
                                     boolean verbose) {
    }

    /**
     * Account for FileSummaryLN's extra marshaled memory. [#17462]
     */
    public void addExtraMarshaledMemorySize(BIN parentBIN) {
        /* Do nothing here.  Overwridden in FileSummaryLN. */
    }

    /*
     * DatabaseEntry utilities
     */

    /**
     * Copies the non-deleted LN's byte array to the entry.  Does not support
     * partial data.
     */
    public void setEntry(DatabaseEntry entry) {
        assert !isDeleted();
        int len = data.length;
        byte[] bytes = new byte[len];
        System.arraycopy(data, 0, bytes, 0, len);
        entry.setData(bytes);
    }

    /**
     * Copies the given byte array to the given destination entry, copying only
     * partial data if the entry is specified to be partial.  If the byte array
     * is null, clears the entry.
     */
    public static void setEntry(DatabaseEntry dest, byte[] bytes) {

        if (bytes != null) {
            boolean partial = dest.getPartial();
            int off = partial ? dest.getPartialOffset() : 0;
            int len = partial ? dest.getPartialLength() : bytes.length;
            if (off + len > bytes.length) {
                len = (off > bytes.length) ? 0 : bytes.length  - off;
            }

            byte[] newdata = null;
            if (len == 0) {
                newdata = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
            } else {
                newdata = new byte[len];
                System.arraycopy(bytes, off, newdata, 0, len);
            }
            dest.setData(newdata);
            dest.setOffset(0);
            dest.setSize(len);
        } else {
            dest.setData(null);
            dest.setOffset(0);
            dest.setSize(0);
        }
    }


    /**
     * Copies the given source entry to the given destination entry, copying
     * only partial data if the destination entry is specified to be partial. 
     */
    public static void setEntry(DatabaseEntry dest, DatabaseEntry src) {

        if (src.getData() != null) {
            byte[] srcBytes = src.getData();
            boolean partial = dest.getPartial();
            int off = partial ? dest.getPartialOffset() : 0;
            int len = partial ? dest.getPartialLength() : srcBytes.length;
            if (off + len > srcBytes.length) {
                len = (off > srcBytes.length) ? 0 : srcBytes.length  - off;
            }

            byte[] newdata = null;
            if (len == 0) {
                newdata = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
            } else {
                newdata = new byte[len];
                System.arraycopy(srcBytes, off, newdata, 0, len);
            }
            dest.setData(newdata);
            dest.setOffset(0);
            dest.setSize(len);
        } else {
            dest.setData(null);
            dest.setOffset(0);
            dest.setSize(0);
        }
    }

    /**
     * Returns a byte array that is a complete copy of the data in a
     * non-partial entry.
     */
    public static byte[] copyEntryData(DatabaseEntry entry) {
        assert !entry.getPartial();
        int len = entry.getSize();
        final byte[] newData =
            (len == 0) ? LogUtils.ZERO_LENGTH_BYTE_ARRAY : (new byte[len]);
        System.arraycopy(entry.getData(), entry.getOffset(),
                         newData, 0, len);
        return newData;
    }

    /**
     * Merges the partial entry with the given byte array, effectively applying
     * a partial entry to an existing record, and returns a enw byte array.
     */
    public static byte[] resolvePartialEntry(DatabaseEntry entry,
                                             byte[] foundDataBytes ) {
        assert foundDataBytes != null;
        final int dlen = entry.getPartialLength();
        final int doff = entry.getPartialOffset();
        final int origlen = foundDataBytes.length;
        final int oldlen = (doff + dlen > origlen) ? (doff + dlen) : origlen;
        final int len = oldlen - dlen + entry.getSize();

        final byte[] newData;
        if (len == 0) {
            newData = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        } else {
            newData = new byte[len];
        }
        int pos = 0;

        /* Keep 0..doff of the old data (truncating if doff > length). */
        int slicelen = (doff < origlen) ? doff : origlen;
        if (slicelen > 0) {
            System.arraycopy(foundDataBytes, 0, newData, pos, slicelen);
        }
        pos += doff;

        /* Copy in the new data. */
        slicelen = entry.getSize();
        System.arraycopy(entry.getData(), entry.getOffset(), newData, pos,
                         slicelen);
        pos += slicelen;

        /* Append the rest of the old data (if any). */
        slicelen = origlen - (doff + dlen);
        if (slicelen > 0) {
            System.arraycopy(foundDataBytes, doff + dlen, newData, pos,
                             slicelen);
        }

        return newData;
    }
}
