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

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.VLSN;

/**
  * VersionedLN is used to  provide an in-memory representation of an LN that
  * makes its VLSN available through btree access.
  *
  * On disk, each log entry is composed of a header (je.log.LogEntryHeader) and
  * a body (je.log.entry.LogEntry). When an LN is materialized in the Btree, it
  * usually holds only the body, and does not have access to information in the
  * log entry header, such as the VLSN. Since version based API operations need
  * access to the VLSN, environments which are configured with
  * je.rep.preserveRecordVersion=true instantiate VersionedLNs instead of LNs,
  * in order to cache the VLSN with the LN, and make it cheaply available to
  * Btree operations. 
  */
// 核心作用是：在内存中为叶子节点（LN）直接缓存并提供 VLSN（Version/Replicated Log Sequence Number，版本复制序列号）的访问能力。
// 背景与痛点：在磁盘日志中，每一条记录的日志都由日志头（LogEntryHeader，包含 VLSN）和日志体（LogEntry，即 LN 的实际数据）两部分组成。当 LN 被加载（Materialize）到内存的 B 树中时，标准 LN 对象通常只保留日志体，并不直接持有日志头中的 VLSN 信息。
// 解决方案：当开启了记录版本保留配置（如 je.rep.preserveRecordVersion=true）时，系统会实例化 VersionedLN 来替代普通的 LN。它在内存中多持有一个 VLSN 字段，使得 B 树操作可以低成本、直接地获取到该记录的 VLSN，从而支持基于版本的 API 操作。
public class VersionedLN extends LN {
    // 存储当前 VersionedLN 关联的复制版本序列号（VLSN）。
    private long vlsnSequence = VLSN.NULL_VLSN_SEQUENCE;

    public VersionedLN() {
    }

    VersionedLN(byte[] data) {
        super(data);
    }

    VersionedLN(DatabaseEntry dbt) {
        super(dbt);
    }

    /** For Sizeof. */
    public VersionedLN(SizeofMarker marker, DatabaseEntry dbt) {
        super(dbt);
    }

    @Override
    public long getVLSNSequence() {
        return vlsnSequence;
    }

    @Override
    public void setVLSNSequence(long seq) {
        vlsnSequence = seq;
    }

    /**
     * Add additional size taken by this LN subclass.
     */
    @Override
    public long getMemorySizeIncludedByParent() {
        return super.getMemorySizeIncludedByParent() +
               MemoryBudget.VERSIONEDLN_OVERHEAD;
    }
}
