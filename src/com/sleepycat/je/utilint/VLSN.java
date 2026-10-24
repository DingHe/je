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
package com.sleepycat.je.utilint;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;

// VLSN 全称为 Virtual Log Sequence Number（虚拟日志序列号）。它是 JE 分布式复制架构中的基石类
// 为复制日志条目（Replicated Log Entry）提供一个全局唯一的顺序标识符。 在多节点复制组中，无论是日志同步、事务提交、流追赶（Catchup），还是前文探讨过的选举排名（Ranking）计算，都高度依赖 VLSN 来界定日志的新鲜度与一致性边界。
// 唯一标识复制日志：替代或补充了物理 LSN（Log Sequence Number），为网络传输和分布式存储中的每条复制日志提供抽象的逻辑序号。
// 支持边界与空状态处理：内置了 NULL_VLSN（无效/空）、FIRST_VLSN（起始序号 1）以及 UNINITIALIZED_VLSN_SEQUENCE（未初始化），完美处理各种边界和历史兼容场景。
public class VLSN implements Loggable, Comparable<VLSN>, Serializable {
    private static final long serialVersionUID = 1L;
    // 磁盘/网络序列化占用字节数
    // 因为底层是 long 类型，所以固定占用 8 个字节。
    public static final int LOG_SIZE = 8;
    // 空 VLSN 的底层数值常量，定义为 -1。
    public static final int NULL_VLSN_SEQUENCE = -1;
    // 全局共享的“空/无效” VLSN 实例（序号为 -1），用于表示未分配或不存在的状态。
    public static final VLSN NULL_VLSN = new VLSN(NULL_VLSN_SEQUENCE);
    // 全局共享的“首个合法” VLSN 实例（序号为 1），代表复制日志的开端。
    public static final VLSN FIRST_VLSN = new VLSN(1);

    /*
     * The distinguished value used to represent VLSN values that have not
     * been set in log entry fields, because the field did not exist in that
     * version of the log or in a non-HA commit/abort variant of a log entry.
     */
    // 未初始化序列号常量（定义为 0）
    // 用于标识那些在旧版本日志或非 HA 提交/中止变体中尚未被赋值的字段。
    public static final int UNINITIALIZED_VLSN_SEQUENCE = 0;

    /*
     * A replicated log entry is identified by a sequence id. We may change the
     * VLSN implementation so it's not a first-class object, in order to reduce
     * its in-memory footprint. In that case, the VLSN value would be a long,
     * and this class would provide static utility methods.
     */
    // 底层的核心序列号数值。
    // 存储实际的 VLSN 长整型数值（例如 1, 2, 3...）。它是整个类的核心数据载体。
    private long sequence;   // sequence number

    public VLSN(long sequence) {
        this.sequence = sequence;
    }

    /**
     * Constructor for VLSNs that are read from disk.
     */
    public VLSN() {
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof VLSN)) {
            return false;
        }

        VLSN otherVLSN = (VLSN) obj;
        return (otherVLSN.sequence == sequence);
    }

    final public boolean equals(VLSN otherVLSN) {
        return (otherVLSN != null) && (otherVLSN.sequence == sequence);
    }

    @Override
    public int hashCode() {
        return Long.valueOf(sequence).hashCode();
    }

    public long getSequence() {
        return sequence;
    }

    public final boolean isNull() {
        return sequence == NULL_VLSN.sequence;
    }

    public static boolean isNull(long sequence) {
        return sequence == NULL_VLSN.sequence;
    }

    /**
     * Return a VLSN which would follow this one.
     */
    // 获取紧随其后的下一个 VLSN。
    public VLSN getNext() {
        return isNull() ? FIRST_VLSN : new VLSN(sequence + 1);
    }

    /**
     * Return a VLSN which would precede this one.
     */
    // 获取紧邻其前的一个 VLSN。
    public VLSN getPrev() {
        return (isNull() || (sequence == 1)) ?
                NULL_VLSN :
                new VLSN(sequence - 1);
    }

    /**
     * Return true if this VLSN's sequence directly follows the "other"
     * VLSN. This handles the case where "other" is a NULL_VLSN.
     */
    // 判断当前 VLSN 是否紧跟在另一个 VLSN 之后
    public boolean follows(VLSN other) {
        return ((other.isNull() && sequence == 1) ||
                ((!other.isNull()) &&
                 (other.getSequence() == (sequence - 1))));
    }

    /**
     * Compares this VLSN's sequence with the specified VLSN's sequence for
     * order. Returns a negative integer, zero, or a positive integer as this
     * sequence is less than, equal to, or greater than the specified sequence.
     */
    @Override
    public int compareTo(VLSN other) {

        if ((sequence == NULL_VLSN.sequence) &&
            (other.sequence == NULL_VLSN.sequence)) {
            return 0;
        }

        if (sequence == NULL_VLSN.sequence) {
            /* If "this" is null, the other VLSN is always greater. */
            return -1;
        }

        if  (other.sequence == NULL_VLSN.sequence) {
            /* If the "other" is null, this VLSN is always greater. */
            return 1;
        }

        long otherSequence = other.getSequence();
        if ((sequence - otherSequence) > 0) {
            return 1;
        } else if (sequence == otherSequence) {
            return 0;
        } else {
            return -1;
        }
    }

    /**
     * Returns the smaller of two VLSNS, ignoring NULL_VLSN values if one value
     * is not NULL_VLSN.
     *
     * @param a a VLSN
     * @param b another VLSN
     * @return the smaller of {@code a} and {@code b}, ignoring NULL_VLSN
     * unless both are NULL_VLSN
     * @throws IllegalArgumentException if either argument is null
     */
    // 计算两个 VLSN 中的较小者
    public static VLSN min(final VLSN a, final VLSN b) {
        if ((a == null) || (b == null)) {
            throw new IllegalArgumentException(
                "The arguments must not be null");
        }
        if (a.isNull()) {
            return b;
        } else if (b.isNull()) {
            return a;
        }
        return (a.compareTo(b) <= 0) ? a : b;
    }

    /**
     * @see Loggable#getLogSize
     */
    // 返回序列化占用的字节数，固定返回 LOG_SIZE（即 8 字节）。
    @Override
    public int getLogSize() {
        return LOG_SIZE;
    }

    /**
     * @see Loggable#writeToLog
     */
    // 将 VLSN 序列化写入日志缓冲区。
    @Override
    public void writeToLog(ByteBuffer buffer) {
        LogUtils.writeLong(buffer, sequence);
    }

    /*
     *  Reading from a byte buffer
     */

    /**
     * @see Loggable#readFromLog
     */
    @Override
    public void readFromLog(ByteBuffer buffer, int entryVersion) {
        sequence = LogUtils.readLong(buffer);
    }

    /**
     * @see Loggable#dumpLog
     */
    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<vlsn v=\"").append(this).append("\">");
    }

    /**
     * @see Loggable#getTransactionId
     */
    @Override
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     */
    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof VLSN)) {
            return false;
        }

        return sequence == ((VLSN) other).sequence;
    }

    @Override
    public String toString() {
        return String.format("%,d", sequence);
    }
}
