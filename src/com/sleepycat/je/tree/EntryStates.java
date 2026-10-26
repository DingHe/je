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

// 核心作用：在 B 树节点（如 IN 或 BIN）中，为了以最高效的内存方式记录和管理每个槽位（Slot）的各种运行时状态，JE 为每个槽位分配了一个字节（Byte）。
// EntryStates 类定义了一系列二进制掩码（Bitmask），通过位运算（按位与、按位或、取反），允许单个字节同时容纳多达数种独立的布尔状态标志。
public class EntryStates {

    /*
     * If we run out of bits, the two OFFHEAP bits could re-use any of the bit
     * values, since the former only appear on level 2 INs and the latter only
     * on BINs.
     */
    // 已知删除标志位（对应二进制的第 0 位）
    // 当某个槽位上的记录已经被删除，且执行该删除操作的事务已经成功提交（Commit）后，该位会被置为 1。系统在遍历树时会利用此标志快速跳过已删除条目。
    static final byte KNOWN_DELETED_BIT = 0x1;
    // 清除已知删除标志的掩码。
    // 通过与状态字节执行按位与（&）操作，可以将该位清零。
    static final byte CLEAR_KNOWN_DELETED_BIT = ~0x1;
    // 脏数据标志位（对应二进制的第 1 位）。
    // 表示该槽位的内容在内存中已被修改、插入或更新，但尚未被持久化写入磁盘日志中（等待检查点 Checkpoint 刷盘）。
    static final byte DIRTY_BIT = 0x2;
    // 清除脏数据标志的掩码。
    static final byte CLEAR_DIRTY_BIT = ~0x2;
    // 堆外缓存脏标志位（对应二进制的第 2 位）。
    // 专门用于托管在堆外缓存（Off-Heap Cache）中的 BIN 节点，指示其在堆外内存中的对应块已被修改。
    static final byte OFFHEAP_DIRTY_BIT = 0x4;
    // 清除堆外缓存脏标志的掩码。
    static final byte CLEAR_OFFHEAP_DIRTY_BIT = ~0x4;
    // 表示删除操作正在进行中或处于未提交的挂起状态，尚未最终确认。
    static final byte PENDING_DELETED_BIT = 0x8;
    static final byte CLEAR_PENDING_DELETED_BIT = ~0x8;
    // 嵌入式 LN 标志位（对应二进制的第 4 位）。
    // 当记录的数据体非常小（小于 je.tree.maxEmbeddedLN）时，系统会将其键和数据直接组合并内联存储在树节点槽位中，此位即代表内联状态。
    static final byte EMBEDDED_LN_BIT = 0x10;
    static final byte CLEAR_EMBEDDED_LN_BIT = ~0x10;
    // 无数据 LN 标志位（对应二进制的第 5 位）。
    // 当需要嵌入存储的记录完全没有数据体（只有键）时，系统会开启此标志位。这样可以避免使用标准的“两部分键（DupKeyData）”格式，从而节省额外的空间开销。
    static final byte NO_DATA_LN_BIT = 0x20;
    static final byte CLEAR_NO_DATA_LN_BIT = ~0x20;
    // 堆外缓存优先级 2 标志位（对应二进制的第 6 位）。
    // 用于堆外缓存管理器的淘汰算法或优先级策略判定。
    static final byte OFFHEAP_PRI2_BIT = 0x40;
    static final byte CLEAR_OFFHEAP_PRI2_BIT = ~0x40;
    // 瞬态/临时标志位的组合掩码。
    static final byte TRANSIENT_BITS = OFFHEAP_DIRTY_BIT | OFFHEAP_PRI2_BIT;
    static final byte CLEAR_TRANSIENT_BITS = ~TRANSIENT_BITS;
}
