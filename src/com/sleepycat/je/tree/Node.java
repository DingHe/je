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

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.Loggable;

/**
 * A Node contains all the common base information for any JE B-Tree node.
 */
// Node 是 JE 中所有 B 树节点（B-Tree Node）以及叶子记录的抽象基类（Abstract Base Class），它实现了 Loggable 接口。
// 核心定位：它封装了 JE B 树中所有节点和记录（如内部节点 IN、叶子节点 LN 等）所共有的基础行为与规范。
// 统一接口：为上层代码提供多态调用的统一入口，例如节点的锁管理、类型判断（是否为 LN 或 IN）、内存统计、日志类型获取以及调试信息打印等。
// 行为约束：通过抽象方法（如 rebuildINList、isValidForDelete、incFetchStats 等），强制子类实现各自特定的树结构管理和统计逻辑。
public abstract class Node implements Loggable {

    /* Used to mean null or none.  See NodeSequence. */
    // 用于表示一个无效的、不存在的或“空”的节点 ID（None / Null）。
    // 在 JE 的节点序列（NodeSequence）及树结构构建中广泛用作边界检查或空指针占位符。
    public static final long NULL_NODE_ID = -1L;

    protected Node() {
    }

    /**
     * Initialize a node that has been faulted in from the log.
     */
    // 初始化从磁盘日志（Log）中被抓取/加载（Faulted in）进内存的节点。
    public void postFetchInit(DatabaseImpl db, long sourceLsn)
        throws DatabaseException {

        /* Nothing to do. */
    }
    // 获取该节点的排他锁（Exclusive Latch）
    public void latch() {
    }
    // 获取该节点的共享锁（Shared Latch）。
    public void latchShared()
        throws DatabaseException {
    }
    // 带缓存模式参数的共享锁获取方法。
    public void latchShared(CacheMode ignore)
        throws DatabaseException {
    }
    // 释放当前节点当前持有的锁（无论是排他锁还是共享锁）。
    public void releaseLatch() {
    }

    /**
     * Since DIN/DBIN/DupCountLN are no longer used in the Btree, this method
     * should normally only be used by dup conversion or entities that do not
     * access records via the Btree.
     *
     * @return true if this node is a duplicate-bearing node type, false
     * if otherwise.
     */
    // 判断当前节点是否包含重复记录（Duplicates）。
    // 注释指出，由于 DIN/DBIN/DupCountLN 在现代 JE B 树中已不再使用，该方法通常仅用于重复键转换或不通过 B 树访问记录的特殊实体。
    public boolean containsDuplicates() {
        return false;
    }

    /**
     * Cover for LN's and just return 0 since they'll always be at the bottom
     * of the tree.
     */
    // 获取节点在 B 树中所处的层级（Level）。
    public int getLevel() {
        return 0;
    }

    /**
     * Add yourself to the in memory list if you're a type of node that
     * should belong.
     */
    // 将节点（如果属于应当纳入内存列表的类型）重新添加到内存 IN 列表中。
    abstract void rebuildINList(INList inList)
        throws DatabaseException;

    /**
     * @return true if you're part of a deletable subtree.
     */
    // 判断当前节点是否属于可删除的子树的一部分。
    abstract boolean isValidForDelete()
        throws DatabaseException;
    // 判断当前节点是否为叶子节点（LN, Leaf Node）。
    public boolean isLN() {
        return false;
    }
    // 判断当前节点是否为内部节点（IN, Internal Node）。
    public boolean isIN() {
        return false;
    }
    // 判断当前节点是否为上层内部节点（Upper IN，即非底部的内部节点）。
    public boolean isUpperIN() {
        return false;
    }
    
    // 判断当前节点是否为底层内部节点（BIN, Bottom Internal Node）。
    public boolean isBIN() {
        return false;
    }
    // 判断当前节点是否为 BIN-delta 节点。
    public boolean isBINDelta() {
        return false;
    }
    // 带加锁检查的 BIN-delta 判断方法。
    public boolean isBINDelta(boolean checkLatched) {
        return false;
    }
    // 判断当前节点是否为旧版重复内部节点（DIN）。
    public boolean isDIN() {
        return false;
    }
    // 判断当前节点是否为旧版重复底层内部节点（DBIN）。
    public boolean isDBIN() {
        return false;
    }

    /**
     * Return the approximate size of this node in memory, if this size should
     * be included in its parents memory accounting.  For example, all INs
     * return 0, because they are accounted for individually. LNs must return a
     * count, they're not counted on the INList.
     */
    // 获取应由其父节点计入内存开销的节点近似大小。
    // 例如，所有 IN 节点返回 0（因为它们在 INList 中被独立统计），而 LN 节点必须返回一个具体的计数值，因为它们不被单独记录在 INList 上，其内存开销需要由父 BIN 承担。
    public long getMemorySizeIncludedByParent() {
        return 0;
    }

    /**
     * Default toString method at the root of the tree.
     */
    // 返回树根节点的默认字符串表示。
    @Override
    public String toString() {
        return this.dumpString(0, true);
    }
    // 将节点的详细调试结构打印到标准输出。
    public void dump(int nSpaces) {
        System.out.print(dumpString(nSpaces, true));
    }
    // 生成包含节点详细信息的字符串。
    String dumpString(int nSpaces, boolean dumpTags) {
        return "";
    }
    // 获取节点的类型名称。
    public String getType() {
        return getClass().getName();
    }

    /**
     * We categorize fetch stats by the type of node, so node subclasses
     * update different stats.
     */
    // 更新节点的抓取（Fetch）统计信息。
    abstract void incFetchStats(EnvironmentImpl envImpl, boolean isMiss);

    /**
     * Returns the generic LogEntryType for this node. Returning the actual
     * type used to log the node is not always possible. Specifically, for LN 
     * nodes the generic type is less specific than the actual type used to log
     * the node:
     *  + A non-transactional type is always returned.
     *  + LOG_INS_LN is returned rather than LOG_UPD_LN.
     *  + LOG_DEL_LN is returned rather than LOG_DEL_DUPLN.
     */
    // 获取该节点的通用日志条目类型（LogEntryType）。
    public abstract LogEntryType getGenericLogType();
    // 获取与该节点关联的事务 ID。
    public long getTransactionId() {
        return 0;
    }
}
