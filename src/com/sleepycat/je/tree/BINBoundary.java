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

/**
 * Contains information about the BIN returned by a search.
 */
// 在执行 B+树搜索（如 Tree.search）的过程中，作为一个轻量级的“状态返回载体”，用来向调用方指示所定位到的目标 BIN（Bottom Internal Node）是否处于整棵树或数据库的极端边界（即它是不是全库的第一个 BIN 或者是最后一个 BIN）。
// 应用场景：当上层游标（Cursor）进行范围扫描（Range Scan）或定位时，需要提前知道当前位置是否已经到达了数据库的边界（例如判断是否还能继续向前或向后翻页遍历），此时就会传入一个 BINBoundary 实例，由搜索算法在寻路过程中动态维护其边界标志。
public class BINBoundary {
    /** The last BIN was returned. */
    // 指示标志。如果其值为 true，代表通过树搜索返回的那个 BIN 是当前数据库中最后一个（最右侧的）BIN；如果为 false，则说明后面还有更多的 BIN。
    public boolean isLastBin;
    /** The first BIN was returned. */
    // 指示标志。如果其值为 true，代表通过树搜索返回的那个 BIN 是当前数据库中第一个（最左侧的）BIN；如果为 false，则说明前面还有更早的 BIN。
    public boolean isFirstBin;
}
