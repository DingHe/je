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

import com.sleepycat.je.evictor.Evictor;

/**
 * The base class for the various array representations used by fields
 * associated with an IN node. Storage efficiency, especially when JE is
 * operating in a "cache full" environment is the prime motivation for the
 * various representations.
 * <p>
 * Each representation assumes that all read operations are done under a shared
 * latch and all updates (set, copy and compact) are done under an exclusive
 * latch. As a result, the representations themselves do not make any
 * provisions for synchronization.
 * <p>
 * The callers of all the potentially representation mutating methods:
 * <ol>
 * <li>
 * {@link #set(int, Object, IN)}
 * </li>
 * <li>
 * {@link #copy(int, int, int, IN)}
 * </li>
 * <li>
 * {@link #compact(IN)}
 * </li>
 * </ol>
 * must be careful to save the result value and use it for subsequent
 * operations, since it could represent the new mutated object.
 */
// INArrayRep 是所有与 IN 节点关联的动态数组表示（如子节点目标引用、键集合等）的抽象基类
// 核心动机：极致的空间效率（Storage Efficiency）。特别是在 JE 运行于“缓存饱满（Cache Full）”的高压环境中，数据结构需要根据填充密度在不同形态（如密集数组与稀疏数组、紧凑表示与标准表示）之间灵活切换。
// 变异机制（Mutation）：通过采用策略模式，当对数组进行修改、扩容或压缩时，底层的具体实现类（如稀疏数组表示）可以动态变异（Mutate）为一个全新的对象实例（例如从稀疏变回密集，或反之）。因此，调用方必须捕获方法的返回值以获取更新后的引用。
// 并发设计假设：该类本身不提供内置的线程同步机制。它假定所有的读操作都在共享锁（Shared Latch）保护下执行，而所有的修改操作（如 set、copy、compact）都在排他锁（Exclusive Latch）保护下执行。
// ARRAY_BASE_TYPE：底层数组表示的基类或接口类型（代表变异前后的统一返回类型）。
// REP_ENUM_TYPE：标识当前具体表示形态的枚举类型（用于区分不同的内部存储策略）。
// ELEMENT_TYPE：数组中存储的单个元素的数据类型。
public abstract class INArrayRep<ARRAY_BASE_TYPE, REP_ENUM_TYPE,
                                 ELEMENT_TYPE> {

    public INArrayRep() {
    }

    /* Returns the type associated with the representation. */
    // 获取当前数组表示形式对应的类型枚举。
    // 用于外部或调试时识别当前底层采用的是哪种存储策略（如密集型还是稀疏型）。
    public abstract REP_ENUM_TYPE getType();

    /**
     * Sets the array element at idx to the node. The underlying representation
     * can change as a result of the set operation.
     *
     * @param idx the index to be set
     * @param e the array elelement at the idx
     *
     * @return either this, or the new representation if there was a mutation.
     */
    // 将指定索引 idx 处的数组元素设为 e。
    // 由于写入可能触发底层结构的动态变异，该方法会返回一个新的或者变异后的数组表示对象（如果未变异则返回 this）。调用方必须使用返回值更新引用。
    public abstract ARRAY_BASE_TYPE set(int idx, ELEMENT_TYPE e, IN parent);

    /**
     * Returns the element at idx.
     */
    // 获取指定索引 idx 处的元素。
    public abstract ELEMENT_TYPE get(int idx);

    /**
     * Copies n elements at index denoted by "from" to the index denoted by
     * "to". Overlapping copies are supported. It's possible that the
     * representation may mutate as a result of the copy.
     *
     * @param from the source (inclusive) of the copy
     * @param to the target (inclusive) of the copy
     * @param n the number of elements to be copied.
     *
     * @return either this, or the new representation if there was a mutation.
     */
    // 将从源索引 from 开始的 n 个元素复制到目标索引 to 处。
    public abstract ARRAY_BASE_TYPE copy(int from, int to, int n, IN parent);

    /**
     * Chooses a more compact representation, if that's possible, otherwise
     * does nothing.
     * <p>
     * WARNING: This method must not change the memory size of the current
     * representation and return 'this', without explicitly adjusting memory
     * usage (via noteRepChange) before returning.  Returning a new instance is
     * the trigger for adjusting memory usage in the parent.
     *
     * @return this or a more compact representation.
     */
    // 尝试将当前数组精简为一个更紧凑的表示形式。
    // 如果当前有大量空洞或可以通过稀疏压缩节省内存，则会转换为更省空间的形态；否则保持不变。返回变异后或当前的实例。
    public abstract ARRAY_BASE_TYPE compact(IN parent);

    /**
     * Changes the capacity, either truncating existing elements at the end if
     * the capacity is reduced, or adding empty elements at the end if the
     * capacity is enlarged. The caller guarantees that all truncated elements
     * are unused.
     */
    // 调整底层数组的容量大小。
    // 若容量减小，则截断末尾多余的元素；若容量增大，则在末尾填充空元素。调用方必须保证所有被截断的元素都是未被使用的。
    public abstract ARRAY_BASE_TYPE resize(int capacity);

    /**
     * Returns the current memory size of the underlying representation in
     * bytes. It returns the size of the representation, excluding the size of
     * the elements contained in it.
     *
     * @return the memory size of the representation in bytes
     */
    // 计算当前底层表示在内存中实际占用的字节大小。
    public abstract long calculateMemorySize();

    /**
     * Update the cache statistics for this representation.
     *
     * @param increment true the stat should be incremented, false if it must
     * be decremented
     * @param evictor the evictor that shoulds ths stat counters
     */
    // 用于增减驱逐器（Evictor）中的缓存统计计数。
    abstract void updateCacheStats(boolean increment, Evictor evictor);

    /**
     * Updates the cache statistics associated with this representation. It
     * should be invoked upon every creation, every rep change and finally when
     * the IN node is decached.
     *
     * @param increment true if the stat is to be incremented, false if it is
     * to be decremented
     */
    final void updateCacheStats(boolean increment, IN parent) {

        if (!parent.getInListResident()) {
            /* If the IN is not in the cache don't accumulate stats for it. */
            return;
        }

        updateCacheStats(increment, parent.getEnv().getEvictor());
    }

    /**
     * Performs the bookkeeping associated with a representation change. It
     * accounts for the change in storage and adjusts the cache statistics.
     *
     * @param newRep the new representation that is replacing this one.
     */
    // 当底层数组发生表示形态变更（Mutation）时，执行配套的簿记（Bookkeeping）清理与更新。
    final void noteRepChange(
        INArrayRep<ARRAY_BASE_TYPE, REP_ENUM_TYPE, ELEMENT_TYPE> newRep,
        IN parent) {

        if (parent == null) {
            /* Only true in unit tests. */
            return;
        }

        parent.updateMemorySize(newRep.calculateMemorySize() -
                                calculateMemorySize());
        updateCacheStats(false, parent);
        newRep.updateCacheStats(true, parent);
    }
}
