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
package com.sleepycat.je;

/**
 * Modes that can be specified for control over caching of records in the JE
 * in-memory cache.  When a record is stored or retrieved, the cache mode
 * determines how long the record is subsequently retained in the JE in-memory
 * cache, relative to other records in the cache.
 *
 * <p>When the cache overflows, JE must evict some records from the cache.  By
 * default, JE uses a Least Recently Used (LRU) algorithm for determining which
 * records to evict.  With the LRU algorithm, JE makes a best effort to evict
 * the "coldest" (least recently used or accessed) records and to retain the
 * "hottest" records in the cache for as long as possible.</p>
 *
 * <p>When an {@link EnvironmentMutableConfig#setOffHeapCacheSize off-heap
 * cache} is configured, records evicted from the main cache are placed in
 * the off-heap cache, and a separate LRU is used to determine when to evict
 * a record from the off-heap cache.</p>
 *
 * <p>JE uses an approximate LRU approach with some exceptions and special
 * cases.</p>
 * <ul>
 *     <li>
 *         Individual records (LNs or Leaf Nodes) do not appear on the LRU
 *         list, i.e., their "hotness" is not explicitly tracked. Instead,
 *         their containing Btree node (BIN or bottom internal node) appears on
 *         the LRU list. Each BIN contains roughly 100 LNs
 *         (see {@link com.sleepycat.je.EnvironmentConfig#NODE_MAX_ENTRIES}).
 *         When an LN is accessed, its BIN is moved to the hot end of the LRU
 *         list, implying that all other LNs in the same BIN also are treated
 *         as if they are hot. The same applies if the BIN is moved to the cold
 *         end of the LRU list. The above statement applies to the off-heap
 *         cache also, when one is configured.
 *     </li>
 *     <li>
 *         When a BIN contains LNs and the BIN reaches the cold end of the LRU
 *         list, memory can be reclaimed by evicting the LNs, and eviction of
 *         the BIN is deferred. The empty BIN is moved to the hot end of the
 *         LRU list. When an off-heap cache is configured, the eviction of LNs
 *         in this manner occurs independently in both caches.
 *     </li>
 *     <li>
 *         When a BIN contains no LNs, it may be evicted entirely. When the
 *         BINs parent node becomes empty, it may also be evicted, and so on.
 *         The BINs and INs are evicted on the basis of an LRU, but with two
 *         exceptions:
 *         <p>
 *         1) Dirty BINs and INs are evicted only after eviction of all
 *         non-dirty BINs and INs. This is important to reduce logging and
 *         associated cleaning costs. When an off-heap cache is configured,
 *         BINs and INs are evicted from the main cache without regard to
 *         whether they are dirty. Dirty BINs and INs are evicted last, as
 *         just described, only from the off-heap cache.
 *         <p>
 *         2) A BIN may be mutated to a BIN-delta to reclaim memory, rather
 *         then being evicted entirely. A BIN-delta contains only the dirty
 *         entries (for LNs recently logged). A BIN-delta is used when its
 *         size relative to the full BIN will be small enough so that it will
 *         be more efficient, both on disk and in memory, to store the delta
 *         rather than the full BIN.
 *         (see {@link com.sleepycat.je.EnvironmentConfig#TREE_BIN_DELTA}).
 *         The advantage of keeping a BIN-delta in cache is that some
 *         operations, particularly record insertions, can be performed using
 *         the delta without having the complete BIN in cache. When a BIN is
 *         mutated to a BIN-delta to reclaim memory, it is placed at the hot
 *         end of the LRU list. When an off-heap cache is configured, BINs are
 *         not mutated to BIN-deltas in the main cache, but rather this is done
 *         only in the off-heap cache.
 *     </li>
 *     <li>
 *         To reduce contention among threads on the LRU list, multiple LRU
 *         lists may be configured. See
 *         {@link com.sleepycat.je.EnvironmentConfig#EVICTOR_N_LRU_LISTS}.
 *         As described in the javadoc for this parameter, there is a trade-off
 *         between thread contention and the accuracy of the LRU. This
 *         parameter determines the number of main cache LRU lists as well as
 *         the number of off-heap cache LRU lists, when an off-heap cache is
 *         configured.
 *     </li>
 *     <li>
 *          A non-default cache mode may be explicitly specified to override
 *          the LRU behavior. See the CacheMode enumeration values for details.
 *          the normal LRU behavior described above. See the CacheMode
 *          enumeration values for details. The behavior of each CacheMode
 *          when an off-heap cache is configured is also described.
 *     </li>
 * </ul>
 *
 * <p>When no cache mode is explicitly specified, the default cache mode is
 * {@link #DEFAULT}. The default mode causes the normal LRU algorithm to be
 * used.</p>
 *
 * <p>An explicit cache mode may be specified as an {@link
 * EnvironmentConfig#setCacheMode Environment property}, a {@link
 * DatabaseConfig#setCacheMode Database property}, a {@link
 * Cursor#setCacheMode Cursor property}, or on a per-operation basis using
 * {@link ReadOptions#setCacheMode(CacheMode)} or {@link
 * WriteOptions#setCacheMode(CacheMode)}.  If none are specified, {@link
 * #DEFAULT} is used.  If more than one non-null property is specified, the
 * Cursor property overrides the Database and Environment properties, and the
 * Database property overrides the Environment property.</p>
 *
 * <p>When all records in a given Database, or all Databases, should be treated
 * the same with respect to caching, using the Database and/or Environment
 * cache mode properties is sufficient. For applications that need finer
 * grained control, the Cursor cache mode property can be used to provide a
 * specific cache mode for individual records or operations.  The Cursor cache
 * mode property can be changed at any time, and the cache mode specified will
 * apply to subsequent operations performed with that Cursor.</p>
 *
 * <p>In a Replicated Environment where a non-default cache mode is desired,
 * the cache mode can be configured on the Master node as described above.
 * However, it is important to configure the cache mode on the Replica nodes
 * using an Environment property.  That way, the cache mode will apply to
 * <em>write</em> operations that are replayed on the Replica for all
 * Databases, even if the Databases are not open by the application on the
 * Replica.  Since all nodes may be Replicas at some point in their life cycle,
 * it is recommended to configure the desired cache mode as an Environment
 * property on all nodes in a Replicated Environment.</p>
 *
 * <p>On a Replica, per-Database control over the cache mode for <em>write</em>
 * operations is possible by opening the Database on the Replica and
 * configuring the cache mode.  Per-Cursor control (meaning per-record or
 * per-operation) control of the cache mode is not possible on a Replica for
 * <em>write</em> operations.  For <em>read</em> operations, both per-Database
 * and per-Cursor control is possible on the Replica, as described above.</p>
 * <p> 
 * The cache related stats in {@link EnvironmentStats} can provide some measure
 * of the effectiveness of the cache mode choice.
 *
 * @see <a href="EnvironmentStats.html#cacheSizing">Cache Statistics:
 * Sizing</a>
 */
// 精细控制 Berkeley DB JE 内存缓存（In-Memory Cache）中记录的缓存行为
// 在执行数据的存储（Store）或检索（Retrieve）操作时，CacheMode 决定了该记录相对于其他记录在内存中保留的时间长短（即“热度”）。JE 默认使用近似的 LRU（最近最少使用）算法来管理内存缓存溢出时的页面驱逐。通过 CacheMode，开发者可以显式干预这一默认行为：
// JE 缓存机制核心概念说明
// LN (Leaf Node，叶子节点)：存储实际用户记录（键值对数据）的节点。
// BIN (Bottom Internal Node，底层内部节点)：B树的底层内部节点，每个 BIN 大约包含 100 个 LN。JE 在 LRU 链表上追踪的是 BIN 而不是单独的 LN。访问某个 LN 会使其所在的 BIN 变为“热（Hot）”。
// Dirty（脏数据）：已被修改但尚未刷写到磁盘日志文件中的内存节点。
// Off-Heap Cache（堆外缓存）：如果配置了堆外内存缓存，从主内存驱逐的记录可以放入堆外缓存中。
public enum CacheMode {

    /**
     * The record's hotness is changed to "most recently used" by the
     * operation.
     *
     * <p>This cache mode is used when the application does not need explicit
     * control over the cache and a standard LRU approach is sufficient.</p>
     *
     * <p>Note that {@code null} may be specified to use the {@code DEFAULT}
     * mode.</p>
     *
     * <p>Specifically:
     * <ul>
     *     <li>The BIN containing the record's LN will remain in the main
     *     cache, and it is moved to the hot end of its LRU list.</li>
     *
     *     <li>When an off-heap cache is configured, the record's LN and BIN
     *     will be loaded into the main cache only. They will be removed from
     *     the off-heap cache, if they were present there. However, if other
     *     LNs belonging to this BIN were present in the off-heap cache, they
     *     will remain there.</li>
     * <ul>
     */
    // 默认的缓存模式。
    // 操作会将记录的“热度”更新为“最近最使用（Most Recently Used）”。
    // 包含该记录 LN 的 BIN 将保留在主缓存中，并被移动到 LRU 链表的热端（Hot End）。
    DEFAULT,

    /**
     * @deprecated please use {@link #DEFAULT} instead. As of JE 4.0, this mode
     * functions exactly as if {@link #DEFAULT} were specified.
     */
    // 保持热点状态。
    // 自 JE 4.0 起已过时，请改用 DEFAULT。
    KEEP_HOT,

    /**
     * The record's hotness or coldness is unchanged by the operation where
     * this cache mode is specified.
     *
     * <p>This cache mode is normally used when the application prefers that
     * the operation should not perturb the cache, for example, when scanning
     * over all records in a database.</p>
     *
     * <p>Specifically:
     * <ul>
     *     <li>A record's LN and BIN must be loaded into the main cache in
     *     order to perform the operation. However, they may be removed from
     *     the main cache after the operation, to avoid a net change to the
     *     cache, according to the rules below.</li>
     *
     *     <li>If the record's LN was not present in the main cache prior to
     *     the operation, then the LN will be evicted from the main cache
     *     after the operation. The LN will not be added to, or removed from,
     *     the off-heap cache.</li>
     *
     *     <li>When the LN is to be evicted from the main cache (according to
     *     the above rules) and the operation is not performed via a cursor,
     *     the LN is evicted when the operation is complete. When a cursor is
     *     used, the LN is evicted when the cursor is moved to a different
     *     record or closed.</li>
     *
     *     <li>If the record's BIN was not present in the main cache prior to
     *     the operation, the action taken depends on whether the BIN is dirty
     *     and whether an off-heap cache is configured.
     *     <ul>
     *         <li>When the BIN is not dirty, the BIN (and LN) will be evicted
     *         from the main cache after the operation. The BIN (and LN) will
     *         not be added to, or removed from, the off-heap cache.</li>
     *
     *         <li>When the BIN is dirty and an off-heap cache is
     *         <em>not</em> configured, the BIN will not be evicted from the
     *         main cache and will be moved to the hot end of its main cache
     *         LRU list. This is done to reduce logging.</li>
     *
     *         <li>When the BIN is dirty and an off-heap cache <em>is</em>
     *         configured, we evict the BIN from the main cache even when it
     *         is dirty because the BIN (and LN) will be stored in the off-heap
     *         cache and the BIN will not be logged. The BIN will be placed at
     *         the hot end of its off-heap LRU list.</li>
     *
     *         <li>Note that when this operation loaded the BIN and the BIN
     *         becomes dirty, it is normally because this operation is a write
     *         operation. However, other concurrent threads can also dirty the
     *         BIN.</li>
     *     </ul>
     *
     *     <li>When the BIN is to be evicted from the main cache (according
     *     to the above rules) and the operation is not performed via a
     *     cursor, the BIN is evicted when the operation is complete. When a
     *     cursor is used, the BIN is evicted only when the cursor moves to a
     *     different BIN or is closed. Because of the way BINs are evicted,
     *     when multiple operations are performed using a single cursor and
     *     not perturbing the cache is desired, it is important to use this
     *     cache mode for all of the operations.</li>
     *
     *     <li>When the BIN was present in the main cache prior to the
     *     operation, its position in the LRU list will not be changed. Its
     *     position in the off-heap LRU list, if it is present in the off-heap
     *     cache, will also not be changed.</li>
     * </ul>
     */
    // 保持记录的缓存热度或冷度不变，不扰乱缓存。
    // 典型场景：当你需要遍历（Scan）数据库中的所有记录，又希望这次扫描不会把原本在缓存中的热点数据挤出去时使用。
    UNCHANGED,

    /**
     * @deprecated please use {@link #UNCHANGED} instead. As of JE 4.0, this
     * mode functions exactly as if {@link #UNCHANGED} were specified.
     */
    // 使记录变冷。
    // 在 JE 4.0 及之后的版本中，它的行为与 UNCHANGED 完全相同。
    MAKE_COLD,

    /**
     * The record's LN is evicted after the operation, and the containing
     * BIN is moved to the hot end of the LRU list.
     *
     * <p>This cache mode is normally used when not all LNs will fit into the
     * main cache, and the application prefers to read the LN from the log file
     * or load it from the off-heap cache when the record is accessed again,
     * rather than have it take up space in the main cache and potentially
     * cause expensive Java GC. By using this mode, the file system cache or
     * off-heap cache can be relied on for holding LNs, which complements the
     * use of the JE cache to hold BINs and INs.</p>
     *
     * <p>Note that using this mode for all operations will prevent the cache
     * from filling, if all internal nodes fit in cache.</p>
     *
     * <p>Specifically:
     * <ul>
     *     <li>The record's LN will be evicted from the main cache after the
     *     operation. The LN will be added to the off-heap cache, if it is not
     *     already present and an off-heap cache is configured.</li>
     *
     *     <li>When the operation is not performed via a cursor, the LN is
     *     evicted when the operation is complete. When a cursor is used, the
     *     LN is evicted when the cursor is moved to a different record or
     *     closed.</li>
     * </ul>
     *
     * @since 3.3.98
     */
    // 操作完成后立即驱逐记录的叶子节点（LN），同时将包含该 LN 的 BIN 移到 LRU 热端。
    // 典型场景：当 LN 总量超出主缓存容量，应用程序宁愿在再次访问时从磁盘日志或堆外缓存中重新读取 LN，也不愿让 LN 占用宝贵的 JVM 堆内存（从而避免引发昂贵的 Java GC）。这允许文件系统缓存或堆外缓存去托管 LN，而主缓存专注于存放树形索引节点（BIN 和 IN）。
    EVICT_LN,

    /**
     * The record's BIN (and its LNs) are evicted after the operation.
     *
     * <p>This cache mode is normally used when not all BINs will fit into the
     * main cache, and the application prefers to read the LN and BIN from the
     * log file or load it from the off-heap cache when the record is accessed
     * again, rather than have them take up space in the JE cache and
     * potentially cause expensive Java GC.</p>
     *
     * <p>Because this mode evicts all LNs in the BIN, even if they are "hot"
     * from the perspective of a different accessor, this mode should be used
     * with caution. One valid use case is where all accessors use this mode;
     * in this case the cache mode might be set on a per-Database or
     * per-Environment basis.</p>
     *
     * <p>Note that using this mode for all operations will prevent the cache
     * from filling, if all upper internal nodes fit in cache.</p>
     *
     * <p>Specifically:
     * <ul>
     *     <li>The record's LN will be evicted from the main cache after the
     *     operation. The LN will be added to the off-heap cache, if it is not
     *     already present and an off-heap cache is configured.</li>
     *
     *     <li>When the operation is not performed via a cursor, the LN is
     *     evicted when the operation is complete. When a cursor is used, the
     *     LN is evicted when the cursor is moved to a different record or
     *     closed.</li>
     *
     *     <li>Whether the BIN is evicted depends on whether the BIN is dirty
     *     and whether an off-heap cache is configured.
     *     <ul>
     *         <li>When the BIN is not dirty, the BIN (and LN) will be evicted
     *         from the main cache after the operation. The BIN (and LN) will
     *         be added to the off-heap cache, if they are not already present
     *         and an off-heap cache is configured. The BIN will be placed at
     *         the hot end of its off-heap LRU list.</li>
     *
     *         <li>When the BIN is dirty and an off-heap cache is
     *         <em>not</em> configured, the BIN will not be evicted from the
     *         main cache and will be moved to the hot end of its main cache
     *         LRU list. This is done to reduce logging.</li>
     *
     *         <li>When the BIN is dirty and an off-heap cache <em>is</em>
     *         configured, we evict the BIN from the main cache even when it
     *         is dirty because the BIN (and LN) will be stored in the off-heap
     *         cache and the BIN will not be logged. The BIN will be placed at
     *         the hot end of its off-heap LRU list.</li>
     *
     *         <li>Note that BIN may have been dirtied by this operation, if
     *         it is a write operation, or by earlier write operations.</li>
     *     </ul>
     *
     *     <li>When the BIN is to be evicted from the main cache (according
     *     to the above rules) and the operation is not performed via a
     *     cursor, the BIN is evicted when the operation is complete. When a
     *     cursor is used, the BIN is evicted only when the cursor moves to a
     *     different BIN or is closed. Because of the way BINs are evicted,
     *     when multiple operations are performed using a single cursor and
     *     not perturbing the cache is desired, it is important to use this
     *     cache mode for all of the operations.</li>
     * </ul>
     *
     * @since 4.0.97
     */
    // 操作完成后，将记录的 BIN（以及属于该 BIN 的所有 LN）全部驱逐出主缓存。
    // 典型场景：当整个数据库的 BIN 无法全部容纳于主缓存中时使用。由于它会驱逐整个 BIN 内的所有 LN（即使某些 LN 对其他访问者来说是“热”的），因此需要谨慎使用。最常见的使用方式是全局所有访问者都采用此模式（配置在 Database 或 Environment 级别）。
    EVICT_BIN,

    /**
     * @hidden
     * For internal use only.
     * Placeholder to avoid DPL class evolution errors. Never actually used.
     * @since 4.0.97
     */
    // 动态占位符（内部保留）。
    DYNAMIC
}
