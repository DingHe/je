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

import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_RELATCHES_REQUIRED;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_ROOT_SPLITS;
import static com.sleepycat.je.dbi.BTreeStatDefinition.GROUP_DESC;
import static com.sleepycat.je.dbi.BTreeStatDefinition.GROUP_NAME;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.latch.LatchContext;
import com.sleepycat.je.latch.LatchFactory;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.latch.LatchTable;
import com.sleepycat.je.latch.SharedLatch;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * Tree implements the JE B+Tree.
 *
 * A note on tree search patterns:
 * There's a set of Tree.search* methods. Some clients of the tree use
 * those search methods directly, whereas other clients of the tree
 * tend to use methods built on top of search.
 *
 * The semantics of search* are
 *   they leave you pointing at a BIN or IN
 *   they don't tell you where the reference of interest is.
 * The semantics of the get* methods are:
 *   they leave you pointing at a BIN or IN
 *   they return the index of the slot of interest
 *   they traverse down to whatever level is needed
 *   they are built on top of search* methods.
 * For the future:
 * Over time, we need to clarify which methods are to be used by clients
 * of the tree. Preferably clients that call the tree use get*, although
 * their are cases where they need visibility into the tree structure.
 *
 * Also, search* should return the location of the slot to save us a
 * second binary search.
 *
 * Search Method Call Hierarchy
 * ----------------------------
 * getFirst/LastNode
 *  search
 *  CALLED BY:
 *   CursorImpl.getFirstOrLast
 *
 * getNext/PrevBin
 *  getParentINForChildIN
 *  searchSubTree
 *  CALLED BY:
 *   DupConvert
 *   CursorImpl.getNext
 *
 * getParentINForChildIN
 *  IN.findParent
 *  does not use shared latching
 *  CALLED BY:
 *   Checkpointer.flushIN (doFetch=false, targetLevel=-1)
 *   FileProcessor.processIN (doFetch=true, targetLevel=LEVEL)
 *   Evictor.evictIN (doFetch=true, targetLevel=-1)
 *   RecoveryManager.replaceOrInsertChild (doFetch=true, targetLevel=-1)
 *   getNext/PrevBin (doFetch=true, targetLevel=-1)
 *
 * search
 *  searchSubTree
 *  CALLED BY:
 *   CursorImpl.searchAndPosition
 *   INCompressor to find BIN
 *
 * searchSubTree
 *  uses shared grandparent latching
 *
 * getParentBINForChildLN
 *  searchSplitsAllowed
 *   CALLED BY:
 *    RecoveryManager.redo
 *    RecoveryManager.recoveryUndo
 *  search
 *   CALLED BY:
 *    RecoveryManager.abortUndo
 *    RecoveryManager.rollbackUndo
 *    FileProcessor.processLN
 *    Cleaner.processPendingLN
 *    UtilizationProfile.verifyLsnIsObsolete (utility)
 *
 * findBinForInsert
 *  searchSplitsAllowed
 *  CALLED BY:
 *   CursorImpl.putInternal
 *
 * searchSplitsAllowed
 *  uses shared non-grandparent latching
 *  CALLED BY:
 *   DupConvert (instead of findBinForInsert, which needs a cursor)
 *
 * Possible Shared Latching Improvements
 * -------------------------------------
 * By implementing shared latching for BINs we would get better concurrency in
 * these cases:
 *  Reads when LN is in cache, or LN is not needed (key-only op, e.g., dups)
 */
// Tree 类是整个 B+树（B+Tree）实现的顶层核心类。
// 管理着整个索引树的根节点（Root）、最大树节点条目数、并发锁/闩机制（Latch）、统计信息，并提供了遍历、查找、插入、删除等核心树操作的入口。
// 主要承担以下职责：
// B+树的根与生命周期管理：持有着指向根节点（Root）的引用，并负责根节点的初始化、替换、分裂（Root Split）以及持久化日志关联。
// 并发控制与并发闩（Latch）保护：通过专属的 rootLatch（根节点闩）来保护根节点在并发环境下的安全，防止在树结构调整（如根分裂）时发生竞态条件。
// 树的漫游与导航（Navigation & Search）：提供了从根向下搜索、获取最左/最右节点、以及在同级 IN（Internal Node）之间进行前进或后退遍历的方法。
// 子树删除与空间回收：实现了整条不必要分支的剪枝与删除逻辑（delete / searchDeletableSubTree），在叶子节点变空时向上清理无用的内部节点。
public final class Tree implements Loggable {

    /* For debug tracing */
    // 用于记录根节点发生分裂（Root Split）时的追踪日志前缀。
    private static final String TRACE_ROOT_SPLIT = "RootSplit:";
    // 关联当前的数据库实例（DatabaseImpl），用于获取环境配置、最大树条目数以及将修改传播到数据库状态中。
    private DatabaseImpl database;
    // 每个树节点（IN/BIN）允许容纳的最大条目数（Max Entries Per Node），通常由数据库配置决定
    private int maxTreeEntriesPerNode;
    // 指向 B+树根节点的子引用（ChildReference）。它封装了根节点的目标对象（Node）、定位键（Key）和日志LSN。
    private ChildReference root;

    /*
     * Latch that must be held when using/accessing the root node.  Protects
     * against the root being changed out from underneath us by splitRoot.
     * After the root IN is latched, the rootLatch can be released.
     */
    // 专用于保护树根节点的共享/排他闩（Shared Latch）。防止在多个线程并发访问或修改树时，根节点被 splitRoot 等操作意外替换。
    private SharedLatch rootLatch;

    /*
     * We don't need the stack trace on this so always throw a static and
     * avoid the cost of Throwable.fillInStack() every time it's thrown.
     * [#13354].
     */
    // 为了避免每次树节点分裂需要扩容时重复创建 Throwable 堆栈轨迹所带来的性能开销，这里直接重用静态异常。
    private static SplitRequiredException splitRequiredException =
        new SplitRequiredException();

    /* Stats */
    // B+树统计信息的分组容器，用于收集和汇报树相关的运行时指标。
    private StatGroup stats;

    /* The number of tree root splited. */
    // 统计树根节点发生分裂（Split）的总次数。
    private IntStat rootSplits;
    /* The number of latch upgrades from shared to exclusive required. */
    // 统计树操作中，闩（Latch）从共享模式（Shared）升级为排他模式（Exclusive）的所需次数。
    private LongStat relatchesRequired;
    // 线程本地变量，用于在树遍历过程中累加统计信息，避免多线程冲突。
    private final ThreadLocal<TreeWalkerStatsAccumulator> treeStatsAccumulatorTL =
        new ThreadLocal<TreeWalkerStatsAccumulator>();

    /* For unit tests */
    // 分别用于模拟竞争条件、测试搜索、检查点、获取父IN以及加载IN时的行为。
    private TestHook waitHook; // used for generating race conditions
    private TestHook searchHook; // [#12736]
    private TestHook ckptHook; // [#13897]
    private TestHook getParentINHook;
    private TestHook fetchINHook;

    /**
     * Embodies an enum for the type of search being performed.  NORMAL means
     * do a regular search down the tree.  LEFT/RIGHT means search down the
     * left/right side to find the first/last node in the tree.
     */
    // 定义了树搜索的类型枚举/常量。
    public static class SearchType {
        /* Search types */
        // 普通向下搜索。
        public static final SearchType NORMAL = new SearchType();
        // 沿左侧搜索以查找树中第一个（最小的）节点。
        public static final SearchType LEFT   = new SearchType();
        // 沿右侧搜索以查找树中最后一个（最大的）节点。
        public static final SearchType RIGHT  = new SearchType();

        /* No lock types can be defined outside this class. */
        private SearchType() {
        }
    }

    /*
     * Class that overrides ChildReference methods to enforce rules that apply
     * to the root.
     *
     * Overrides fetchTarget() so that if the rootLatch is not held exclusively
     * when the root is fetched, we upgrade it to exclusive. Also overrides
     * setter methods to assert that an exclusive latch is held.
     *
     * Overrides setDirty to dirty the DatabaseImpl, so that the MapLN will be
     * logged during the next checkpoint. This is critical when updating the
     * root LSN.
     */
    // 专用于根节点的子引用类，用于强制实施针对根节点的特殊规则（如锁升级、脏数据标记）。
    private class RootChildReference extends ChildReference {

        private RootChildReference() {
            super();
        }

        private RootChildReference(Node target, byte[] key, long lsn) {
            super(target, key, lsn);
        }

        /* Caller is responsible for releasing rootLatch. */
        @Override
        public Node fetchTarget(DatabaseImpl database, IN in)
            throws DatabaseException {

            if (getTarget() == null && !rootLatch.isExclusiveOwner()) {

                rootLatch.release();
                rootLatch.acquireExclusive();

                /*
                 * If the root field changed while unlatched then we have an
                 * invalid state and cannot continue. [#21686]
                 */
                if (this != root) {
                    throw EnvironmentFailureException.unexpectedState(
                        database.getEnv(),
                        "Root changed while unlatched, dbId=" +
                        database.getId());
                }
            }

            return super.fetchTarget(database, in);
        }

        @Override
        public void setTarget(Node target) {
            assert rootLatch.isExclusiveOwner();
            super.setTarget(target);
        }

        @Override
        public void clearTarget() {
            assert rootLatch.isExclusiveOwner();
            super.clearTarget();
        }

        @Override
        public void setLsn(long lsn) {
            assert rootLatch.isExclusiveOwner();
            super.setLsn(lsn);
        }

        @Override
        void updateLsnAfterOptionalLog(DatabaseImpl dbImpl, long lsn) {
            assert rootLatch.isExclusiveOwner();
            super.updateLsnAfterOptionalLog(dbImpl, lsn);
        }

        @Override
        void setDirty() {
            super.setDirty();
            database.setDirty();
        }
    }

    /**
     * Create a new tree.
     */
    public Tree(DatabaseImpl database) {
        init(database);
        setDatabase(database);
    }

    /**
     * Create a tree that's being read in from the log.
     */
    public Tree() {
        init(null);
        maxTreeEntriesPerNode = 0;
    }

    /**
     * constructor helper
     */
    private void init(DatabaseImpl database) {
        this.root = null;
        this.database = database;

        /* Do the stats definitions. */
        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        relatchesRequired = new LongStat(stats, BTREE_RELATCHES_REQUIRED);
        rootSplits = new IntStat(stats, BTREE_ROOT_SPLITS);
    }

    /**
     * Set the database for this tree. Used by recovery when recreating an
     * existing tree.
     */
    public void setDatabase(DatabaseImpl database) {
        this.database = database;

        final EnvironmentImpl envImpl = database.getEnv();

        /*
         * The LatchContext for the root is special in that it is considered a
         * Btree latch (the Btree latch table is used), but the context is not
         * implemented by the IN class.
         */
        final LatchContext latchContext = new LatchContext() {
            @Override
            public int getLatchTimeoutMs() {
                return envImpl.getLatchTimeoutMs();
            }
            @Override
            public String getLatchName() {
                return "RootLatch";
            }
            @Override
            public LatchTable getLatchTable() {
                return LatchSupport.btreeLatchTable;
            }
            @Override
            public EnvironmentImpl getEnvImplForFatalException() {
                return envImpl;
            }
        };

        rootLatch = LatchFactory.createSharedLatch(
            latchContext, false /*exclusiveOnly*/);


        maxTreeEntriesPerNode = database.getNodeMaxTreeEntries();
    }

    /**
     * @return the database for this Tree.
     */
    public DatabaseImpl getDatabase() {
        return database;
    }

    /**
     * Called when latching a child and the parent is latched. Used to
     * opportunistically validate the parent pointer.
     */
    private static void latchChild(final IN parent,
                                   final IN child,
                                   final CacheMode cacheMode) {
        child.latch(cacheMode);

        if (child.getParent() != parent) {
            throw EnvironmentFailureException.unexpectedState();
        }
    }

    /**
     * Called when latching a child and the parent is latched. Used to
     * opportunistically validate the parent pointer.
     */
    private static void latchChildShared(final IN parent,
                                         final IN child,
                                         final CacheMode cacheMode) {
        child.latchShared(cacheMode);

        if (child.getParent() != parent) {
            throw EnvironmentFailureException.unexpectedState();
        }
    }

    public void latchRootLatchExclusive()
        throws DatabaseException {

        rootLatch.acquireExclusive();
    }

    public void releaseRootLatch()
        throws DatabaseException {

        rootLatch.release();
    }

    /**
     * Set the root for the tree. Should only be called within the root latch.
     */
    // 替换整棵树的根节点引用（例如在根节点分裂 splitRoot 或初始化时）。必须在持有根排他锁的前提下调用，确保根切换的原子性。
    public void setRoot(ChildReference newRoot, boolean notLatched) {
        assert (notLatched || rootLatch.isExclusiveOwner());
        root = newRoot;
    }

    public ChildReference makeRootChildReference(
        Node target,
        byte[] key,
        long lsn) {

        return new RootChildReference(target, key, lsn);
    }

    private RootChildReference makeRootChildReference() {
        return new RootChildReference();
    }

    /*
     * A tree doesn't have a root if (a) the root field is null, or (b) the
     * root is non-null, but has neither a valid target nor a valid LSN. Case
     * (b) can happen if the database is or was previously opened in deferred
     * write mode.
     *
     * @return false if there is no real root.
     */
    // 通过安全校验树的根引用对象（root）及其内存目标与磁盘 LSN 状态，判断当前树是否拥有一个真实、有效的根节点。
    public boolean rootExists() {
        if (root == null) {
            return false;
        }
        // 说明根节点当前并没有驻留在内存中（可能被驱逐了，或者尚未加载）。
        if ((root.getTarget() == null) &&
            // 说明该根节点也没有关联任何有效的磁盘日志坐标（即从未落盘过）。
            (root.getLsn() == DbLsn.NULL_LSN)) {
            return false;
        }

        return true;
    }

    /**
     * Perform a fast check to see if the root IN is resident.  No latching is
     * performed.  To ensure that the root IN is not loaded by another thread,
     * this method should be called while holding a write lock on the MapLN.
     * That will prevent opening the DB in another thread, and potentially
     * loading the root IN. [#13415]
     */
    public boolean isRootResident() {
        return root != null && root.getTarget() != null;
    }

    /**
     * Helper to obtain the root IN with shared root latching.  Optionally
     * updates the generation of the root when latching it.
     */
    // 根节点访问与生命周期管理核心方法
    public IN getRootIN(CacheMode cacheMode)
        throws DatabaseException {

        return getRootINInternal(cacheMode, false/*exclusive*/);
    }

    /**
     * Helper to obtain the root IN with exclusive root latching.  Optionally
     * updates the generation of the root when latching it.
     */
    public IN getRootINLatchedExclusive(CacheMode cacheMode)
        throws DatabaseException {

        return getRootINInternal(cacheMode, true/*exclusive*/);
    }
    // 获取 B+树根内部节点（Root IN）的内部通用方法
    // 在持有树根机制共享锁（rootLatch）的安全保护下，安全地获取并返回根节点 IN 对象，同时支持按需对根 IN 节点施加共享锁或排他锁。
    // boolean exclusive 根 IN 节点的加锁模式标志。如果为 true：表示获取到根 IN 对象后，需要对其施加排他锁（Exclusive Latch / 写锁）。
    private IN getRootINInternal(CacheMode cacheMode, boolean exclusive)
        throws DatabaseException {

        rootLatch.acquireShared();
        try {
            return getRootINRootAlreadyLatched(cacheMode, exclusive);
        } finally {
            rootLatch.release();
        }
    }

    /**
     * Helper to obtain the root IN, when the root latch is already held.
     */
    // 在假定顶层的树根保护机制（rootLatch）已经处于被持有（Held）的前提下，安全地从缓存或磁盘中加载根内部节点（Root IN），
    // 并根据调用方的需求为其施加排他锁（Exclusive Latch）或共享锁（Shared Latch），最后将其返回。
    public IN getRootINRootAlreadyLatched(
        CacheMode cacheMode,
        boolean exclusive) {
        // 快速检查整棵 B+树当前是否存在一个真实的、有效的根节点。
        if (!rootExists()) {
            return null;
        }
        // 如果根 IN 当前已经驻留在内存中，它会直接被返回。
        // 如果根 IN 曾被内存驱逐器（Evictor）驱逐出内存，该方法会触发底层机制（通过 LSN）将其从磁盘日志中重新加载（Fault in）到内存中。
        final IN rootIN = (IN) root.fetchTarget(database, null);

        if (exclusive) {
            rootIN.latch(cacheMode);
        } else {
            rootIN.latchShared(cacheMode);
        }
        return rootIN;
    }

    public IN getResidentRootIN(boolean latched)
        throws DatabaseException {

        IN rootIN = null;
        if (rootExists()) {
            rootIN = (IN) root.getTarget();
            if (rootIN != null && latched) {
                rootIN.latchShared(CacheMode.UNCHANGED);
            }
        }
        return rootIN;
    }

    public IN withRootLatchedExclusive(WithRootLatched wrl)
        throws DatabaseException {

        try {
            rootLatch.acquireExclusive();
            return wrl.doWork(root);
        } finally {
            rootLatch.release();
        }
    }

    public IN withRootLatchedShared(WithRootLatched wrl)
        throws DatabaseException {

        try {
            rootLatch.acquireShared();
            return wrl.doWork(root);
        } finally {
            rootLatch.release();
        }
    }

    /**
     * Get LSN of the rootIN. Obtained without latching, should only be
     * accessed while quiescent.
     */
    public long getRootLsn() {
        if (root == null) {
            return DbLsn.NULL_LSN;
        } else {
            return root.getLsn();
        }
    }

    /**
     * Cheaply calculates and returns the maximum possible number of LNs in the
     * btree.
     */
    public long getMaxLNs() {

        final int levels;
        final int topLevelSlots;
        rootLatch.acquireShared();
        try {
            IN rootIN = (IN) root.fetchTarget(database, null);
            levels = rootIN.getLevel() & IN.LEVEL_MASK;
            topLevelSlots = rootIN.getNEntries();
        } finally {
            rootLatch.release();
        }
        return (long) (topLevelSlots *
                       Math.pow(database.getNodeMaxTreeEntries(), levels - 1));
    }

    /**
     * Deletes a BIN specified by key from the tree. If the BIN resides in a
     * subtree that can be pruned away, prune as much as possible, so we
     * don't leave a branch that has no BINs.
     *
     * It's possible that the targeted BIN will now have entries, or will
     * have resident cursors. Either will prevent deletion (see exceptions).
     *
     * Unlike splits, IN deletion does not immediately log the subtree parent
     * or its ancestors. It is sufficient to simply dirty the subtree parent.
     * Logging is not necessary for correctness, and if a checkpoint does not
     * flush the subtree parent then recovery will add the BINs to the
     * compressor queue when redoing the LN deletions.
     *
     * @param idKey - the identifier key of the node to delete.
     *
     * @throws NodeNotEmptyException if the BIN is not empty. The deletion is
     * no longer possible.
     *
     * @throws CursorsExistException is the BIN has cursors. The deletion
     * should be retried later by the INCompressor.
     */
    public void delete(byte[] idKey)
        throws NodeNotEmptyException, CursorsExistException {

        final EnvironmentImpl envImpl = database.getEnv();
        final Logger logger = envImpl.getLogger();

        final List<SplitInfo> nodeLadder = searchDeletableSubTree(idKey);

        if (nodeLadder == null) {
            /*
             * The tree is empty, so do nothing.  Root compression is no
             * longer supported.  Root compression has no impact on memory
             * usage now that we evict the root IN.  It reduces log space
             * taken by INs for empty (but not removed) databases, yet
             * requires logging a INDelete and MapLN; this provides very
             * little benefit, if any.  Because it requires extensive
             * testing (which has not been done), this minor benefit is not
             * worth the cost.  And by removing it we no longer log
             * INDelete, which reduces complexity going forward. [#17546]
             */
            return;
        }

        /* Detach this subtree. */
        final SplitInfo detachPoint = nodeLadder.get(0);
        try {
            final IN branchParent = detachPoint.parent;
            final IN branchRoot = detachPoint.child;

            if (logger.isLoggable(Level.FINEST)) {
                LoggerUtils.envLogMsg(
                    Level.FINEST, envImpl,
                    "Tree.delete() " +
                    Thread.currentThread().getId() + "-" +
                    Thread.currentThread().getName() + "-" +
                    envImpl.getName() +
                    " Deleting child node: " + branchRoot.getNodeId() +
                    " from parent node: " + branchParent.getNodeId() +
                    " parent has " + branchParent.getNEntries() +
                    " children");
            }

            branchParent.deleteEntry(detachPoint.index);

            /*
             * Remove deleted INs from the INList/cache and count them as
             * provisionally obsolete. The parent is not logged immediately, so
             * we can't count them immediately obsolete. They will be counted
             * obsolete when an ancestor is logged non-provisionally. [#21348]
             */
            final INList inList = database.getEnv().getInMemoryINs();

            for (final SplitInfo info : nodeLadder) {

                final IN child = info.child;

                assert !child.isBINDelta(false);
                assert !(child.isUpperIN() && child.getNEntries() > 1);
                assert !(child.isBIN() && child.getNEntries() > 0);

                /*
                 * Remove child from cache. The branch root was removed by
                 * deleteEntry above.
                 */
                if (child != branchRoot) {
                    inList.remove(child);
                }

                /* Count full and delta versions as obsolete. */
                branchParent.trackProvisionalObsolete(
                    child, child.getLastFullLsn());

                branchParent.trackProvisionalObsolete(
                    child, child.getLastDeltaLsn());
            }

            if (logger.isLoggable(Level.FINE)) {
                LoggerUtils.envLogMsg(
                    Level.FINE, envImpl,
                    "SubtreeRemoval: subtreeRoot = " + branchRoot.getNodeId());
            }

        } finally {
            releaseNodeLadderLatches(nodeLadder);
        }
    }

    /**
     * Search down the tree using a key, but instead of returning the BIN that
     * houses that key, find the point where we can detach a deletable
     * subtree. A deletable subtree is a branch where each IN has one child,
     * and the bottom BIN has no entries and no resident cursors. That point
     * can be found by saving a pointer to the lowest node in the path with
     * more than one entry.
     *
     *              INa
     *             /   \
     *          INb    INc
     *          |       |
     *         INd     ..
     *         / \
     *      INe  ..
     *       |
     *     BINx (suspected of being empty)
     *
     * In this case, we'd like to prune off the subtree headed by INe. INd
     * is the parent of this deletable subtree.
     *
     * The method returns a list of parent/child/index structures. In this
     * example, the list will hold:
     *  INd/INe/index
     *  INe/BINx/index
     * All three nodes will be EX-latched.
     *
     * @return null if the entire Btree is empty, or a list of SplitInfo for
     * the branch to be deleted. If non-null is returned, the INs in the list
     * will be EX-latched; otherwise, no INs will be latched.
     *
     * @throws NodeNotEmptyException if the BIN is not empty.
     *
     * @throws CursorsExistException is the BIN has cursors.
     */
    private List<SplitInfo> searchDeletableSubTree(byte[] key)
        throws NodeNotEmptyException, CursorsExistException {

        assert (key!= null);

        IN parent = getRootINLatchedExclusive(CacheMode.UNCHANGED);

        if (parent == null) {
            /* Tree was never persisted. */
            return null;
        }

        final ArrayList<SplitInfo> nodeLadder = new ArrayList<>();

        try {
            IN child;
            IN pinedIN = null;

            do {
                if (parent.getNEntries() == 0) {
                    throw EnvironmentFailureException.unexpectedState(
                        "Found upper IN with 0 entries");
                }

                if (parent.getNEntries() > 1) {
                    /*
                     * A node with more than one entry is the lowest potential
                     * branch parent. Unlatch/discard ancestors of this parent.
                     */
                    for (final SplitInfo info : nodeLadder) {
                        info.parent.releaseLatch();
                    }
                    nodeLadder.clear();
                    pinedIN = null;
                } else if (parent.isPinned()) {
                    pinedIN = parent;
                }

                final int index = parent.findEntry(key, false, false);
                assert index >= 0;

                /* Get the child node that matches. */
                child = parent.fetchIN(index, CacheMode.UNCHANGED);

                latchChild(parent, child, CacheMode.UNCHANGED);

                nodeLadder.add(new SplitInfo(parent, child, index));

                /* Continue down a level */
                parent = child;
            } while (!parent.isBIN());

            if (pinedIN != null) {
                throw CursorsExistException.CURSORS_EXIST;
            }

            /*
             * See if there is a reason we can't delete this BIN -- i.e.
             * new items have been inserted, or a cursor exists on it.
             */
            assert (child.isBIN());
            final BIN bin = (BIN) child;

            if (bin.getNEntries() != 0) {
                throw NodeNotEmptyException.NODE_NOT_EMPTY;
            }

            if (bin.isBINDelta()) {
                throw EnvironmentFailureException.unexpectedState(
                    "Found BIN delta with 0 entries");
            }

            /*
             * This case can happen if we are keeping a cursor on an empty
             * BIN as we traverse.
             */
            if (bin.nCursors() > 0 || child.isPinned()) {
                throw CursorsExistException.CURSORS_EXIST;
            }

            if (nodeLadder.get(0).parent.getNEntries() <= 1) {
                /* The entire tree is empty. */
                releaseNodeLadderLatches(nodeLadder);
                return null;
            }

            return nodeLadder;

        } catch (Throwable e) {
            releaseNodeLadderLatches(nodeLadder);
            /* Release parent in case it was not added to nodeLadder. */
            parent.releaseLatchIfOwner();
            throw e;
        }
    }

    /**
     * Release latched acquired by searchDeletableSubTree. Each child is
     * latched, plus the parent of the first node (the branch parent).
     */
    private void releaseNodeLadderLatches(List<SplitInfo> nodeLadder)
        throws DatabaseException {

        if (nodeLadder.isEmpty()) {
            return;
        }

        nodeLadder.get(0).parent.releaseLatch();

        for (final SplitInfo info : nodeLadder) {
            info.child.releaseLatch();
        }

        nodeLadder.clear();
    }

    /**
     * Find the leftmost node (IN or BIN) in the tree.
     *
     * @return the leftmost node in the tree, null if the tree is empty.  The
     * returned node is latched and the caller must release it.
     */
    // 查找整棵 B+树最左侧（最小）叶子节点的方法
    // 通过树的极左侧深度搜索，精准定位并返回全树第一个叶子节点（BIN），同时确保返回时该 BIN 处于加锁状态，并且如果它是精简的 BIN 格式，会将其安全突变为完整的 BIN。
    public BIN getFirstNode(CacheMode cacheMode)
        throws DatabaseException {

        BIN bin = search(
            null /*key*/, SearchType.LEFT, null /*binBoundary*/,
            cacheMode, null /*comparator*/);

        if (bin != null) {
            bin.mutateToFullBIN(false /*leaveFreeSlot*/);
        }

        return bin;
    }

    /**
     * Find the rightmost node (IN or BIN) in the tree.
     *
     * @return the rightmost node in the tree, null if the tree is empty.  The
     * returned node is latched and the caller must release it.
     */
    // 最右侧（最大）的叶子节点（BIN），返回时该 BIN 已加锁，供上层进行全表扫描或首尾定位。
    public BIN getLastNode(CacheMode cacheMode)
        throws DatabaseException {

        BIN bin = search(
            null /*key*/, SearchType.RIGHT, null /*binBoundary*/,
            cacheMode, null /*comparator*/);

        if (bin != null) {
            bin.mutateToFullBIN(false /*leaveFreeSlot*/);
        }

        return bin;
    }

    /**
     * Return a reference to the adjacent BIN.
     *
     * @param bin The BIN to find the next BIN for.  This BIN is latched.
     *
     * @return The next BIN, or null if there are no more.  The returned node
     * is latched and the caller must release it.  If null is returned, the
     * argument BIN remains latched.
     */
    // 获取给定 BIN 在树中物理相邻的右侧或左侧邻居 BIN，支撑游标的常规前后范围扫描。
    public BIN getNextBin(BIN bin, CacheMode cacheMode)
        throws DatabaseException {

        return (BIN) getNextIN(bin, true, false, cacheMode);
    }

    /**
     * Return a reference to the previous BIN.
     *
     * @param bin The BIN to find the next BIN for.  This BIN is latched.
     *
     * @return The previous BIN, or null if there are no more.  The returned
     * node is latched and the caller must release it.  If null is returned,
     * the argument bin remains latched.
     */
    public BIN getPrevBin(BIN bin, CacheMode cacheMode)
        throws DatabaseException {

        return (BIN) getNextIN(bin, false, false, cacheMode);
    }

    /**
     * Returns the next IN in the tree before/after the given IN, and at the
     * same level.  For example, if a BIN is passed in the prevIn parameter,
     * the next BIN will be returned.
     *
     * TODO: A possible problem with this method is that we don't know for
     * certain whether it works properly in the face of splits.  There are
     * comments below indicating it does.  But the Cursor.checkForInsertion
     * method was apparently added because getNextBin/getPrevBin didn't work
     * properly, and may skip a BIN.  So at least it didn't work properly in
     * the distant past.  Archeology and possibly testing are needed to find
     * the truth.  Hopefully it does now work, and Cursor.checkForInsertion can
     * be removed.
     *
     * TODO: To eliminate EX latches on upper INs, a new getParentINForChildIN
     * is needed, which will return with both the parent and the grandparent
     * SH-latched. If we do this, then in Tree.getNextIN() the call to
     * searchSubtree() will be able to do grandparent latching, and the call
     * to parent.fetchIN(index) will also be replace with a local version of
     * grandparent latching. 
     */
    // 核心横向遍历方法
    // 获取同级相邻的内部节点 / BIN
    // 在 B+树中，当游标（Cursor）在当前的叶子节点（BIN）扫描到头需要跨越到相邻的下一个或上一个 BIN 时，或者在树中进行同层遍历时，就会调用该方法。
    // 它通过“向上回溯父节点（Ascend） $\rightarrow$ 在父节点中寻找相邻兄弟槽位 $\rightarrow$ 向下钻取（Descend）”的经典 B+树邻居查找算法来实现。
    public IN getNextIN(
        // 当前的起始内部节点（通常是 BIN 或 IN）。调用该方法时，当前线程必须已经持有该节点的锁。
        IN prevIn,
        // 如果为 true：表示向右寻找下一个邻居节点（Forward）。
        // 如果为 false：表示向左寻找上一个邻居节点（Backward）
        boolean forward,
        // 如果为 true：返回时对邻居节点施加共享锁（Shared Latch）
        // 如果为 false：返回时对邻居节点施加排他锁（Exclusive Latch）。
        boolean latchShared,
        CacheMode cacheMode) {
        // 调用方必须已经持有 prevIn 的锁。
        assert(prevIn.isLatchOwner());

        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.expectBtreeLatchesHeld(1);
        }
        // 将当前的 prevIn（若是 BIN）突变为完整格式（mutateToFullBIN），确保其条目完整。
        prevIn.mutateToFullBIN(false /*leaveFreeSlot*/);

        /*
         * Use the right most key (for a forward progressing cursor) or the
         * left most key (for a backward progressing cursor) as the search key.
         * The reason is that the IN may get split while finding the next IN so
         * it's not safe to take the IN's identifierKey entry.  If the IN gets
         * split, then the right (left) most key will still be on the
         * resultant node.  The exception to this is that if there are no
         * entries, we just use the identifier key.
         */
        // 确定搜索键（searchKey）
        // 为什么不直接用 identifierKey？：因为在查找邻居的过程中，如果当前节点或兄弟节点发生了分裂（Split），直接用旧的标识键可能不安全。
        // 按方向取键：若正向遍历（forward=true），取当前节点中最右侧（最大）的键；若反向遍历，取最左侧（最小）的键。若当前节点为空，则退化使用其 identifierKey。
        final byte[] searchKey;

        if (prevIn.getNEntries() == 0) {
            searchKey = prevIn.getIdentifierKey();
        } else if (forward) {
            searchKey = prevIn.getKey(prevIn.getNEntries() - 1);
        } else {
            searchKey = prevIn.getKey(0);
        }
        // 记录目标树高层级
        final int targetLevel = prevIn.getLevel();
        // 初始化当前节点指针
        IN curr = prevIn;
        boolean currIsLatched = false;
        // 父节点指针
        IN parent = null;
        // 邻居节点指针
        IN nextIN = null;
        boolean nextINIsLatched = false;
        // 退出标志
        boolean normalExit = false;

        /*
         * Ascend the tree until we find a level that still has nodes to the
         * right (or left if !forward) of the path that we're on.  If we reach
         * the root level, we're done.
         */
        try {
            // 核心：while(true) 循环（向上回溯与横向寻找）
            while (true) {

                /*
                 * Move up a level from where we are now and check to see if we
                 * reached the top of the tree.
                 */
                currIsLatched = false;
                // 如果在回溯过程中 curr 已经是树的根节点（isRoot()），说明已经到了树顶且没有更多同级兄弟了，释放锁并返回 null（表示没有相邻节点了）。
                if (curr.isRoot()) {
                    /* We've reached the root of the tree. */
                    curr.releaseLatch();

                    if (LatchSupport.TRACK_LATCHES) {
                        LatchSupport.expectBtreeLatchesHeld(0);
                    }
                    normalExit = true;
                    return null;
                }
                // 获取当前节点的父节点
                final SearchResult result = getParentINForChildIN(
                    curr, false, /*useTargetLevel*/
                    true, /*doFetch*/ cacheMode);

                if (result.exactParentFound) {
                    if (LatchSupport.TRACK_LATCHES) {
                        LatchSupport.expectBtreeLatchesHeld(1);
                    }
                    parent = result.parent;
                } else {
                    throw EnvironmentFailureException.unexpectedState(
                        "Failed to find parent for IN");
                }

                /*
                 * Figure out which entry we are in the parent. Add (subtract)
                 * 1 to move to the next (previous) one and check that we're
                 * still pointing to a valid child.  Don't just use the result
                 * of the parent.findEntry call in getParentNode, because we
                 * want to use our explicitly chosen search key.
                 */
                // 在父节点中定位兄弟槽位
                int index = parent.findEntry(searchKey, false, false);

                final boolean moreEntriesThisIn;
                // 若正向，索引加 1（index++），判断是否还未超出父节点的条目总数。
                if (forward) {
                    index++;
                    moreEntriesThisIn = (index < parent.getNEntries());
                } else {
                    moreEntriesThisIn = (index > 0);
                    index--;
                }
                // 前父节点内是否还包含同级的右侧（或左侧）兄弟节点。
                // 分支 A：若父节点内存在同级兄弟
                if (moreEntriesThisIn) {

                    /*
                     * There are more entries to the right of the current path
                     * in parent.  Get the entry, and then descend down the
                     * left most path to an IN.
                     */
                    // 直接命中同层邻居：若兄弟节点的树高（level）与目标层级完全一致，说明直接找到了相邻的 BIN 或同级 IN：
                    nextIN = parent.fetchIN(index, cacheMode);

                    if (LatchSupport.TRACK_LATCHES) {
                        LatchSupport.expectBtreeLatchesHeld(1);
                    }

                    if (nextIN.getLevel() == targetLevel) {

                        if (latchShared) {
                            latchChildShared(parent, nextIN, cacheMode);
                        } else {
                            latchChild(parent, nextIN, cacheMode);
                        }
                        nextINIsLatched = true;

                        nextIN.mutateToFullBIN(false /*leaveFreeSlot*/);

                        parent.releaseLatch();
                        parent = null; // to avoid falsely unlatching parent

                        final TreeWalkerStatsAccumulator treeStatsAccumulator =
                            getTreeStatsAccumulator();
                        if (treeStatsAccumulator != null) {
                            nextIN.accumulateStats(treeStatsAccumulator);
                        }

                        if (LatchSupport.TRACK_LATCHES) {
                            LatchSupport.expectBtreeLatchesHeld(1);
                        }

                        normalExit = true;
                        return nextIN;

                    } else {
                    // 分支 B：若落在了更高层的内部节点 (else 块)
                    // 跨子树向下钻取：如果获取到的 nextIN 处于更高的层级（说明当前子树已经扫描完，需要跨越到隔壁子树的最左侧或最右侧叶子节点），则对其加锁、释放父锁，然后调用 searchSubTree 一路向下钻取到目标层级并返回。
                        /*
                         * We landed at a higher level than the target level.
                         * Descend down to the appropriate level.
                         */
                        assert(nextIN.isUpperIN());
                        nextIN.latch(cacheMode);
                        nextINIsLatched = true;

                        parent.releaseLatch();
                        parent = null; // to avoid falsely unlatching parent
                        nextINIsLatched = false;

                        final IN ret = searchSubTree(
                            nextIN, null, /*key*/
                            (forward ? SearchType.LEFT : SearchType.RIGHT),
                            targetLevel, latchShared, cacheMode,
                            null /*comparator*/);

                        if (LatchSupport.TRACK_LATCHES) {
                            LatchSupport.expectBtreeLatchesHeld(1);
                        }

                        if (ret.getLevel() == targetLevel) {
                            normalExit = true;
                            return ret;
                        } else {
                            throw EnvironmentFailureException.unexpectedState(
                                "subtree did not have a IN at level " +
                                targetLevel);
                        }
                    }
                }

                /* Nothing at this level. Ascend to a higher level. */
                // 分支 C：若当前父节点已无更多兄弟（继续向上回溯）
                curr = parent;
                currIsLatched = true;
                parent = null; // to avoid falsely unlatching parent below
            }
        } finally {
            if (!normalExit) {
                if (curr != null && currIsLatched) {
                    curr.releaseLatch();
                }

                if (parent != null) {
                    parent.releaseLatch();
                }

                if (nextIN != null && nextINIsLatched) {
                    nextIN.releaseLatch();
                }
            }
        }
    }

    /**
     * Search for the parent P of a given IN C (where C is viewed as a logical
     * node; not as a java obj). If found, P is returned latched exclusively.
     * The method is used when C has been accessed "directly", i.e., not via a
     * tree search, and we need to perform an operation on C that requires an
     * update to its parent. Such situations arise during eviction (C has been
     * accessed via the LRU list), checkpointing, and recovery (C has been read
     * from the log and is not attached to the in-memory tree).
     *
     * The method uses C's identifierKey to search down the tree until:
     *
     * (a) doFetch is false and we need to access a node that is not cached.
     * In this case, we are actually looking for the cached copies of both C
     * and its parent, so a cache miss on the path to C is considered a 
     * failure. This search mode is used by the evictor: to evict C (which has
     * been retrieved from the LRU), its parent must be found and EX-latched;
     * however, if the C has been evicted already by another thread, there is
     * nothing to do (C will GC-ed).
     * or
     * (b) We reach a node whose node id equals the C's node id. In this case,
     * we know for sure that C still belongs to the BTree and its parent has
     * been found.
     * or
     * (c) useTargetLevel is true and we reach a node P that is at one level
     * above C's level. We know that P contains a slot S whose corresponding
     * key range includes C's identifierKey. Since we haven't read the child
     * node under S to check its node id, we cannot know for sure that C is
     * still in the tree. Nevertheless, we consider this situation a success,
     * i.e., P is the parent node we are looking for. In this search mode,
     * after this method returns, the caller is expected to take further
     * action based on the info in slot S. For example, if C was created
     * by reading a log entry at LSN L, and the LSN at slot S is also L, then
     * we know P is the real parent (and we have thus saved a possible extra
     * I/O to refetch the C node from the log to check its node id). This
     * search mode is used by the cleaner.
     * or
     * (d) None of the above conditions occur and the bottom of the BTree is
     * reached. In this case, no parent exists (the child node is an old 
     * version of a node that has been removed from the BTree).
     *
     * @param child The child node for which to find the parent.  This node is
     * latched by the caller and is unlatched by this function before returning
     * to the caller.
     *
     * @param useTargetLevel If true, the search is considered successful if
     * a node P is reached at one level above C's level. P is the parent to
     * return to the caller.
     *
     * @param doFetch if false, stop the search if we run into a non-resident
     * child and assume that no parent exists.
     *
     * @param cacheMode The CacheMode for affecting the hotness of the nodes
     * visited during the search.
     *
     * @return a SearchResult object. If the parent has been found,
     * result.foundExactMatch is true, result.parent refers to that node, and
     * result.index is the slot for the child IN inside the parent IN.
     * Otherwise, result.foundExactMatch is false and  result.parent is null.
     */
    public SearchResult getParentINForChildIN(
        IN child,
        boolean useTargetLevel,
        boolean doFetch,
        CacheMode cacheMode)
        throws DatabaseException {

        return getParentINForChildIN(
            child, useTargetLevel, doFetch,
            cacheMode, null /*trackingList*/);
    }


    /**
     * This version of getParentINForChildIN does the same thing as the version
     * above, but also adds a "trackingList" param. If trackingList is not
     * null, the LSNs of the parents visited along the way are added to the
     * list, as a debug tracing mechanism. This is meant to stay in production,
     * to add information to the log.
     */
    public SearchResult getParentINForChildIN(
        IN child,
        boolean useTargetLevel,
        boolean doFetch,
        CacheMode cacheMode,
        List<TrackingInfo> trackingList)
        throws DatabaseException {

        /* Sanity checks */
        if (child == null) {
            throw EnvironmentFailureException.unexpectedState(
                "getParentINForChildIN given null child node");
        }

        assert child.isLatchOwner();

        /*
         * Get information from child before releasing latch.
         */
        long targetId = child.getNodeId();
        byte[] targetKey = child.getIdentifierKey();
        int targetLevel = (useTargetLevel ? child.getLevel() : -1);
        int exclusiveLevel = child.getLevel() + 1;
        boolean requireExactMatch = true;

        child.releaseLatch();

        return getParentINForChildIN(
            targetId, targetKey, targetLevel,
            exclusiveLevel, requireExactMatch, doFetch,
            cacheMode, trackingList);
    }

    /**
     * This version of getParentINForChildIN() is the actual implementation
     * of the previous 2 versions (read the comments there), but it also
     * implements one additional use cases via the extra "requireExactMatch"
     * param.
     *
     * requireExactMatch == false && doFetch == false
     * In this case we are actually looking for the lowest cached ancestor
     * of the C node. The method will always return a node (considered as the
     * "parent") unless the BTree is empty (has no nodes at all). The returned
     * node must be latched, but not necessarily in EX mode. This search mode
     * is used by the checkpointer.
     *
     * The exclusiveLevel param:
     * In general, if exclusiveLevel == L, nodes above L will be SH latched and
     * nodes at or below L will be EX-latched. In all current use cases, L is
     * set to 1 + C.level. Note that if doFetch == false, the normalized
     * exclusiveLevel must be >= 2 so that loadIN can be called.
     */
    public SearchResult getParentINForChildIN(
        long targetNodeId,
        byte[] targetKey,
        int targetLevel,
        int exclusiveLevel,
        boolean requireExactMatch,
        boolean doFetch,
        CacheMode cacheMode,
        List<TrackingInfo> trackingList)
        throws DatabaseException {

        /* Call hook before latching. No latches are held. */
        TestHookExecute.doHookIfSet(getParentINHook);

        assert doFetch || (exclusiveLevel & IN.LEVEL_MASK) >= 2;

        /*
         * SearchResult is initialized as follows:
         * exactParentFound = false;
         * parent = null; index = -1; childNotResident = false;
         */
        SearchResult result = new SearchResult();

        /* Get the tree root, SH-latched. */
        IN rootIN = getRootIN(cacheMode);

        if (rootIN == null) {
            return result;
        }

        /* If the root is the target node, there is no parent */
        assert(rootIN.getNodeId() != targetNodeId);
        assert(rootIN.getLevel() >= exclusiveLevel) :
            " rootLevel=" + rootIN.getLevel() +
            " exLevel=" + exclusiveLevel;

        IN parent = rootIN;
        IN child = null;
        boolean success = false;

        try {

            if (rootIN.getLevel() <= exclusiveLevel) {
                rootIN.releaseLatch();
                rootIN = getRootINLatchedExclusive(cacheMode);
                assert(rootIN != null);
                parent = rootIN;
            }

            while (true) {

                assert(parent.getNEntries() > 0);

                result.index = parent.findEntry(targetKey, false, false);

                if (trackingList != null) {
                    trackingList.add(new TrackingInfo(
                        parent.getLsn(result.index), parent.getNodeId(),
                        parent.getNEntries(), result.index));
                }

                assert TestHookExecute.doHookIfSet(searchHook);

                if (targetLevel > 0 && parent.getLevel() == targetLevel + 1) {
                    result.exactParentFound = true;
                    result.parent = parent;
                    break;
                }

                if (doFetch) {
                    child = parent.fetchINWithNoLatch(
                        result, targetKey, cacheMode);

                    if (child == null) {
                        if (trackingList != null) {
                            trackingList.clear();
                        }
                        result.reset();

                        TestHookExecute.doHookIfSet(fetchINHook, child);

                        rootIN = getRootIN(cacheMode);
                        assert(rootIN != null);

                        if (rootIN.getLevel() <= exclusiveLevel) {
                            rootIN.releaseLatch();
                            rootIN = getRootINLatchedExclusive(cacheMode);
                            assert(rootIN != null);
                        }

                        parent = rootIN;
                        continue;
                    }
                } else {

                    /*
                     * We can only call loadIN if we have an EX-latch on the
                     * parent. However, calling loadIN is only necessary when
                     * the parent is at level 2, since UINs are not cached
                     * off-heap, and exclusiveLevel is currently always >= 2.
                     */
                    if (parent.getNormalizedLevel() == 2) {
                        child = parent.loadIN(result.index, cacheMode);
                    } else {
                        child = (IN) parent.getTarget(result.index);
                    }
                }

                assert(child != null || !doFetch);

                if (child == null) {
                    if (requireExactMatch) {
                        parent.releaseLatch();
                    } else {
                        result.parent = parent;
                    }
                    result.childNotResident = true;
                    break;
                }

                if (child.getNodeId() == targetNodeId) {
                    result.exactParentFound = true;
                    result.parent = parent;
                    break;
                }

                if (child.isBIN()) {
                    if (requireExactMatch) {
                        parent.releaseLatch();
                    } else {
                        result.parent = parent;
                    }
                    break;
                }

                /* We can search further down the tree. */
                if (child.getLevel() <= exclusiveLevel) {
                    latchChild(parent, child, cacheMode);
                } else {
                    latchChildShared(parent, child, cacheMode);
                }

                parent.releaseLatch();
                parent = child;
                child = null;
            }

            success = true;
            
        } finally {
            
            if (!success) {
                if (parent.isLatchOwner()) {
                    parent.releaseLatch();
                }

                if (child != null && child.isLatchOwner()) {
                    child.releaseLatch();
                }
            }
        }

        if (result.parent != null) {
            if (LatchSupport.TRACK_LATCHES) {
                LatchSupport.expectBtreeLatchesHeld(1);
            }
            assert((!doFetch && !requireExactMatch) ||
                   result.parent.isLatchOwner());
        }

        return result;
    }

    /**
     * Return a reference to the parent of this LN. This searches through the
     * tree and allows splits, if the splitsAllowed param is true. Set the
     * tree location to the proper BIN parent whether or not the LN child is
     * found. That's because if the LN is not found, recovery or abort will
     * need to place it within the tree, and so we must point at the
     * appropriate position.
     *
     * <p>When this method returns with location.bin non-null, the BIN is
     * latched and must be unlatched by the caller.  Note that location.bin may
     * be non-null even if this method returns false.</p>
     *
     * @param location a holder class to hold state about the location
     * of our search. Sort of an internal cursor.
     *
     * @param key key to navigate through main key
     *
     * @param splitsAllowed true if this method is allowed to cause tree splits
     * as a side effect. In practice, recovery can cause splits, but abort
     * can't.
     *
     * @param blindDeltaOps Normally, if this method lands on a BIN-delta and
     * the search key is not in that delta, it will mutate the delta to a full
     * BIN to make sure whether the search key exists in the tree or not. 
     * However, by passing true for blindDeltaOps, the caller indicates that 
     * it doesn't really care whether the key is in the tree or not: it is
     * going to insert the key in the BIN-delta, if not already there,
     * essentially overwritting the slot that may exist in the full BIN. So,
     * if blindDeltaOps is true, the method will not mutate a BIN-delta parent
     * (unless the BIN-delta has no space for a slot insertion).
     *
     * @param cacheMode The CacheMode for affecting the hotness of the tree.
     *
     * @return true if node found in tree.
     * If false is returned and there is the possibility that we can insert
     * the record into a plausible parent we must also set
     * - location.bin (may be null if no possible parent found)
     * - location.lnKey (don't need to set if no possible parent).
     */
    public boolean getParentBINForChildLN(
        TreeLocation location,
        byte[] key,
        boolean splitsAllowed,
        boolean blindDeltaOps,
        CacheMode cacheMode)
        throws DatabaseException {

        /*
         * Find the BIN that either points to this LN or could be its
         * ancestor.
         */
        location.reset();
        BIN bin;
        int index;

        if (splitsAllowed) {
            bin = searchSplitsAllowed(key, cacheMode, null /*comparator*/);
        } else {
            bin = search(key, cacheMode);
        }

        if (bin == null) {
            return false;
        }

        try {
            while (true) {

                location.bin = bin;

                index = bin.findEntry(
                    key, true /*indicateIfExact*/, false /*exactSearch*/);

                boolean match = (index >= 0 &&
                                 (index & IN.EXACT_MATCH) != 0);

                index &= ~IN.EXACT_MATCH;
                location.index = index;
                location.lnKey = key;

                /*
                if (!match && bin.isBINDelta() && blindDeltaOps) {
                      System.out.println(
                          "Blind op on BIN-delta : " + bin.getNodeId() +
                          " nEntries = " +
                          bin.getNEntries() +
                          " max entries = " +
                          bin.getMaxEntries() +
                          " full BIN entries = " +
                          bin.getFullBinNEntries() +
                          " full BIN max entries = " +
                          bin.getFullBinMaxEntries());
                }
                */

                if (match) {
                    location.childLsn = bin.getLsn(index);
                    location.childLoggedSize = bin.getLastLoggedSize(index);
                    location.isKD = bin.isEntryKnownDeleted(index);
                    location.isEmbedded = bin.isEmbeddedLN(index);

                    return true;

                } else {

                    if (bin.isBINDelta() &&
                        (!blindDeltaOps ||
                         bin.getNEntries() >= bin.getMaxEntries())) {

                        bin.mutateToFullBIN(splitsAllowed /*leaveFreeSlot*/);
                        location.reset();
                        continue;
                    }

                    return false;
                }
            }

        } catch (RuntimeException e) {
            bin.releaseLatch();
            location.bin = null;
            throw e;
        }
    }

    /**
     * Find the BIN that is relevant to the insert.  If the tree doesn't exist
     * yet, then create the first IN and BIN.  On return, the cursor is set to
     * the BIN that is found or created, and the BIN is latched.
     */
    public BIN findBinForInsert(final byte[] key, final CacheMode cacheMode) {

        boolean rootLatchIsHeld = false;
        BIN bin = null;

        try {
            long logLsn;

            /*
             * We may have to try several times because of a small
             * timing window, explained below.
             */
            while (true) {

                rootLatchIsHeld = true;
                rootLatch.acquireShared();

                if (!rootExists()) {

                    rootLatch.release();
                    rootLatch.acquireExclusive();
                    if (rootExists()) {
                        rootLatch.release();
                        rootLatchIsHeld = false;
                        continue;
                    }

                    final EnvironmentImpl env = database.getEnv();
                    final INList inMemoryINs = env.getInMemoryINs();

                    /*
                     * This is an empty tree, either because it's brand new
                     * tree or because everything in it was deleted. Create an
                     * IN and a BIN.  We could latch the rootIN here, but
                     * there's no reason to since we're just creating the
                     * initial tree and we have the rootLatch held. Remember
                     * that referred-to children must be logged before any
                     * references to their LSNs.
                     */
                    IN rootIN =
                        new IN(database, key, maxTreeEntriesPerNode, 2);
                    rootIN.setIsRoot(true);

                    rootIN.latch(cacheMode);

                    /* First BIN in the tree, log provisionally right away. */
                    bin = new BIN(database, key, maxTreeEntriesPerNode, 1);
                    bin.latch(cacheMode);
                    logLsn = bin.optionalLogProvisionalNoCompress(rootIN);

                    /*
                     * Log the root right away. Leave the root dirty, because
                     * the MapLN is not being updated, and we want to avoid
                     * this scenario from [#13897], where the LN has no
                     * possible parent.
                     *  provisional BIN
                     *  root IN
                     *  checkpoint start
                     *  LN is logged
                     *  checkpoint end
                     *  BIN is dirtied, but is not part of checkpoint
                     */
                    boolean insertOk = rootIN.insertEntry(bin, key, logLsn);
                    assert insertOk;

                    logLsn = rootIN.optionalLog();
                    rootIN.setDirty(true);  /*force re-logging, see [#13897]*/

                    root = makeRootChildReference(rootIN, new byte[0], logLsn);

                    rootIN.releaseLatch();

                    /* Add the new nodes to the in memory list. */
                    inMemoryINs.add(bin);
                    inMemoryINs.add(rootIN);
                    env.getEvictor().addBack(bin);

                    rootLatch.release();
                    rootLatchIsHeld = false;

                    break;
                } else {
                    rootLatch.release();
                    rootLatchIsHeld = false;

                    /*
                     * There's a tree here, so search for where we should
                     * insert. However, note that a window exists after we
                     * release the root latch. We release the latch because the
                     * search method expects to take the latch. After the
                     * release and before search, the INCompressor may come in
                     * and delete the entire tree, so search may return with a
                     * null.
                     */
                    bin = searchSplitsAllowed(key, cacheMode);

                    if (bin == null) {
                        /* The tree was deleted by the INCompressor. */
                        continue;
                    } else {
                        /* search() found a BIN where this key belongs. */
                        break;
                    }
                }
            }
        } finally {
            if (rootLatchIsHeld) {
                rootLatch.release();
            }
        }

        /* testing hook to insert item into log. */
        assert TestHookExecute.doHookIfSet(ckptHook);
        
        return bin;
    }

    /**
     * Do a key based search, permitting pre-emptive splits. Returns the
     * target node's parent.
     */
    public BIN searchSplitsAllowed(byte[] key, CacheMode cacheMode) {

        return searchSplitsAllowed(key, cacheMode, null);
    }


    private BIN searchSplitsAllowed(
        byte[] key,
        CacheMode cacheMode,
        Comparator<byte[]> comparator) {
        
        BIN insertTarget = null;

        while (insertTarget == null) {

            rootLatch.acquireShared();

            boolean rootLatched = true;
            boolean rootINLatched = false;
            boolean success = false;
            IN rootIN = null;

            /*
             * Latch the rootIN, check if it needs splitting. If so split it
             * and update the associated MapLN. To update the MapLN, we must
             * lock it, which implies that all latches must be released prior
             * to the lock, and as a result, the root may require splitting
             * again or may be split by another thread. So we must restart
             * the loop to get the latest root.
             */
            try {
                if (!rootExists()) {
                    return null;
                }

                rootIN = (IN) root.fetchTarget(database, null);

                if (rootIN.needsSplitting()) {

                    rootLatch.release();
                    rootLatch.acquireExclusive();

                    if (!rootExists()) {
                        return null;
                    }

                    rootIN = (IN) root.fetchTarget(database, null);

                    if (rootIN.needsSplitting()) {

                        splitRoot(cacheMode);

                        rootLatch.release();
                        rootLatched = false;

                        EnvironmentImpl env = database.getEnv();
                        env.getDbTree().optionalModifyDbRoot(database);

                        continue;
                    }
                }

                rootIN.latchShared(cacheMode);
                rootINLatched = true;
                success = true;

            } finally {
                if (!success && rootINLatched) {
                    rootIN.releaseLatch();
                }
                if (rootLatched) {
                    rootLatch.release();
                }
            }

            /*
             * Now, search the tree, doing splits if required. The rootIN
             * is latched in SH mode, but this.root is not latched. If any
             * splits are needed, this.root will first be latched exclusivelly
             * and will stay latched until all splits are done.
             */
            try {
                assert(rootINLatched);
                
                insertTarget = searchSplitsAllowed(
                    rootIN, key, cacheMode, comparator);

                if (insertTarget == null) {
                    if (LatchSupport.TRACK_LATCHES) {
                        LatchSupport.expectBtreeLatchesHeld(0);
                    }
                    relatchesRequired.increment();
                    database.getEnv().incRelatchesRequired();
                }
            } catch (SplitRequiredException e) {

                /*
                 * The last slot in the root was used at the point when this
                 * thread released the rootIN latch in order to force splits.
                 * Retry. SR [#11147].
                 */
                continue;
            }
        }

        return insertTarget;
    }

    /**
     * Search the tree, permitting preemptive splits.
     *
     * When this returns, parent will be unlatched unless parent is the
     * returned IN.
     */
    private BIN searchSplitsAllowed(
        IN rootIN,
        byte[] key,
        CacheMode cacheMode,
        Comparator<byte[]> comparator)
        throws SplitRequiredException {

        assert(rootIN.isLatchOwner());
        if (!rootIN.isRoot()) {
            throw EnvironmentFailureException.unexpectedState(
                "A null or non-root IN was given as the parent");
        }

        int index;
        IN parent = rootIN;
        IN child = null;
        boolean success = false;

        /*
         * Search downward until we hit a node that needs a split. In that
         * case, retreat to the top of the tree and force splits downward.
         */        
        try {
            do {
                if (parent.getNEntries() == 0) {
                    throw EnvironmentFailureException.unexpectedState(
                        "Found upper IN with 0 entries");
                }

                index = parent.findEntry(key, false, false, comparator);
                assert index >= 0;

                child = parent.fetchINWithNoLatch(index, key, cacheMode);

                if (child == null) {
                    return null; // restart the search
                }

                /* if child is a BIN, it is actually EX-latched */
                latchChildShared(parent, child, cacheMode);

                /*
                 * If we need to split, try compressing first and check again.
                 * Mutate to a full BIN because compression has no impact on a
                 * BIN-delta, and a full BIN is needed for splitting anyway.
                 */
                if (child.needsSplitting()) {

                    child.mutateToFullBIN(false /*leaveFreeSlot*/);

                    database.getEnv().lazyCompress(
                        child, true /*compressDirtySlots*/);

                    if (child.needsSplitting()) {

                        child.releaseLatch();
                        parent.releaseLatch();
                        
                        /* SR [#11144]*/
                        assert TestHookExecute.doHookIfSet(waitHook);
                                
                        /*
                         * forceSplit may throw SplitRequiredException if it
                         * finds that the root needs splitting. Allow the
                         * exception to propagate up to the caller, who will
                         * do the root split. Otherwise, restart the search
                         * from the root IN again.
                         */
                        rootIN = forceSplit(key, cacheMode);
                        parent = rootIN;

                        assert(rootIN.isLatchOwner());
                        if (!rootIN.isRoot()) {
                            throw EnvironmentFailureException.unexpectedState(
                            "A null or non-root IN was given as the parent");
                        }
                        continue;
                    }
                }

                /* Continue down a level */
                parent.releaseLatch();
                parent = child;
                child = null;
                
            } while (!parent.isBIN());

            success = true;
            return (BIN)parent;

        } finally {
            if (!success) {
                if (child != null && child.isLatchOwner()) {
                    child.releaseLatch();
                }
                if (parent != child && parent.isLatchOwner()) {
                    parent.releaseLatch();
                }
            }
        }
    }

    /**
     * Do pre-emptive splitting: search down the tree until we get to the BIN
     * level, and split any nodes that fit the splittable requirement except
     * for the root. If the root needs splitting, a splitRequiredException
     * is thrown and the root split is handled at a higher level.
     *
     * Note that more than one node in the path may be splittable. For example,
     * a tree might have a level2 IN and a BIN that are both splittable, and
     * would be encountered by the same insert operation.
     *
     * Splits cause INs to be logged in all ancestors, including the root. This
     * is to avoid the "great aunt" problem described in LevelRecorder.
     *
     * INs below the root are logged provisionally; only the root is logged
     * non-provisionally. Provisional logging is necessary during a checkpoint
     * for levels less than maxFlushLevel.
     *
     * This method acquires and holds this.rootLatch in EX mode during its
     * whole duration (so splits are serialized). The rootLatch is released
     * on return.
     *
     * @return the tree root node, latched in EX mode. This may be different
     * than the tree root when this method was called, because no latches are
     * held on entering this method. 
     *
     * All latches are released in case of exception.
     */
    private IN forceSplit(byte[] key, CacheMode cacheMode)
        throws DatabaseException, SplitRequiredException {

        final ArrayList<SplitInfo> nodeLadder = new ArrayList<SplitInfo>();

        boolean allLeftSideDescent = true;
        boolean allRightSideDescent = true;
        int index;
        IN parent;
        IN child = null;
        IN rootIN = null;

        /*
         * Latch the root in order to update the root LSN when we're done.
         * Latch order must be: root, root IN. We'll leave this method with the
         * original parent latched.
         */
        rootLatch.acquireExclusive();

        boolean success = false;
        try {
            /* The root IN may have been evicted. [#16173] */
            rootIN = (IN) root.fetchTarget(database, null);
            parent = rootIN;
            parent.latch(cacheMode);

            /*
             * Another thread may have crept in and
             *  - used the last free slot in the parent, making it impossible
             *    to correctly propagate the split.
             *  - actually split the root, in which case we may be looking at
             *    the wrong subtree for this search.
             * If so, throw and retry from above. SR [#11144]
             */
            if (rootIN.needsSplitting()) {
                throw splitRequiredException;
            }

            /*
             * Search downward to the BIN level, saving the information
             * needed to do a split if necessary.
             */
            do {
                if (parent.getNEntries() == 0) {
                    throw EnvironmentFailureException.unexpectedState(
                        "Found upper IN with 0 entries");
                }

                /* Look for the entry matching key in the current node. */
                index = parent.findEntry(key, false, false);
                assert index >= 0;
                if (index != 0) {
                    allLeftSideDescent = false;
                }
                if (index != (parent.getNEntries() - 1)) {
                    allRightSideDescent = false;
                }

                /*
                 * Get the child node that matches. We only need to work on
                 * nodes in residence.
                 */
                child = parent.loadIN(index, cacheMode);

                if (child == null) {
                    break;
                }

                latchChild(parent, child, cacheMode);

                nodeLadder.add(new SplitInfo(parent, child, index));

                /* Continue down a level */
                parent = child;
            } while (!parent.isBIN());

            boolean startedSplits = false;

            /*
             * Process the accumulated nodes from the bottom up. Split each
             * node if required. If the node should not split, we check if
             * there have been any splits on the ladder yet. If there are none,
             * we merely release the node, since there is no update.  If splits
             * have started, we need to propagate new LSNs upward, so we log
             * the node and update its parent.
             */
            long lastParentForSplit = Node.NULL_NODE_ID;

            for (int i = nodeLadder.size() - 1; i >= 0; i -= 1) {
                final SplitInfo info = nodeLadder.get(i);

                child = info.child;
                parent = info.parent;
                index = info.index;

                /* Opportunistically split the node if it is full. */
                if (child.needsSplitting()) {

                    child.mutateToFullBIN(false /*leaveFreeSlot*/);

                    final IN grandParent =
                        (i > 0) ? nodeLadder.get(i - 1).parent : null;

                    if (allLeftSideDescent || allRightSideDescent) {
                        child.splitSpecial(
                            parent, index, grandParent, maxTreeEntriesPerNode,
                            key, allLeftSideDescent);
                    } else {
                        child.split(
                            parent, index, grandParent, maxTreeEntriesPerNode);
                    }

                    lastParentForSplit = parent.getNodeId();
                    startedSplits = true;

                    /*
                     * If the DB root IN was logged, update the DB tree's child
                     * reference. Now the MapLN is logically dirty. Be sure to
                     * flush the MapLN if we ever evict the root.
                     */
                    if (parent.isRoot()) {
                        root.updateLsnAfterOptionalLog(
                            database, parent.getLastLoggedLsn());
                    }
                } else {
                    if (startedSplits) {
                        final long newChildLsn;

                        /*
                         * If this child was the parent of a split, it's
                         * already logged by the split call. We just need to
                         * propagate the logging upwards. If this child is just
                         * a link in the chain upwards, log it.
                         */
                        if (lastParentForSplit == child.getNodeId()) {
                            newChildLsn = child.getLastLoggedLsn();
                        } else {
                            newChildLsn = child.optionalLogProvisional(parent);
                        }

                        parent.updateEntry(
                            index, newChildLsn, VLSN.NULL_VLSN_SEQUENCE,
                            0/*lastLoggedSize*/);

                        /*
                         * The root is never a 'child' in nodeLadder so it must
                         * be logged separately.
                         */
                        if (parent.isRoot()) {

                            final long newRootLsn = parent.optionalLog();

                            root.updateLsnAfterOptionalLog(
                                database, newRootLsn);
                        }
                    }
                }
                child.releaseLatch();
                child = null;
            }
            success = true;
        } finally {
            if (!success) {
                if (child != null) {
                    child.releaseLatchIfOwner();
                }

                for (SplitInfo info : nodeLadder) {
                    info.child.releaseLatchIfOwner();
                }

                if (rootIN != null) {
                    rootIN.releaseLatchIfOwner();
                }
            }

            rootLatch.release();
        }

        return rootIN;
    }

    /**
     * Split the root of the tree.
     */
    private void splitRoot(CacheMode cacheMode)
        throws DatabaseException {

        /*
         * Create a new root IN, insert the current root IN into it, and then
         * call split.
         */
        EnvironmentImpl env = database.getEnv();
        INList inMemoryINs = env.getInMemoryINs();

        IN curRoot = null;
        curRoot = (IN) root.fetchTarget(database, null);
        curRoot.latch(cacheMode);
        long curRootLsn = 0;
        long logLsn = 0;
        IN newRoot = null;
        try {

            /*
             * Make a new root IN, giving it an id key from the previous root.
             */
            byte[] rootIdKey = curRoot.getKey(0);
            newRoot = new IN(database, rootIdKey, maxTreeEntriesPerNode,
                             curRoot.getLevel() + 1);
            newRoot.latch(cacheMode);
            newRoot.setIsRoot(true);
            curRoot.setIsRoot(false);

            /*
             * Make the new root IN point to the old root IN. Log the old root
             * provisionally, because we modified it so it's not the root
             * anymore, then log the new root. We are guaranteed to be able to
             * insert entries, since we just made this root.
             */
            boolean logSuccess = false;
            try {
                curRootLsn = curRoot.optionalLogProvisional(newRoot);

                boolean inserted = newRoot.insertEntry(
                    curRoot, rootIdKey, curRootLsn);
                assert inserted;

                logLsn = newRoot.optionalLog();
                logSuccess = true;
            } finally {
                if (!logSuccess) {
                    /* Something went wrong when we tried to log. */
                    curRoot.setIsRoot(true);
                }
            }

            inMemoryINs.add(newRoot);

            /*
             * Don't add the new root into the LRU because it has a cached
             * child.
             */

            /*
             * Make the tree's root reference point to this new node. Now the
             * MapLN is logically dirty, but the change hasn't been logged.  Be
             * sure to flush the MapLN if we ever evict the root.
             */
            root.setTarget(newRoot);
            root.updateLsnAfterOptionalLog(database, logLsn);
            curRoot.split(newRoot, 0, null, maxTreeEntriesPerNode);
            root.setLsn(newRoot.getLastLoggedLsn());

        } finally {
            /* FindBugs ignore possible null pointer dereference of newRoot. */
            newRoot.releaseLatch();
            curRoot.releaseLatch();
        }
        rootSplits.increment();
        traceSplitRoot(Level.FINE, TRACE_ROOT_SPLIT, newRoot, logLsn,
                       curRoot, curRootLsn);
    }

    public BIN search(byte[] key, CacheMode cacheMode) {

        return search(key, SearchType.NORMAL, null, cacheMode, null);
    }

    /**
     * Search the tree, starting at the root. Depending on search type either
     * (a) search for the BIN that *should* contain a given key, or (b) return
     * the right-most or left-most BIN in the tree.
     *
     * Preemptive splitting is not done during the search.
     *
     * @param key - the key to search for, or null if searchType is LEFT or
     * RIGHT.
     *
     * @param searchType - The type of tree search to perform.  NORMAL means
     * we're searching for key in the tree.  LEFT/RIGHT means we're descending
     * down the left or right side, resp.
     *
     * @param binBoundary - If non-null, information is returned about whether
     * the BIN found is the first or last BIN in the database.
     *
     * @return - the BIN that matches the criteria, if any. Returns null if
     * the root is null. BIN is latched (unless it's null) and must be
     * unlatched by the caller.  In a NORMAL search, it is the caller's
     * responsibility to do the findEntry() call on the key and BIN to locate
     * the entry (if any) that matches key. 
     */
    // 最核心的向下检索方法
    // 从 B+树的根节点（Root IN）出发，根据指定的搜索类型（常规查找特定 Key，或者寻找全树最左/最右的极值 BIN），通过锁耦合（Latch Coupling）机制一层层向下遍历，最终精准定位并返回目标叶子节点（BIN）。
    // 返回时该 BIN 已被加锁，调用方使用完毕后必须负责将其释放。
    public BIN search(
        // 要查找的目标键（Key）。如果 searchType 是 LEFT 或 RIGHT（寻找极值），此参数通常传入 null。
        byte[] key,
        // 树的搜索类型枚举。
        SearchType searchType,
        // 边界追踪标志对象（可为 null）。如果传入非空对象，该方法会顺便计算并返回所找到的 BIN 是否为数据库中的“第一个 BIN”或“最后一个 BIN”。
        BINBoundary binBoundary,
        // 缓存控制模式。在加载子节点时控制 LRU 队列更新及缓存行为。
        CacheMode cacheMode,
        // 缓存控制模式。在加载子节点时控制 LRU 队列更新及缓存行为。
        Comparator<byte[]> comparator) {
        // 获取根节点并判空
        IN rootIN = getRootIN(cacheMode);

        if (rootIN == null) {
            return null;
        }
        // 确保如果执行 LEFT 或 RIGHT 极值搜索时，key 必须为 null
        assert ((searchType != SearchType.LEFT &&
                 searchType != SearchType.RIGHT) || key == null);
        // 若传入了 binBoundary 追踪器，先将其默认设为既是第一也是最后，后续在向下遍历槽位时再逐层修正。
        if (binBoundary != null) {
            binBoundary.isLastBin = true;
            binBoundary.isFirstBin = true;
        }
        // 标记是否成功到达目标 BIN
        boolean success = false;
        // 槽位索引
        int index;
        // 从根节点 rootIN 开始
        IN parent = rootIN;
        // 子节点引用
        IN child = null;

        TreeWalkerStatsAccumulator treeStatsAccumulator =
            getTreeStatsAccumulator();

        try {
            if (treeStatsAccumulator != null) {
                parent.accumulateStats(treeStatsAccumulator);
            }

            do {
                // 核心遍历循环 (do-while)
                // 如果上层内部节点（IN）的条目数为 0，说明树状态异常，抛出环境失败异常。
                if (parent.getNEntries() == 0) {
                    throw EnvironmentFailureException.unexpectedState(
                        "Upper IN with 0 entries");
                }
                // NORMAL 查找
                // 在当前 IN 的有序槽位中通过二分查找定位匹配或应插入的 Key 所在索引。
                if (searchType == SearchType.NORMAL) {
                    index = parent.findEntry(key, false, false, comparator);
                // LEFT 查找
                // 直接锁定最左侧的第一个槽位（索引 0）
                } else if (searchType == SearchType.LEFT) {
                    index = 0;
                // RIGHT 查找
                // 直接锁定最右侧的最后一个槽位（索引 parent.getNEntries() - 1）。
                } else if (searchType == SearchType.RIGHT) {
                    index = parent.getNEntries() - 1;

                } else {
                    throw EnvironmentFailureException.unexpectedState(
                        "Invalid value of searchType: " + searchType);
                }

                assert(index >= 0);
                // 更新 BIN 边界状态
                if (binBoundary != null) {
                    if (index != parent.getNEntries() - 1) {
                        binBoundary.isLastBin = false;
                    }
                    if (index != 0) {
                        binBoundary.isFirstBin = false;
                    }
                }
                // 在不加锁的情况下获取子节点（防止死锁或长时间阻塞）。若子节点未在内存中，会触发从磁盘加载。
                child = parent.fetchINWithNoLatch(index, key, cacheMode);
                // 并发空指针防御：若并发环境下子节点刚好被驱逐（child == null），则退守回根节点重新开始循环。
                if (child == null) {
                    parent = getRootIN(cacheMode);
                    assert(parent != null);
                    if (treeStatsAccumulator != null) {
                        parent.accumulateStats(treeStatsAccumulator);
                    }
                    continue;
                }

                /* Latch the child. Note: BINs are always latched exclusive. */
                // 对子节点 child 加锁

                latchChildShared(parent, child, cacheMode);

                if (treeStatsAccumulator != null) {
                    child.accumulateStats(treeStatsAccumulator);
                }
                // 立即释放当前父节点 parent 的锁（
                parent.releaseLatch();
                parent = child;
                child = null;
            // 只要当前的 parent 还不是底层的叶子节点（BIN），就持续向下钻取。
            } while (!parent.isBIN());

            success = true;
            return (BIN)parent;

        } finally {
            if (!success) {

                /*
                 * In [#14903] we encountered a latch exception below and the
                 * original exception was lost.  Print the stack trace and
                 * allow the original exception to be thrown if this happens
                 * again, to get more information about the problem.
                 */
                try {
                    if (child != null && child.isLatchOwner()) {
                        child.releaseLatch();
                    }

                    if (parent != child && parent.isLatchOwner()) {
                        parent.releaseLatch();
                    }
                } catch (Exception e) {
                    LoggerUtils.traceAndLogException(
                        database.getEnv(), "Tree", "searchSubTreeInternal", "",
                        e);
                }
            }
        }
    }

    /*
     * Search for the given key in the subtree rooted at the given parent IN.
     * The search descends until the given target level, and the IN that
     * contains or covers the key is returned latched in EX or SH mode as
     * specified by the latchShared param.
     *
     * The method uses grandparent latching, but only if the parent is the
     * root of the whole Btree and it is SH-latched on entry.  
     */
    private IN searchSubTree(
        IN parent,
        byte[] key,
        SearchType searchType,
        int targetLevel,
        boolean latchShared,
        CacheMode cacheMode,
        Comparator<byte[]> comparator) {

        /*
         * If a an intermediate IN (e.g., from getNextIN) was
         * originally passed, it was latched exclusively.
         */
        assert(parent != null &&
               (parent.isRoot() ||
                parent.isLatchExclusiveOwner()));

        if ((searchType == SearchType.LEFT ||
             searchType == SearchType.RIGHT) &&
            key != null) {

            /*
             * If caller is asking for a right or left search, they shouldn't
             * be passing us a key.
             */
            throw EnvironmentFailureException.unexpectedState(
                "searchSubTree passed key and left/right search");
        }

        assert(parent.isUpperIN());
        assert(parent.isLatchOwner());

        boolean success = false;
        int index;
        IN subtreeRoot = parent;
        IN child = null;
        IN grandParent = null;
        boolean childIsLatched = false;
        boolean grandParentIsLatched = false;
        boolean doGrandparentLatching = !parent.isLatchExclusiveOwner();

        TreeWalkerStatsAccumulator treeStatsAccumulator =
            getTreeStatsAccumulator();

        try {
            do {
                if (treeStatsAccumulator != null) {
                    parent.accumulateStats(treeStatsAccumulator);
                }

                assert(parent.getNEntries() > 0);

                if (searchType == SearchType.NORMAL) {
                    /* Look for the entry matching key in the current node. */
                    index = parent.findEntry(key, false, false, comparator);
                } else if (searchType == SearchType.LEFT) {
                    /* Left search, always take the 0th entry. */
                    index = 0;
                } else if (searchType == SearchType.RIGHT) {
                    /* Right search, always take the highest entry. */
                    index = parent.getNEntries() - 1;
                } else {
                    throw EnvironmentFailureException.unexpectedState(
                        "Invalid value of searchType: " + searchType);
                }

                assert(index >= 0);

                /*
                 * Get the child IN.
                 *
                 * If the child is not cached and we are usimg grandparent
                 * latching, then:
                 *
                 * (a) If "parent" is not the subtree root, is is always
                 * SH-latched at this point. So, to fetch the child, we need to
                 * unlatch the parent and relatch it exclusively. Because we
                 * have the grandparent latch (in either SH or EX mode), the
                 * parent will not be evicted or detached from the tree and the
                 * index of the child within the parent won't change. After
                 * the parent is EX-latched, we can release the grandparent so
                 *. it won't be held while reading the child from the log.
                 *
                 * (b) If "parent" is the BTree root, it may be SH-latched. In
                 * this case, since there is no grandparent, we must unlatch
                 * the parent and relatch it in EX mode under the protection 
                 * of the rootLatch; then we restart the do-loop.
                 *
                 * (c) If "parent" is the subtree root, but not the root of
                 * the full Btree, then it must be EX-latched already, and 
                 * we can just fetch the child. 
                 */
                child = (IN) parent.getTarget(index);

                if (child == null && doGrandparentLatching) {

                    if (parent != subtreeRoot) {

                        assert(!parent.isLatchExclusiveOwner());
                        parent.releaseLatch();
                        parent.latch(cacheMode);
                        grandParent.releaseLatch();
                        grandParentIsLatched = false;
                        grandParent = null;
                        doGrandparentLatching = false;

                    } else if (parent.isRoot() &&
                               !parent.isLatchExclusiveOwner()) {

                        parent.releaseLatch();
                        subtreeRoot = getRootINLatchedExclusive(cacheMode);
                        parent = subtreeRoot;
                        assert(parent != null);
                        assert(grandParent == null);
                        doGrandparentLatching = false;

                        continue;
                    }

                    child = parent.fetchIN(index, cacheMode);

                } else if (child == null) {

                    child = parent.fetchIN(index, CacheMode.UNCHANGED);
                }

                /* After fetching the child we can release the grandparent. */
                if (grandParent != null) {
                    grandParent.releaseLatch();
                    grandParentIsLatched = false;
                }

                /* Latch the child. Note: BINs are always latched exclusive. */
                if (child.getLevel() == targetLevel) {
                    if (latchShared) {
                        child.latchShared(cacheMode);
                    } else {
                        child.latch(cacheMode);
                    }
                }
                else if (doGrandparentLatching) {
                } else {
                    latchChild(parent, child, cacheMode);
                }
                childIsLatched = true;

                child.mutateToFullBIN(false /*leaveFreeSlot*/);

                if (treeStatsAccumulator != null) {
                    child.accumulateStats(treeStatsAccumulator);
                }

                /* Continue down a level */
                if (doGrandparentLatching) {
                    grandParent = parent;
                    grandParentIsLatched = true;
                } else {
                    parent.releaseLatch();
                }

                parent = child;

            } while (!parent.isBIN() && parent.getLevel() != targetLevel);

            success = true;
            return child;

        } finally {
            if (!success) {

                /*
                 * In [#14903] we encountered a latch exception below and the
                 * original exception was lost.  Print the stack trace and
                 * allow the original exception to be thrown if this happens
                 * again, to get more information about the problem.
                 */
                try {
                    if (child != null && childIsLatched) {
                        child.releaseLatch();
                    }

                    if (parent != child) {
                        parent.releaseLatch();
                    }
                } catch (Exception e) {
                    LoggerUtils.traceAndLogException(
                        database.getEnv(), "Tree", "searchSubTreeInternal", "",
                        e);
                }
            }

            if (grandParent != null && grandParentIsLatched) {
                grandParent.releaseLatch();
            }
        }
    }

    /**
     * rebuildINList is used by recovery to add all the resident nodes to the
     * IN list.
     */
    public void rebuildINList()
        throws DatabaseException {

        INList inMemoryList = database.getEnv().getInMemoryINs();

        if (root != null) {
            rootLatch.acquireShared();
            try {
                Node rootIN = root.getTarget();
                if (rootIN != null) {
                    rootIN.rebuildINList(inMemoryList);
                }
            } finally {
                rootLatch.release();
            }
        }
    }

    /**
     * Debugging check that all resident nodes are on the INList and no stray
     * nodes are present in the unused portion of the IN arrays.
     */
    public void validateINList(IN parent)
        throws DatabaseException {

        if (parent == null) {
            parent = (IN) root.getTarget();
        }

        if (parent != null) {
            INList inList = database.getEnv().getInMemoryINs();

            if (!inList.contains(parent)) {
                throw EnvironmentFailureException.unexpectedState(
                    "IN " + parent.getNodeId() + " missing from INList");
            }

            for (int i = 0;; i += 1) {
                try {
                    Node node = parent.getTarget(i);

                    if (i >= parent.getNEntries()) {
                        if (node != null) {
                            throw EnvironmentFailureException.unexpectedState(
                                "IN " + parent.getNodeId() +
                                " has stray node " + node +
                                " at index " + i);
                        }
                        byte[] key = parent.getKey(i);
                        if (key != null) {
                            throw EnvironmentFailureException.unexpectedState(
                               "IN " + parent.getNodeId() +
                               " has stray key " + key +
                               " at index " + i);
                        }
                    }

                    if (node instanceof IN) {
                        validateINList((IN) node);
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    break;
                }
            }
        }
    }

    /*
     * Logging support
     */

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        int size = 1;                          // rootExists
        if (root != null) {
            size += root.getLogSize();
        }
        return size;
    }

    /**
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        byte booleans = (byte) ((root != null) ? 1 : 0);
        logBuffer.put(booleans);
        if (root != null) {
            root.writeToLog(logBuffer);
        }
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        boolean rootExists = false;
        byte booleans = itemBuffer.get();
        rootExists = (booleans & 1) != 0;
        if (rootExists) {
            root = makeRootChildReference();
            root.readFromLog(itemBuffer, entryVersion);
        }
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<root>");
        if (root != null) {
            root.dumpLog(sb, verbose);
        }
        sb.append("</root>");
    }

    /**
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    /**
     * @return the TreeStats for this tree.
     */
    int getTreeStats() {
        return rootSplits.get();
    }

    private TreeWalkerStatsAccumulator getTreeStatsAccumulator() {
        if (EnvironmentImpl.getThreadLocalReferenceCount() > 0) {
            return treeStatsAccumulatorTL.get();
        } else {
            return null;
        }
    }

    public void setTreeStatsAccumulator(TreeWalkerStatsAccumulator tSA) {
        treeStatsAccumulatorTL.set(tSA);
    }

    public void loadStats(StatsConfig config, BtreeStats btreeStats) {
        /* Add the tree statistics to BtreeStats. */
        btreeStats.setTreeStats(stats.cloneGroup(false));

        if (config.getClear()) {
            relatchesRequired.clear();
            rootSplits.clear();
        }
    }

    /*
     * Debugging stuff.
     */
    public void dump() {
        System.out.println(dumpString(0));
    }

    public String dumpString(int nSpaces) {
        StringBuilder sb = new StringBuilder();
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<tree>");
        sb.append('\n');
        if (root != null) {
            sb.append(DbLsn.dumpString(root.getLsn(), nSpaces));
            sb.append('\n');
            IN rootIN = (IN) root.getTarget();
            if (rootIN == null) {
                sb.append("<in/>");
            } else {
                sb.append(rootIN.toString());
            }
            sb.append('\n');
        }
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("</tree>");
        return sb.toString();
    }

    /**
     * Unit test support to validate subtree pruning. Didn't want to make root
     * access public.
     */
    boolean validateDelete(int index)
        throws DatabaseException {

        rootLatch.acquireShared();
        try {
            IN rootIN = (IN) root.fetchTarget(database, null);
            rootIN.latch();
            try {
                return rootIN.validateSubtreeBeforeDelete(index);
            } finally {
                rootIN.releaseLatch();
            }
        } finally {
            rootLatch.release();
        }
    }

    /* For unit testing only. */
    public void setWaitHook(TestHook hook) {
        waitHook = hook;
    }

    /* For unit testing only. */
    public void setSearchHook(TestHook hook) {
        searchHook = hook;
    }

    /* For unit testing only. */
    public void setCkptHook(TestHook hook) {
        ckptHook = hook;
    }

    /* For unit testing only. */
    public void setGetParentINHook(TestHook hook) {
        getParentINHook = hook;
    }

    public void setFetchINHook(TestHook hook) {
        fetchINHook = hook;
    }
    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceSplitRoot(Level level,
                                String splitType,
                                IN newRoot,
                                long newRootLsn,
                                IN oldRoot,
                                long oldRootLsn) {
        Logger logger = database.getEnv().getLogger();
        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(splitType);
            sb.append(" newRoot=").append(newRoot.getNodeId());
            sb.append(" newRootLsn=").
                append(DbLsn.getNoFormatString(newRootLsn));
            sb.append(" oldRoot=").append(oldRoot.getNodeId());
            sb.append(" oldRootLsn=").
                append(DbLsn.getNoFormatString(oldRootLsn));
            LoggerUtils.logMsg(
                logger, database.getEnv(), level, sb.toString());
        }
    }

    private static class SplitInfo {
        IN parent;
        IN child;
        int index;

        SplitInfo(IN parent, IN child, int index) {
            this.parent = parent;
            this.child = child;
            this.index = index;
        }
    }
}
