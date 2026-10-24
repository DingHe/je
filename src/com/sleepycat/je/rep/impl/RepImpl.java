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

package com.sleepycat.je.rep.impl;

import static com.sleepycat.je.rep.NoConsistencyRequiredPolicy.NO_CONSISTENCY;
import static com.sleepycat.je.rep.impl.RepParams.NODE_NAME;
import static com.sleepycat.je.rep.impl.RepParams.REPLAY_FREE_DISK_PERCENT;
import static com.sleepycat.je.rep.impl.RepParams.TEST_JE_VERSION;
import static com.sleepycat.je.rep.impl.RepParams.VLSN_MAX_DIST;
import static com.sleepycat.je.rep.impl.RepParams.VLSN_MAX_MAP;
import static com.sleepycat.je.rep.impl.RepParams.VLSN_STRIDE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Formatter;
import java.util.logging.Level;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.TransactionTimeoutException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.RepConfigProxy;
import com.sleepycat.je.dbi.StartupTracker.Phase;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.recovery.RecoveryInfo;
import com.sleepycat.je.recovery.VLSNRecoveryProxy;
import com.sleepycat.je.rep.DatabasePreemptedException;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.LockPreemptedException;
import com.sleepycat.je.rep.LogFileRewriteListener;
import com.sleepycat.je.rep.LogOverwriteException;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.RepStatManager;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.RestartRequiredException;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.SyncupProgress;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.impl.node.MasterTransfer;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.NodeState;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.impl.node.Replay;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.stream.ArbiterFeederSource;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.FeederReader;
import com.sleepycat.je.rep.stream.FeederTxns;
import com.sleepycat.je.rep.subscription.StreamAuthenticator;
import com.sleepycat.je.rep.txn.MasterThreadLocker;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.txn.ReadonlyTxn;
import com.sleepycat.je.rep.txn.ReplayTxn;
import com.sleepycat.je.rep.txn.ReplicaThreadLocker;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.rep.utilint.StatCaptureRepDefinitions;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.rep.vlsn.VLSNRecoveryTracker;
import com.sleepycat.je.statcap.StatManager;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.ThreadLocker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnEnd;
import com.sleepycat.je.txn.VersionedWriteTxnEnd;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.utilint.BooleanStat;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StringStat;
import com.sleepycat.je.utilint.VLSN;
// RepImpl 是 Berkeley DB JE 高可用复制模块（High Availability, HA）的核心引擎实现类。它直接继承自单机版核心引擎类 EnvironmentImpl，并实现了 RepEnvConfigObserver 接口。
// 高可用环境的控制中心：作为单机 EnvironmentImpl 在复制集群环境下的扩展与超集，它统筹管理整个复制节点（ReplicatedEnvironment）的所有底层服务、状态机与网络通信。
// 复制状态与同步管理者：负责维护集群中节点的身份（Master/Replica/Arbiter 等）、管理复制组元数据、协调复制日志重放（Replay）、追踪全局虚拟 LSN（VLSNIndex）以及处理节点间的心跳与选举。
// 支持动态热更新：通过实现 RepEnvConfigObserver 接口，能够实时监听并动态调整高可用复制相关的可变配置属性。
public class RepImpl
    extends EnvironmentImpl
    implements RepEnvConfigObserver {
    // VLSN 索引实例。VLSN（Replication Virtual LSN）是 Berkeley DB JE 高可用复制模块的核心概念。它为复制流中的每一条日志记录分配一个单调递增的虚拟序列号（VLSN），
    // 并维护 VLSN 到物理 LSN（Log Sequence Number）的双向映射。该索引是副本节点同步、日志重放和一致性检查的基础。
    private VLSNIndex vlsnIndex;
    /* VLSNIndexAccess coordinates the closing of the vlsn index */
    // VLSN 索引关闭协调器。正如注释所述，它专门用于协调和控制在数据库环境关闭（特别是检查点前后）时如何安全、有序地关闭和持久化 vlsnIndex 占用的资源。
    private final VLSNIndexAccess vlsnIndexAccess = new VLSNIndexAccess();
    // Feeder 事务管理器。当当前节点作为主节点（Master）通过 Feeder 线程向其他副本（Replica）或订阅者传输日志流时，该组件负责管理和追踪与传输流相关的活跃事务上下文。
    private final FeederTxns feederTxns;

    /*
     * The repNode is only non-null when the replicated environment has joined
     * a group. It's null otherwise.
     */
    // 复制节点核心控制实例。
    // 仅当当前高可用复制环境已经成功加入一个复制组（Group）时才不为 null；在未加入或断开连接状态下均为 null。
    // 它封装了网络通信、心跳、选举和主从角色逻辑。被声明为 volatile 是为了确保多线程环境下对节点状态变更的可见性。
    private volatile RepNode repNode;
    // 日志重放引擎（Replay）。它是副本节点（Replica）的核心组件。
    // 负责接收来自主节点的复制日志条目，并在本地数据库中进行解析、应用和重放，以保持副本数据与主节点强一致或最终一致。
    private Replay replay;

    /*
     * This is the canonical nameIdPair instance used by the node. The internal
     * Id part of the pair will be updated when the node actually joins the
     * group.
     */
    // 节点的规范化名称与 ID 对（Name-ID Pair）。
    // 点使用的规范化标识实例。它包含节点的字符串名称（Name）和内部数字 ID（ID）。
    // 其中内部 ID 部分会在节点真正加入复制组、从集群分配到有效 ID 后被动态更新。
    private NameIdPair nameIdPair;
    // 节点状态机管理器。
    // 负责实时追踪、记录和维护当前复制环境所处的高可用状态（例如 MASTER、REPLICA、DETACHED、UNKNOWN 等），并对外提供状态查询和状态变更事件通知支持。
    private final NodeState nodeState;

    /*
     * The clockskew used by this environment in ms. It's only used by testing
     * to inject clock skew between ReplicatedEnvironments.
     */
    // 时钟偏移量（毫秒）
    // 仅用于测试环境
    private static int clockSkewMs = 0;

    /*
     * A handle to the group database. This handle is initialized lazily when
     * the contents of the database are first required. It's set to null upon
     * shutdown. The handle must be initialized lazily because the database is
     * created by the master, and we only know master identity later.  The
     * RepImpl manages the rep group database, so that the lifetime of the
     * databaseImpl handle can be managed more easily to mesh with the opening
     * and closing of the RepImpl.
     */
    // 复制组元数据数据库（Group Database）的内部实现句柄。
    // 只有在首次需要访问复制组成员信息时才会被创建和打开，并在环境关闭时被置为 null。
    // 采用延迟初始化的原因是：复制组数据库是由集群的主节点（Master）创建的，而新节点在启动初期并不能立刻知晓谁是主节点
    private DatabaseImpl groupDbImpl = null;

    /* The status presents whether this replica is doing rollback. */
    // 备份禁止标志（回滚状态保护）
    // 指示当前副本节点是否正在执行日志回滚（Rollback）操作。在回滚期间，为了防止数据处于不一致状态或引发冲突，通常会禁止外部发起数据库备份（Backup）。
    private boolean backupProhibited = false;

    /*
     * Represents whether this Environment is allowed to convert a
     * non-replicated Environment to replicated.
     */
    // 允许单机转复制环境标志。
    private boolean allowConvert = false;

    /** Config params for preserving and caching the VLSN. */
    // 保留 VLSN 配置标志。
    // 指示系统是否需要为每条记录持久化并保留其虚拟 LSN（VLSN）版本信息（通常用于高级数据一致性追踪或时间旅行查询）。
    private boolean preserveVLSN;
    // 缓存 VLSN 配置标志。
    private boolean cacheVLSN;

    /*
     * True if TTL is available. Volatile is not used, since checking more than
     * once is idempotent.
     */
    // 指示当前环境是否支持并启用了 TTL 特性。正如注释所述，由于多次检查结果是幂等的，因此这里没有使用 volatile 修饰。
    private boolean isTTLAvailable = false;

    /* Keep an eye on the ongoing DbBackups. */
    // 活跃数据库备份集合。
    private final Set<DbBackup> backups = new HashSet<DbBackup>();

    /*
     * The list of observers who are notified when a mutable rep param changes.
     */
    // 复制配置变更观察者列表。
    private final List<RepEnvConfigObserver> repConfigObservers;

    /*
     * Lock used to control access and lazy initialization of groupDbImpl,
     * ensuring that there is exactly one database made. A mutex is used rather
     * than synchronization to allow us to probe for contention on the
     * groupDbImpl.
     */
    // 组数据库延迟初始化与访问互斥锁。
    // 用于控制对 groupDbImpl（复制组元数据数据库）的并发访问和安全初始化，确保仅创建一个实例。
    private final ReentrantLock groupDbLock = new ReentrantLock();
    // 副本确认超时时间。控制主节点等待副本对写入日志进行 ACK 确认的最大等待时间。
    private int replicaAckTimeout;
    // 仲裁器确认超时时间。控制主节点等待仲裁器响应 ACK 的超时限制。
    private int arbiterAckTimeout;
    // 副本不足超时时间。当集群中存活的副本数量不满足法定人数（Quorum）要求时，写操作在超时后会抛出异常。
    private int insufficientReplicasTimeout;
    // 重放事务锁超时时间。副本节点在重放主节点日志事务时所采用的锁超时阈值。
    private int replayTxnTimeout;
    // 默认副本一致性策略。定义了副本在执行读操作时默认要求的一致性约束。
    private ReplicaConsistencyPolicy defaultConsistencyPolicy;
    // 是否允许仲裁器参与写确认的布尔标志。
    private boolean allowArbiterAck;

    /*
     * Arbiters, subscribers and networkBackup use RepImpls which are read
     * only and have some daemon functionality disabled.
     */
    // 指示当前节点是否为仲裁器（Arbiter）角色。
    private boolean isArbiter;
    // 指示当前节点是否为订阅者（Subscriber）角色。
    private boolean isSubscriber;
    // 指示当前节点是否为网络备份（Network Backup）角色。
    private boolean isNetworkBackup;

    /*
     * NodeStats are currently not public, but we may want to evaluate
     * and decide if they would be useful, perhaps as a debugging aid.
     */
    // 复制节点专有的统计指标聚合组。
    private final StatGroup nodeStats;
    // 指示当前节点是否经历了硬恢复（Hard Recovery）的布尔统计项。
    private final BooleanStat hardRecoveryStat;
    private final StringStat hardRecoveryInfoStat;

    /*
     * Used to block transaction commit/abort execution just before completing
     * a Master Transfer operation.
     */
    // 事务阻塞门栓（CountDownLatch）。
    // 当主节点转移完成第一阶段后，会通过设置一个新的门栓来临时阻塞前台事务在提交或中止阶段的完成，以便安全锁定终极 VLSN。
    private volatile CountDownLatch blockTxnLatch = new CountDownLatch(0);

    /**
     * A lock used to coordinate access to {@link #blockTxnLatch}.
     * <p>
     * When a Master Transfer operation completes Phase 1, it sets a new {@code
     * CountDownLatch} in order to block the completion of transactions at the
     * commit or abort stage.  We must avoid having it do so at an awkward
     * moment.  There are two (unrelated) cases:
     * <ol>
     * <li>There is a brief period between the time a transaction "awaits" the
     * latch (in {@code checkBlock()}) and the time it publishes its VLSN.  We
     * must avoid having Master Transfer read its "ultimate goal" VLSN during
     * that interval.
     * <li>The Feeder input thread occasionally updates the GroupDB, upon
     * receiving a Heartbeat response.  That happens in a transaction, like any
     * other, so it could be subject to the normal blockage in Phase 2.  But
     * the Feeder input thread is of course also the thread that we rely on for
     * making progress towards the goal of Master Transfer; so blocking it is
     * counterproductive.
     * </ol>
     *
     * @see MasterTransfer
     * @see ReplicatedEnvironment#transferMaster
     */
    // 由于存在“事务等待门栓与发布 VLSN 之间的时间差”以及“Feeder 输入线程需要通过事务更新 GroupDB（不能被死板阻塞）”这两种特殊情况，
    // 必须通过该公平读写锁来精细化协调对 blockTxnLatch 的访问，避免死锁或阻碍主节点转移的进展。
    private final ReentrantReadWriteLock blockLatchLock =
        new ReentrantReadWriteLock(true);

    /* application listener for syncups. */
    // 同步阶段进度监听器。允许应用程序监听节点间进行日志同步（Syncup）时的进度。
    private final ProgressListener<SyncupProgress> syncupProgressListener;

    /* Application callback to be notified before we overwrite log files. */
    // 日志文件重写/覆盖前监听器。在底层的日志文件被覆盖或重写前触发应用回调。
    private final LogFileRewriteListener logRewriteListener;

    /* Configuration for ServiceDispatcher communication */
    // 服务分发器（ServiceDispatcher）通信网络配置。
    private final ReplicationNetworkConfig repNetConfig;

    /*
     * Factory for creating channel instances.  Not available until
     * initializeChannelFactory is called.
     */
    // 网络通信通道工厂。
    // 负责创建节点间传输数据的底层 Socket 通道，在调用 initializeChannelFactory() 之前为 null。
    private volatile DataChannelFactory channelFactory;

    /**
     * Used for testing, to create log files with
     * VLSN.UNINITIALIZED_VLSN_SEQUENCE as the value for the dtvlsn
     */
    // 模拟旧版本 DTVLSN 主节点的测试开关。仅用于测试，允许创建将 DTVLSN 设为未初始化序列的日志文件。
    private static boolean simulatePreDTVLSNMaster = false;

    /*
     * Used to verify VLSN invariants as they are written to the log on the
     * master
     */
    // 主节点 VLSN 写入不变式校验历史值。
    // 用于在主节点将日志写入磁盘时，实时校验 VLSN 和 DTVLSN 是否严格单调递增，确保不变式不被打破。
    private long prevLoggedVLSN = VLSN.NULL_VLSN_SEQUENCE;
    private long prevLoggedDTVLSN = VLSN.NULL_VLSN_SEQUENCE;

    /*
     * The filter transmitted to a Feeder so that records can be filtered at
     * the source
     */
    // Feeder 源头日志过滤器。用于在主节点向副本传输日志流时，在源头直接对记录进行过滤。
    private final FeederFilter feederFilter;

    /**
     * The feeder authenticator generator is instantiated by the replicated
     * environment creator.
     */
    // 复制流认证器（Stream Authenticator）。
    // 由复制环境的创建者实例化并注入，用于在副本连接主节点或订阅数据时进行身份验证与安全校验。
    private volatile StreamAuthenticator authenticator = null;

    public RepImpl(File envHome,
                   EnvironmentConfig envConfig,
                   EnvironmentImpl sharedCacheEnv,
                   RepConfigProxy repConfigProxy)
        throws EnvironmentNotFoundException, EnvironmentLockedException {

        super(envHome, envConfig, sharedCacheEnv, repConfigProxy);

        allowConvert =
            RepInternal.getAllowConvert(((ReplicationConfig) repConfigProxy));

        repConfigObservers = new ArrayList<RepEnvConfigObserver>();
        addRepConfigObserver(this);

        repNetConfig =
                ((ReplicationConfig)repConfigProxy).getRepNetConfig();
        nodeState = new NodeState(nameIdPair, this);

        if (isArbiter || isSubscriber || isNetworkBackup ) {
            nodeStats = null;
            syncupProgressListener = null;
            logRewriteListener = null;
            hardRecoveryStat = null;
            hardRecoveryInfoStat = null;
            feederTxns = null;
            feederFilter = null;
            return;
        }

        feederTxns = new FeederTxns(this);
        replay = new Replay(this, nameIdPair);

        nodeStats = new StatGroup(RepImplStatDefinition.GROUP_NAME,
                                  RepImplStatDefinition.GROUP_DESC);
        hardRecoveryStat = new BooleanStat(nodeStats,
                                           RepImplStatDefinition.HARD_RECOVERY);
        hardRecoveryInfoStat =
            new StringStat(nodeStats, RepImplStatDefinition.HARD_RECOVERY_INFO,
                           "This node did not incur a hard recovery.");

        syncupProgressListener =
            ((ReplicationConfig)repConfigProxy).getSyncupProgressListener();
        logRewriteListener =
            ((ReplicationConfig)repConfigProxy).getLogFileRewriteListener();

        feederFilter =
            ((ReplicationConfig)repConfigProxy).getFeederFilter();

        authenticator = ((ReplicationConfig)repConfigProxy).getAuthenticator();
    }

    /**
     * Called by the EnvironmentImpl constructor.  Some rep params,
     * preserveVLSN for example, are accessed by the EnvironmentImpl via
     * methods (getPreserveVLSN for example), so they need to be initialized
     * early.
     */
    @Override
    protected void initConfigParams(EnvironmentConfig envConfig,
                                    RepConfigProxy repConfigProxy) {

        /* Init standalone config params first. */
        super.initConfigParams(envConfig, repConfigProxy);

        /* Init rep config params. */
        replicaAckTimeout =
            configManager.getDuration(RepParams.REPLICA_ACK_TIMEOUT);
        insufficientReplicasTimeout =
            configManager.getDuration(RepParams.INSUFFICIENT_REPLICAS_TIMEOUT);
        replayTxnTimeout =
            configManager.getDuration(RepParams.REPLAY_TXN_LOCK_TIMEOUT);
        defaultConsistencyPolicy = RepUtils.getReplicaConsistencyPolicy
            (configManager.get(RepParams.CONSISTENCY_POLICY));
        preserveVLSN =
            configManager.getBoolean(RepParams.PRESERVE_RECORD_VERSION);
        cacheVLSN =
            configManager.getBoolean(RepParams.CACHE_RECORD_VERSION);
        allowArbiterAck =
            configManager.getBoolean(RepParams.ALLOW_ARBITER_ACK);
        isArbiter =
            configManager.getBoolean(RepParams.ARBITER_USE);
        isSubscriber =
            configManager.getBoolean(RepParams.SUBSCRIBER_USE);
        isNetworkBackup =
            configManager.getBoolean(RepParams.NETWORKBACKUP_USE);
        arbiterAckTimeout =
            configManager.getDuration(RepParams.ARBITER_ACK_TIMEOUT);
    }

    @Override
    protected Formatter initFormatter() {

        /*
         * The nameIdPair field is assigned here rather than in the constructor
         * because of base class/subclass dependencies. initFormatter() is
         * called by the base class constructor, and nameIdPair must be
         * available at that time.
         */
        nameIdPair = new NameIdPair(configManager.get(NODE_NAME));
        return new ReplicationFormatter(nameIdPair);
    }

    @Override
    public String getMonitorClassName() {
        return "com.sleepycat.je.rep.jmx.RepJEMonitor";
    }

    @Override
    public String getDiagnosticsClassName() {
        return "com.sleepycat.je.rep.jmx.RepJEDiagnostics";
    }

    /**
     * @see super#initConfigManager
     */
    @Override
    protected DbConfigManager
        initConfigManager(EnvironmentConfig envConfig,
                          RepConfigProxy repConfigProxy) {
        return new RepConfigManager(envConfig, repConfigProxy);
    }

    @Override
    public boolean getAllowRepConvert() {
        return allowConvert;
    }

    /**
     * @see super#resetConfigManager
     */
    @Override
    protected DbConfigManager resetConfigManager(EnvironmentConfig newConfig) {
        /* Save all the replication related properties. */
        RepConfigManager repConfigManager = (RepConfigManager) configManager;
        ReplicationConfig repConfig = repConfigManager.makeReplicationConfig();
        return new RepConfigManager(newConfig, repConfig);
    }

    public ReplicationConfig cloneRepConfig() {
        RepConfigManager repConfigManager = (RepConfigManager) configManager;
        return repConfigManager.makeReplicationConfig();
    }

    /* Make an ReplicatedEnvironment handle for this RepImpl. */
    public ReplicatedEnvironment makeEnvironment() {
        return new ReplicatedEnvironment(getEnvironmentHome(),
                                         cloneRepConfig(),
                                         cloneConfig());
    }

    public ReplicationMutableConfig cloneRepMutableConfig() {
        RepConfigManager repConfigManager = (RepConfigManager) configManager;
        return repConfigManager.makeReplicationConfig();
    }

    public void setRepMutableConfig(ReplicationMutableConfig config)
        throws DatabaseException {

        /* Clone the current config. */
        RepConfigManager repConfigManager = (RepConfigManager) configManager;
        ReplicationConfig newConfig = repConfigManager.makeReplicationConfig();

        /* Copy in the mutable props. */
        config.copyMutablePropsTo(newConfig);
        repConfigManager = new RepConfigManager
            (configManager.getEnvironmentConfig(), newConfig);

        /*
         * Update the current config and notify observers.  The config manager
         * is replaced with a new instance that uses the new configuration.
         * This avoids synchronization issues: other threads that have a
         * reference to the old configuration object are not impacted.
         *
         * Notify listeners in reverse order of registration so that the
         * environment listener is notified last and can start daemon threads
         * after they are configured.
         */
        for (int i = repConfigObservers.size() - 1; i >= 0; i -= 1) {
            RepEnvConfigObserver o = repConfigObservers.get(i);
            o.repEnvConfigUpdate(repConfigManager, newConfig);
        }
    }

    @Override
    public void repEnvConfigUpdate(RepConfigManager configMgr,
                                   ReplicationMutableConfig newConfig)
        throws DatabaseException {

        allowArbiterAck =
            configMgr.getBoolean(RepParams.ALLOW_ARBITER_ACK);

        if (repNode == null) {
            return;
        }

        repNode.getArbiter().processConfigChange(newConfig);

        repNode.getElectionQuorum().setElectableGroupSizeOverride
            (newConfig.getElectableGroupSizeOverride());

        /* Account for mutation of deprecated HA LogFlusher params. */
        getLogFlusher().configFlushTask(configMgr);

        repNode.getReplica().getDbCache().setConfig(configMgr);
    }

    public synchronized void addRepConfigObserver(RepEnvConfigObserver o) {
        repConfigObservers.add(o);
    }

    /**
     * The VLSNIndex must be created, merged and flushed before the recovery
     * checkpoint. This method should be called even if there is no recovery
     * checkpoint, because it sets up needed data structures.
     *
     * On the face of it, it seems that one could flush the VLSNIndex cache
     * after the recovery checkpoint, before the Replicator constructor returns
     * and before any user level HA operations can start. That's not sufficient
     * because the recovery checkpoint is shortening the recovery interval for
     * future recoveries, and any information that has been garnered must be
     * persisted. Here's an example of what might happen after a series of
     * recoveries if we fail to flush VLSNIndex as part of the recovery
     * checkpoint:
     *
     * Environment recovers for first time, brand new environment
     *    recovery did not find any VLSNs in log, because log is brand new
     *    recovery logs ckpt 1start
     *    recovery logs ckpt 1 end
     *
     *    VLSN 1 logged
     *    VLSN 2 logged
     *    VLSN 3 logged
     *
     *  crash .... Environment recovers
     *  recovery crawls log from ckpt 1 start onward, finds VLSNs 1-3
     *  recovery logs ckpt 2 start
     *  recovery logs ckpt 2 end
     *  VLSN index instantiated, VLSNs 1-3 added in but not written too disk
     *
     *  crash ... Environment recovers
     *  recovery crawls log from ckpt start 2 start onward, finds no VLSNs.
     *
     * Instead, the flushed VLSN has to be logged before the checkpoint end
     * record that is used for the next recovery.
     */
    @Override
    public void preRecoveryCheckpointInit(RecoveryInfo recoveryInfo) {

        int stride = configManager.getInt(VLSN_STRIDE);
        int maxMappings = configManager.getInt(VLSN_MAX_MAP);
        int maxDist = configManager.getInt(VLSN_MAX_DIST);

        /*
         * Our local nameIdPair field isn't set yet because we haven't finished
         * our initialization, so get it from the config manager.
         */
        NameIdPair useNameIdPair =
            new NameIdPair(configManager.get(NODE_NAME));

        vlsnIndex = new VLSNIndex(this, DbType.VLSN_MAP.getInternalName(),
                                  useNameIdPair, stride, maxMappings, maxDist,
                                  recoveryInfo);
        replay.preRecoveryCheckpointInit(recoveryInfo);
    }

    /**
     * Returns the current state associated with this ReplicatedEnvironment
     *
     * @return the externally visible ReplicatedEnvironment state
     */
    public ReplicatedEnvironment.State getState() {
        return nodeState.getRepEnvState();
    }

    /**
     * Returns the state change event that transitioned the
     * ReplicatedEnviroment to its current state.
     */
    public StateChangeEvent getStateChangeEvent() {
        return nodeState.getStateChangeEvent();
    }

    public NodeState getNodeState() {
        return nodeState;
    }

    /**
     * Wait for this node to join a replication group and return whether it is
     * a MASTER or REPLICA. Note that any method that creates or clears the
     * repNode field must be synchronized.
     */
    public synchronized ReplicatedEnvironment.State
        joinGroup(ReplicaConsistencyPolicy consistency,
                  QuorumPolicy initialElectionPolicy)
        throws ReplicaConsistencyException, DatabaseException {

        startupTracker.start(Phase.TOTAL_JOIN_GROUP);
        try {
            if (repNode == null) {
                repNode = new RepNode(this, replay, nodeState);
            }

            return repNode.joinGroup(consistency, initialElectionPolicy);
        } catch (IOException ioe) {
            throw EnvironmentFailureException.unexpectedException
                (this, "Problem attempting to join on " + getSocket(), ioe);
        } finally {
            startupTracker.stop(Phase.TOTAL_JOIN_GROUP);
        }
    }

    /**
     * Initialize the DataChannelFactory in our configuration for use.
     * This is public to allow access by the ReplicatedEnvironment constructor.
     * @throws IllegalArgumentException if the ReplicationNetworkConfig
     * is invalid.
     */
    public void initializeChannelFactory() {
        if (channelFactory != null) {
            return;
        }

        synchronized (this) {
            if (channelFactory == null) {
                channelFactory =
                    DataChannelFactoryBuilder.construct(
                        repNetConfig,
                        DataChannelFactoryBuilder.makeLoggerFactory(this));
            }
        }
    }

    @Override
    protected Environment createInternalEnvironment() {
        return new InternalReplicatedEnvironment
            (getEnvironmentHome(), cloneRepConfig(), cloneConfig(), this);
    }

    /**
     * @see EnvironmentImpl#setupClose
     * Release all replication resources that can be released before the
     * checkpoint. Note that any method that creates or clears the repNode
     * field must be called from a synchronized caller.
     *
     * Note that the vlsnIndex is closed as a callback, from
     * postCheckpointPreEnvClose()
     * @throws DatabaseException
     *
     */
    @Override
    protected synchronized void setupClose(PrintWriter errors)
        throws DatabaseException {

        if (groupDbImpl != null) {
            getDbTree().releaseDb(groupDbImpl);
            groupDbImpl = null;
            LoggerUtils.fine
                (envLogger, this, "Group member database shutdown");
        }

        try {
            if (repNode != null) {
                repNode.shutdown();
                repNode = null;
            }
        } catch (InterruptedException e) {
            appendException(errors, e, "shutting down node " + nameIdPair);
        }
    }

    /**
     * Close any resources that need to be closed after the closing checkpoint.
     * Note that since Replay.close closes open transactions, it must be
     * invoked after the checkpoint has been completed, so that the checkpoint
     * operation can correctly account for the open transactions.
     */
    @Override
    protected synchronized void postCheckpointClose(boolean checkpointed)
        throws DatabaseException {

        if (replay != null) {
            replay.close();
            replay = null;
        }

        vlsnIndexAccess.closeVLSNIndex(checkpointed);
    }

    /**
     * @see EnvironmentImpl#setupClose
     *
     * Note: this conversion process will iterate over all user created
     * databases in the environment, which could be potentially be a costly
     * affair. However, let's opt for simplicity and defer any optimizations
     * until we see whether this is an important use case.
     */
    @Override
    protected void postRecoveryConversion() {

        super.postRecoveryConversion();

        if (needRepConvert) {
            /* Set NameDb to replicated. */
            DatabaseImpl nameDb = null;
            try {
                nameDb = dbMapTree.getDb(DbTree.NAME_DB_ID);
                if (!nameDb.isReplicated()) {
                    nameDb.setIsReplicatedBit();
                    nameDb.setDirty();
                }
            } finally {
                if (nameDb != null) {
                    dbMapTree.releaseDb(nameDb);
                }
            }

            /* Set user defined databases to replicated. */
            Map<DatabaseId, String> idNameMap = dbMapTree.getDbNamesAndIds();
            for (DatabaseId id : idNameMap.keySet()) {
                DatabaseImpl db = null;
                try {
                    db = dbMapTree.getDb(id);
                    if (db != null &&
                        !DbTree.isReservedDbName(idNameMap.get(id))) {

                        db.setIsReplicatedBit();
                        db.setDirty();
                    }
                } finally {
                    if (db != null) {
                        dbMapTree.releaseDb(db);
                    }
                }
            }

            /*
             * Do a checkpointer to flush dirty datbaseImpls that are converted
             * to replicated and write the current VLSNRange to the log.
             */
            CheckpointConfig ckptConfig = new CheckpointConfig();
            ckptConfig.setForce(true);
            ckptConfig.setMinimizeRecoveryTime(true);
            invokeCheckpoint(ckptConfig, "Environment conversion");
        }
    }

    /*
     * Close enough resources to support reopening the environment in the same
     * JVM.
     * @see EnvironmentImpl#doCloseAfterInvalid()
     */
    @Override
    public synchronized void doCloseAfterInvalid() {

        try {
            /* Release the repNode, in order to release sockets. */
            if (repNode != null) {
                repNode.shutdown();
                repNode = null;
            }
        } catch (Exception ignore) {
        }

        super.doCloseAfterInvalid();
    }

    /**
     * Used by error handling to forcibly close an environment, and by tests to
     * close an environment to simulate a crash.  Database handles do not have
     * to be closed before calling this method.  A checkpoint is not performed.
     * The various thread pools will be shutdown abruptly.
     *
     * @throws DatabaseException
     */
    @Override
    public void abnormalClose()
        throws DatabaseException {

        /*
         * Shutdown the daemons, and the checkpointer in particular, before
         * nulling out the vlsnIndex.
         */
        shutdownDaemons();

        try {
            if (repNode != null) {

                /*
                 * Don't fire a LeaveGroupEvent if it's an abnormal close,
                 * otherwise an EnvironmentFailureException would be thrown
                 * because daemons of this Environment have been shutdown.
                 */
                repNode.getMonitorEventManager().disableLeaveGroupEvent();
                repNode.shutdown();
                repNode = null;
            }
        } catch (InterruptedException ignore) {
            /* ignore */
        }

        try {
            vlsnIndexAccess.abnormalCloseVLSNIndex();
        } catch (DatabaseException ignore) {
            /* ignore */
        }

        try {
            super.abnormalClose();
        } catch (DatabaseException ignore) {
            /* ignore */
        }
    }

    /**
     * A replicated log entry has been written on this node. Update the
     * VLSN->LSN mapping. Called outside the log write latch.
     * @throws DatabaseException
     */
    @Override
    public void registerVLSN(LogItem logItem) {
        LogEntryHeader header = logItem.header;
        VLSN vlsn = header.getVLSN();

        /*
         * Although the very first replicated entry of the system is never a
         * syncable log entry type, the first GlobalCBVLSN of the system must
         * start at 1. If we only track the first syncable entry, the
         * GlobalCBVLSN will start a a value > 1, and replicas that are
         * starting up from VLSN 1 will be caught in spurious network restores
         * because VLSN 1 < the GlobalCBVLSN. Therefore treat the VLSN 1 as a
         * syncable entry for the sake of the GlobalCBVLSN.
         */
        if (LogEntryType.isSyncPoint(header.getType()) ||
            VLSN.FIRST_VLSN.equals(vlsn)) {
            repNode.trackSyncableVLSN(vlsn, logItem.lsn);
        }
        vlsnIndex.put(logItem);
    }

    /**
     * Generate the next VLSN and update the DTVLSN value in the item. Note
     * that this method is only invoked when the node is in the Master state,
     * since the master assigns new VLSNs and DTVLSNs, and the replicas simply
     * preserve them.
     *
     * The DTVLSN value must be calculated under the same latch as the updating
     * of the VLSN to ensure that the following invariants are maintained:
     *
     * lsn1 > lsn2 ==> VLSN(lsn1) > VLSN(lsn2)
     * vlsn2 > vlsn1 ==> DTVLSN(vlsn2) >= DTVLSN(vlsn1)
     *
     * where vlsn2 and vlsn1 are transaction commit or abort records.
     *
     * Replicas, when replaying their stream, verify that this invariant is
     * maintained.
     *
     * Commit/Abort records for Replication groups that have a single electable
     * and durable node have their dtvlsn written as the associated VLSN, that
     * is, DTVLSN(vlsn) == vlsn. For all other RG configurations, DTVLSN(vlsn)
     * < vlsn.
     *
     * Commit/Abort Log records that are created by replaying an HA stream from
     * a pre DTVLSN feeder, will have their dtvlsns set to
     * VLSN.UNINITIALIZED_VLSN_SEQUENCE during replica replay. They do not
     * follow this code path.
     *
     * @param entry the log entry with which the VLSN will be associated. If
     * the log entry represents a commit or abort entry, its DTVLSN is modified
     * so that it's correct when it's serialized out.
     */
    @Override
    public VLSN assignVLSNs(LogEntry entry) {
        final VLSN vlsn = vlsnIndex.bump();

        final byte itemType = entry.getLogType().getTypeNum();
        if (itemType != LogEntryType.LOG_TXN_COMMIT.getTypeNum() &&
            itemType !=  LogEntryType.LOG_TXN_ABORT.getTypeNum()) {
            return  vlsn;
        }

        /*
         * A commit or abort record. Compute the DTVLSN value to associate with
         * the commit record and store it into the buffer at the appropriate
         * position.
         */
        final long dtvlsn;

        if (simulatePreDTVLSNMaster) {
            dtvlsn = VLSN.UNINITIALIZED_VLSN_SEQUENCE;
        } else if (repNode.isNeedsAcks()) {
            /*
             * Use the dtvlsn value being tracked via acknowledgments from
             * replicas when replication is being used for durability.
             */
            dtvlsn = getRepNode().getDTVLSN();
        } else {
            /*
             * Replicated environment, but replication is not being used for
             * durability. That is, the commit is self-acknowledged, set dtvlsn
             * == vlsn.
             */
            dtvlsn = getRepNode().updateDTVLSN(vlsn.getSequence());
        }

        final VersionedWriteTxnEnd txnEnd =
            (VersionedWriteTxnEnd)entry.getMainItem();

        /*
         * As a cheap sanity check, commits/aborts on the master are created
         * with VLSN.NULL values, so they can be verified here.
         */
        final long checkDTVLSN = txnEnd.getDTVLSN();
        if (checkDTVLSN != VLSN.NULL_VLSN_SEQUENCE) {
            throw new IllegalStateException("NULL DTVLSN expected at VLSN:" +
                                            vlsn + " not " + checkDTVLSN);
        }

        txnEnd.setDTVLSN(dtvlsn);

        /* Verify invariant */
        if (prevLoggedVLSN > vlsn.getSequence()) {
            if (dtvlsn < prevLoggedDTVLSN) {
                String msg =
                    "DTVLSNs must be in ascending order in the stream. " +
                    " prev DTVLSN:" + prevLoggedDTVLSN +
                    " next DTVLSN:" + dtvlsn + " at VLSN: " + vlsn;
                throw EnvironmentFailureException.unexpectedState(this, msg);
            }
        }
        prevLoggedVLSN = vlsn.getSequence();
        prevLoggedDTVLSN = dtvlsn;

        return vlsn;
    }

    /**
     * Flush any information that needs to go out at checkpoint.  Specifically,
     * write any in-memory VLSN->LSN mappings to the VLSNIndex database so we
     * are guaranteed that the VLSNIndex database will recover properly.
     * This must be committed with noSync because
     *  - the ensuing checkpoint end record will be logged with an fsync and
     *    will effectively force this out
     *  - it's important to minmize lock contention on the vlsn index and
     *    any fsync done during a checkpoint will be expensive, as there may
     *    be quite a lot to push to disk. We don't want to incur that cost
     *    while holding locks on the vlsn index. [#20702]
     */
    @Override
    public void preCheckpointEndFlush()
        throws DatabaseException {

        if (vlsnIndex != null) {
            vlsnIndex.flushToDatabase(Durability.COMMIT_NO_SYNC);
        }
    }

    @Override
    public boolean isMaster() {

        /*
         * The volatile repNode field might be modified by joinGroup(),
         * leaveGroup, or close(), which are synchronized. Keep this method
         * unsynchronized, assign to a temporary field to guard against a
         * change.
         */
        RepNode useNode = repNode;
        if (useNode == null) {
            return false;
        }
        return useNode.isMaster();
    }

    public void setChangeListener(StateChangeListener listener) {
        StateChangeListener prevListener = nodeState.getChangeListener();
        nodeState.setChangeListener(listener);

        /*
         * Call back so that it's aware of the last state change event and
         * the application can initialize itself correctly as a master or
         * replica.
         */
        final StateChangeEvent stateChangeEvent =
            nodeState.getStateChangeEvent();
        try {
            /* Invoke application code and handle any app exceptions. */
            listener.stateChange(stateChangeEvent);
        } catch (Exception e) {
            /* Revert the change. */
            nodeState.setChangeListener(prevListener);
            LoggerUtils.severe
                (envLogger, this,
                 "State Change listener exception: " + e.getMessage());
            /* An application error. */
            throw new EnvironmentFailureException
                (this, EnvironmentFailureReason.LISTENER_EXCEPTION, e);
        }
    }

    public StateChangeListener getChangeListener() {
        return nodeState.getChangeListener();
    }

    public VLSNIndex getVLSNIndex() {
        return vlsnIndex;
    }

    public FeederTxns getFeederTxns() {
        return feederTxns;
    }

    public ReplicatedEnvironmentStats getStats(StatsConfig config) {
        return getStats(config, statKey);
    }

    @Override
    public Collection<StatGroup> getRepStatGroups(StatsConfig config,
                                                  Integer statKey1) {
        ReplicatedEnvironmentStats res = getStats(config, statKey1);
        return (res == null) ? null : res.getStatGroups();
    }

    @Override
    public SortedSet<String> getStatCaptureProjections() {
        return new StatCaptureRepDefinitions().getStatisticProjections();
    }

    @Override
    public StatManager createStatManager() {
        return new RepStatManager(this);
    }

    public FeederFilter getFeederFilter() {
        return feederFilter;
    }

    public ReplicatedEnvironmentStats getStatsInternal(StatsConfig config) {
        if (repNode == null) {
            return null;
        }
        return repNode.getStats(config);
    }

    public ReplicatedEnvironmentStats getStats(
            StatsConfig config,
            Integer contextKey) {
        return ((RepStatManager)statManager).getRepStats(config, contextKey);
    }

    public Replay getReplay() {
        return replay;
    }

    /**
     * Ensures that the environment is currently a Master before proceeding
     * with an operation that requires it to be the master.
     *
     * @throws UnknownMasterException if the node is disconnected
     * @throws ReplicaWriteException if the node is currently a replica
     */
    public void checkIfMaster(Locker locker)
        throws UnknownMasterException, ReplicaWriteException {

        final StateChangeEvent event = nodeState.getStateChangeEvent();

        switch (nodeState.getRepEnvState()) {
            case MASTER:
                break;

            case REPLICA:
                throw new ReplicaWriteException(locker, event);

            case UNKNOWN:
                throw new UnknownMasterException(locker, event);

            case DETACHED:
                throw new UnknownMasterException(locker, event);

            default:
                throw EnvironmentFailureException.unexpectedState
                    ("Unexpected state: " + nodeState.getRepEnvState());
        }
    }

    /**
     * @return the repNode. May return null.
     */
    public RepNode getRepNode() {
        return repNode;
    }

    /**
     * Create an appropriate type of ThreadLocker. Specifically, it creates an
     * MasterThreadLocker if the node is currently a Master, and a
     * ReplicaThreadLocker otherwise, that is, if the node is a Replica, or
     * it's currently in a DETACHED state.
     *
     * @return an instance of MasterThreadLocker or ReplicaThreadLocker
     */
    @Override
    public ThreadLocker createRepThreadLocker() {
        return (isMaster() ?
                new MasterThreadLocker(this) :
                new ReplicaThreadLocker(this));
    }

    /**
     * Create an appropriate type of Replicated transaction. Specifically,
     * it creates a MasterTxn, if the node is currently a Master, a ReadonlyTxn
     * otherwise, that is, if the node is a Replica, or it's currently in a
     * DETACHED state.
     *
     * Note that a ReplicaTxn, used for transaction replay on a Replica is not
     * created on this path. It's created explicitly in the Replay loop by a
     * Replica.
     *
     * @param config  the transaction configuration
     *
     * @return an instance of MasterTxn or ReadonlyTxn
     * @throws DatabaseException
     */
    @Override
    public Txn createRepUserTxn(TransactionConfig config)
        throws DatabaseException {

        return (isMaster() &&
            !config.getReadOnly() &&
            !config.getLocalWrite()) ?
            MasterTxn.create(this, config, nameIdPair) :
            new ReadonlyTxn(this, config);
    }

    /**
     * Ensure that a  sufficient number of feeders are available before
     * proceeding with a master transaction begin.
     *
     * @param txn the master transaction being initiated.
     *
     * @throws InterruptedException
     * @throws DatabaseException if there were insufficient Replicas after the
     * timeout period.
     */
    public void txnBeginHook(MasterTxn txn)
        throws InterruptedException,
               DatabaseException {

        checkIfInvalid();
        final long txnTimeout = txn.getTxnTimeout();
        int timeout = insufficientReplicasTimeout;

        if ((txnTimeout != 0) && (txnTimeout < insufficientReplicasTimeout)) {
            timeout = (int) txnTimeout;
        }

        repNode.getDurabilityQuorum().ensureReplicasForCommit(txn, timeout);
    }

    /**
     * Installs the commit-blocking latch that is used to halt the commit/abort
     * of transactions, in the final phase of a master transfer.
     *
     * @see #updateCBVLSN(LocalCBVLSNUpdater)
     */
    public void blockTxnCompletion(CountDownLatch blocker)
        throws InterruptedException {

        ReentrantReadWriteLock.WriteLock lock = blockLatchLock.writeLock();
        lock.lockInterruptibly();
        try {
            blockTxnLatch = blocker;
        } finally {
            lock.unlock();
        }
    }

    /**
     * If the GlobalCBVLSN is not defunct, updates the CBVLSN on behalf of a
     * Feeder input thread (or FeederManager running in the RepNode thread).
     * If the GlobalCBVLSN is defunct, does nothing.
     * <p>
     * Does the update while avoiding the possibility that any resulting
     * GroupDB update may get blocked behind the final phase of a master
     * transfer.
     * <p>
     * We skip the update if we're at the point of blocking new transactions
     * for a master transfer.  And we use a read/write lock in order to be able
     * to examine that state safely.
     */
    public void updateCBVLSN(LocalCBVLSNUpdater updater) {

        if (repNode.isGlobalCBVLSNDefunct()) {
            return;
        }

        ReentrantReadWriteLock.ReadLock lock = blockLatchLock.readLock();
        lock.lock();
        try {
            if (blockTxnLatch.getCount() > 0) {
                return;
            }
            updater.update();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Releases the transaction block latch.
     */
    public void unblockTxnCompletion() {
        LoggerUtils.info(envLogger, this, "Releasing commit block latch");
        blockTxnLatch.countDown();
    }

    /**
     * This hook is used primarily to perform the final checks before allowing
     * the commit operation to proceed. The following checks are performed
     * here:
     *
     * 1) Check for master
     * 2) Check for sufficient Feeder connections to ensure that the commit
     *    policy could be implemented. There is no guarantee that they will all
     *    ack the commit request.
     *
     * The method also associates a latch with the transaction. The latch is
     * used to delay the commit operation until a sufficient number of commits
     * have been received.
     *
     * In addition, when mastership transfers are done, and this node is the
     * original master, commits and aborts are blocked so as to avoid hard
     * recovery after electing a new master, see [#18081].
     *
     * @param txn the master transaction being committed
     *
     * @throws InsufficientReplicasException if the feeder is not in contact
     * with enough replicas.
     * @throws RestartRequiredException if the environment is invalid.
     * @throws UnknownMasterException if the current master is unknown.
     * @throws ReplicaWriteException if the node transitioned to a Replica
     * after the transaction was initiated.
     */
    public void preLogCommitHook(MasterTxn txn)
        throws InsufficientReplicasException,
               RestartRequiredException,
               UnknownMasterException,
               ReplicaWriteException,
               EnvironmentFailureException {

        checkIfInvalid();
        checkIfMaster(txn);
        checkBlock(txn);

        /* Still a master, check for a sufficient number of connections */
        int activeReplicaCount =
            repNode.feederManager().activeAckReplicaCount();
        ReplicaAckPolicy ackPolicy =
            txn.getCommitDurability().getReplicaAck();
        int requiredAckCount = txn.getRequiredAckCount();

        if (envLogger.isLoggable(Level.FINE)) {
            LoggerUtils.fine(envLogger, this,
                             "Txn " + txn.getId() + " requires: " +
                             requiredAckCount + " active: " +
                             activeReplicaCount +
                             " replica acks. Commit Policy: " + ackPolicy);
        }

        if (requiredAckCount > activeReplicaCount) {
            /* Check for possible activation of Primary */
            if (ackPolicy.equals(ReplicaAckPolicy.SIMPLE_MAJORITY) &&
                repNode.getArbiter().activateArbitration()) {
                txn.resetRequiredAckCount();
            } else if (useArbiter(txn)) {
                /*
                 * Note we could change the check to allow a degraded
                 * write from any group size. Limit is place at rep group
                 * size of two due to the priority of requirements
                 * and lower cost of testing.
                 */
                txn.setArbiterAck(true);
            } else {
                /*
                 * Capture the set to ensure it's consistent with the exception
                 * message.
                 */
                final boolean includeArbiters =
                    !ackPolicy.equals(ReplicaAckPolicy.ALL);
                final Set<String> activeAckRepSet =
                    repNode.feederManager().activeAckReplicas(includeArbiters);

                if (requiredAckCount > activeAckRepSet.size()) {
                    /* No change in window, throw exception */
                    InsufficientReplicasException ire =
                    new InsufficientReplicasException
                        (txn, ackPolicy, requiredAckCount, activeAckRepSet);
                    LoggerUtils.info(envLogger, this, ire.getMessage());
                    throw ire;
                }

                /**
                 * A new replica became active in the window between the
                 * first fast check and the second slower check, just continue
                 */
            }
        }
        feederTxns.setupForAcks(txn);
    }

    /*
     * Block transaction commits/aborts if this node is the original master
     * and we're doing Master Transfer.
     */
    private void checkBlock(MasterTxn txn) {
        try {

            /*
             * Lock out the setting of the block latch by Master Transfer in
             * the interval between waiting on the latch and setting the VLSN
             * for the commit: Master Transfer needs to get a coherent idea of
             * the final VLSN when it sets the latch.  This lock will be
             * released by the {@code postLogXxxHook()} functions, one of which
             * is guaranteed to be called, unless an Environment-invalidating
             * exception occurs.
             */
            if (txn.lockOnce()) {
                blockLatchLock.readLock().lockInterruptibly();
            }

            if (blockTxnLatch.getCount() > 0) {
                LoggerUtils.info(envLogger, this,
                                 "Block transaction: " + txn.getId() +
                                 " pending master transfer. Write locks = " +
                                 txn.getWriteLockIds());
            }

            final long txnTimeout = txn.getTxnTimeout();
            if (txnTimeout <= 0) {
                blockTxnLatch.await();
            } else if (! blockTxnLatch.await(txnTimeout,
                                             TimeUnit.MILLISECONDS)) {

                final String message =
                    "Timed out waiting for master transfer. " +
                    "Configured transaction timeout:" + txnTimeout + "ms";

                throw new TransactionTimeoutException(txn, message);
            }

            checkIfInvalid();

            /*
             * Check again, after the block! The block may be a result of a
             * master->replica transfer, and if this node transitions from
             * master to replica, this node will be disqualified from being
             * able to commit transactions.
             */
            checkIfMaster(txn);

        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(this, e);
        }
    }

    /**
     * It ensures that the feeder obtains the requisite number of
     * acknowledgments required for a successful commit.
     *
     * @param txn The MasterTxn that was committed locally.
     *
     * @throws InterruptedException if the thread was interrupted while
     * waiting for acknowledgments.
     * @throws InsufficientAcksException if the master received an insufficient
     * number of commit acknowledgments within the replica commit timeout
     * period.
     * @throws EnvironmentFailureException
     */
    public void postLogCommitHook(MasterTxn txn, LogItem commitItem)
       throws InsufficientAcksException,
              InterruptedException,
              EnvironmentFailureException {
        final long txnTimeout = txn.getTxnTimeout();
        int timeout = replicaAckTimeout;

        if ((txnTimeout != 0) && (txnTimeout < replicaAckTimeout)) {
            timeout = (int) txnTimeout;
        }
        postLogCommitHookInternal(txn, commitItem, timeout);
    }


    private void postLogCommitHookInternal(MasterTxn txn, LogItem commitItem,
                                           int ackTimeout)
        throws InsufficientAcksException,
               InterruptedException,
               EnvironmentFailureException {
        if (txn.unlockOnce()) {
            blockLatchLock.readLock().unlock();
        }

        if (!isValid()) {
            final int currentRequiredAckCount = repNode.getDurabilityQuorum().
            getCurrentRequiredAckCount(txn.getCommitDurability().
                                       getReplicaAck());
            if (currentRequiredAckCount > 0) {
                /* Throw a more actionable and accurate exception than EFE */
                final String msg =
                    "Acks could not be obtained because the environment" +
                    "was invalidated";
                LoggerUtils.info(envLogger, this, msg);
                throw new InsufficientAcksException(msg);
            }
            /* No acks are required, transaction is complete. */
            return;
        }

        /* Check if using Arbiter for transaction. */
        if (txn.getArbiterAck()) {
            // get the arbiter acker source and add txn id to its queue.
            Feeder arbFeeder = repNode.feederManager().getArbiterFeeder();
            if (arbFeeder != null) {
                ArbiterFeederSource as = arbFeeder.getArbiterFeederSource();
                as.addCommit(commitItem);
            }
        }

        /* Don't do master check, the transaction has already been committed */
        try {
            feederTxns.awaitReplicaAcks(txn, ackTimeout);
        } catch (InsufficientAcksException e) {
            if (txn.getArbiterAck() == false && useArbiter(txn)) {
                txn.setArbiterAck(true);
                postLogCommitHookInternal(txn, commitItem, arbiterAckTimeout);
                return;
            }
            LoggerUtils.info(envLogger, this, e.getMessage());
            throw e;
        }
    }

    /**
     * Invoked before aborting a MasterTxn, this happens when the master is
     * going to be a replica because of mastership transfer. We do this to make
     * sure that the replica going to be the master has the most recent log and
     * no hard recovery would happen after its election, see SR [#18081].
     *
     * @param txn The MasterTxn that was aborted locally.
     *
     * @throws ReplicaWriteException if the node transitioned to a Replica
     * after the transaction was initiated.
     * @throws UnknownMasterException if the current master is unknown.
     * @throws EnvironmentFailureException
     */
    public void preLogAbortHook(MasterTxn txn)
        throws EnvironmentFailureException,
               ReplicaWriteException,
               UnknownMasterException {

        checkIfInvalid();
        checkIfMaster(txn);
        checkBlock(txn);
    }

    /**
     * Releases the block latch lock, if held.  This hook is called in the
     * normal course of Txn.abort(), once the abort log record has been written
     * and the associated VLSN stored.
     */
    public void postLogAbortHook(MasterTxn txn) {
        if (txn.unlockOnce()) {
            blockLatchLock.readLock().unlock();
        }
    }

    /**
     * Removes any pending acknowledgments that were registered by the
     * preLogCommitHook.  This hook is called only when a {@code commit()}
     * fails and therefore must be aborted.
     */
    public void postLogCommitAbortHook(MasterTxn txn) {
        LoggerUtils.info(envLogger, this,
                         "post log abort hook for txn: " + txn.getId());
        if (txn.unlockOnce()) {
            blockLatchLock.readLock().unlock();
        }
        feederTxns.clearTransactionAcks(txn);
    }

    /**
     * Create a ReplayTxn for recovery processing.
     */
    @Override
    public Txn createReplayTxn(long txnId)
        throws DatabaseException {

        return
            new ReplayTxn(this, TransactionConfig.DEFAULT, txnId, envLogger);
    }

    /**
     * Used by environment recovery to get a tracker to collect VLSN-LSN
     * mappings that are within the recovery part of the log. These might
     * not be reflected in the persistent mapping db.
     */
    @Override
    public VLSNRecoveryProxy getVLSNProxy() {
        int stride = configManager.getInt(RepParams.VLSN_STRIDE);
        int maxMappings = configManager.getInt(RepParams.VLSN_MAX_MAP);
        int maxDist = configManager.getInt(RepParams.VLSN_MAX_DIST);

        return new VLSNRecoveryTracker(this, stride, maxMappings, maxDist);
    }

    public UUID getUUID() {
        return repNode.getUUID();
    }

    /**
     * Used during testing to introduce artificial clock skews.
     */
    public static void setSkewMs(int skewMs) {
        clockSkewMs = skewMs;
    }

    public static int getClockSkewMs() {
        return clockSkewMs;
    }

    /**
     * Truncate the head of the VLSNIndex to allow file deletion, if possible.
     */
    @Override
    public boolean tryVlsnHeadTruncate(long bytesNeeded) {
        return vlsnIndex.tryTruncateFromHead(bytesNeeded);
    }

    public int getNodeId() {
        return nameIdPair.getId();
    }

    public NameIdPair getNameIdPair() {
        return nameIdPair;
    }

    @Override
    public long getReplayTxnTimeout() {
        return replayTxnTimeout;
    }

    /* Return the default consistency policy. */
    @Override
    public ReplicaConsistencyPolicy getDefaultConsistencyPolicy() {
        return defaultConsistencyPolicy;
    }

    /**
     * The default consistency is not currently mutable in the API, but can be
     * set for testing purposes.
     *
     * TODO: Make it mutable in the API, since Durability is mutable.
     */
    public void setDefaultConsistencyPolicy(ReplicaConsistencyPolicy policy) {
        defaultConsistencyPolicy = policy;
    }

    /* Returns the on disk LSN for VLSN. */
    private long getLsnForVLSN(VLSN vlsn, int readBufferSize) {
        /* Returns the file number which is nearest to the vlsn. */
        long fileNumber = vlsnIndex.getLTEFileNumber(vlsn);

        /* Start reading from the nearest file. */
        FeederReader feederReader =
            new FeederReader(this,
                             vlsnIndex,
                             DbLsn.makeLsn(fileNumber, 0),
                             readBufferSize);

        try {
            feederReader.initScan(vlsn);

            /*
             * Go on scan the log until FeederReader find the target VLSN,
             * thrown out an EnvironmentFailureException if it can't be found.
             */
            if (!feederReader.readNextEntry()) {
                throw EnvironmentFailureException.unexpectedState
                    ("VLSN not found: " + vlsn);
            }
        } catch (IOException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }

        return feederReader.getLastLsn();
    }

    /**
     * Returns the logged durable txn VLSN. The logged DTVLSN is part of the
     * last txn commit or abort entry.
     *
     * @return the persistent DTVLSN. The value may be
     * VLSN.UNINITIALIZED_VLSN_SEQUENCE if the environment was newly created,
     * that is, it has no transactions in it as yet, or if the last entry was
     * created by a pre-DTVLSN master.
     *
     * @throws FileNotFoundException if the file containing the last txn commit
     * or abort entry does not exist
     */
    public long getLoggedDTVLSN()
        throws FileNotFoundException {

        final VLSN lastTxnEnd = getLastTxnEnd();

        if (lastTxnEnd.isNull()) {
            /* A brand new environment with no transactions in it. */
            return VLSN.UNINITIALIZED_VLSN_SEQUENCE;
        }

        final long lsn = getLsnForVLSN(lastTxnEnd,
                                       1024 /* buffer size for txn end */);
        final TxnEnd txnEnd =
            (TxnEnd)getLogManager().getLogEntry(lsn).getMainItem();

        long dtvlsn = txnEnd.getDTVLSN();
        if (dtvlsn != VLSN.UNINITIALIZED_VLSN_SEQUENCE) {
            return dtvlsn;
        }

        /* A JE version <= 7.1 log entry. */
        LoggerUtils.logMsg(envLogger, this, Level.INFO,
                           "Pre DTVLSN log, starting with zero dtvlsn");

        return dtvlsn;
    }

    /* Returns the end of the log. */
    @Override
    public long getEndOfLog() {
        return vlsnIndex.getRange().getLast().getSequence();
    }

    /**
     * Returns true if the VLSN is preserved as the record version.
     */
    @Override
    public boolean getPreserveVLSN() {
        return preserveVLSN;
    }

    /**
     * Returns true if the VLSN is both preserved and cached.
     */
    @Override
    public boolean getCacheVLSN() {
        return preserveVLSN && cacheVLSN;
    }

    /**
     * @see EnvironmentImpl#getName
     */
    @Override
    public String getName() {
        return nameIdPair + ":" + super.getName();
    }

    /**
     * Return true if this environment is part of a replication group.
     */
    @Override
    public boolean isReplicated() {
        return true;
    }

    /**
     * Return true if this environment is used as an Arbiter.
     */
    @Override
    public boolean isArbiter() {
        return isArbiter;
    }

    /**
     * Check whether this environment can be opened on an existing environment
     * directory.
     */
    @Override
    public void checkRulesForExistingEnv(boolean dbTreeReplicatedBit,
                                         boolean dbTreePreserveVLSN)
        throws UnsupportedOperationException {

        if (!dbTreeReplicatedBit) {

            /*
             * We are attempting to open an existing, non-replicated
             * environment.
             */
            throw new UnsupportedOperationException
                ("This environment must be converted for replication." +
                 " using com.sleepycat.je.rep.util.DbEnableReplication.");
        }

        /* The preserveVLSN setting is forever immutable. */
        if (dbTreePreserveVLSN != getPreserveVLSN()) {
            throw new IllegalArgumentException
                (RepParams.PRESERVE_RECORD_VERSION.getName() +
                 " parameter may not be changed." +
                 " Previous value: " + dbTreePreserveVLSN +
                 " New value: " + getPreserveVLSN());
        }
    }

    /**
     * Returns the hostname associated with this node.
     *
     * @return the hostname
     */
    // 负责从当前节点的复制配置中解析并返回该节点绑定的主机名（Hostname）
    public String getHostName() {
        String hostAndPort = configManager.get(RepParams.NODE_HOST_PORT);
        int colonToken = hostAndPort.indexOf(":");
        return (colonToken >= 0) ?
               hostAndPort.substring(0, colonToken) :
               hostAndPort;
    }

    /**
     * Returns the  port used by the replication node.
     *
     * @return the port number
     */
    // 负责从当前节点的配置中解析并返回复制节点所使用的网络端口号（Port）
    public int getPort() {
        String hostAndPort = configManager.get(RepParams.NODE_HOST_PORT);
        int colonToken = hostAndPort.indexOf(":");
        return (colonToken >= 0) ?
                Integer.parseInt(hostAndPort.substring(colonToken + 1)) :
                configManager.getInt(RepParams.DEFAULT_PORT);
    }

    /* Convenience method for returning replication sockets. */
    // 用于构建并返回当前复制节点网络通信所使用的完整套接字地址对象
    public InetSocketAddress getSocket() {
        return new InetSocketAddress(getHostName(), getPort());
    }

    /**
     * Returns the JE version that is currently running on this node,
     * consulting the TEST_JE_VERSION configuration parameter for a test
     * override.
     */
    public JEVersion getCurrentJEVersion() {
        final String testJEVersion = configManager.get(TEST_JE_VERSION);
        return testJEVersion.isEmpty() ?
            JEVersion.CURRENT_VERSION :
            new JEVersion(testJEVersion);
    }

    /**
     * Returns the set of sockets associated with helper nodes.
     *
     * @return the set of helper sockets, returns an empty set if there
     * are no helpers.
     */
    public Set<InetSocketAddress> getHelperSockets() {
        String helperHosts = configManager.get(RepParams.HELPER_HOSTS);
        return HostPortPair.getSockets(helperHosts);
    }

    /**
     * Called when a node has identified itself as the master, which is when
     * the RepNode.selfElect is called. The database should not exist at
     * this point.
     *
     * Lock hierarchy: GroupDbLock -> sync on EnvironmentImpl
     * @throws DatabaseException
     */
    public DatabaseImpl createGroupDb()
        throws DatabaseException {

        assert isMaster();

        try {
            groupDbLock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }

        try {
            if (groupDbImpl != null) {
                throw EnvironmentFailureException.unexpectedState
                    ("GroupDb should not exist.");
            }

            DatabaseImpl newDbImpl = null;
            Txn txn = null;
            try {
                TransactionConfig txnConfig = new TransactionConfig();
                txnConfig.setDurability(new Durability(SyncPolicy.SYNC,
                                                       SyncPolicy.SYNC,
                                                       ReplicaAckPolicy.NONE));
                txnConfig.setConsistencyPolicy(NO_CONSISTENCY);
                txn = new MasterTxn(this,
                                    txnConfig,
                                    getNameIdPair());

                /* Database should not exist yet, create it now */
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(true);
                dbConfig.setExclusiveCreate(true);
                dbConfig.setReplicated(true);

                newDbImpl = getDbTree().createInternalDb
                    (txn, DbType.REP_GROUP.getInternalName(), dbConfig);
                txn.commit();
                txn = null;
            } finally {
                if (txn != null) {
                    txn.abort();
                }
            }

            groupDbImpl = newDbImpl;
        } finally {
            groupDbLock.unlock();
        }
        return groupDbImpl;
    }

    /**
     * Open the group db, which should exist already, using NO_CONSISTENCY.
     */
    public DatabaseImpl getGroupDb()
        throws DatabaseNotFoundException,
               DatabaseException {

        return openGroupDb(false /* doLockProbe */);
    }

    /**
     * Open the group db, which should exist already, using NO_CONSISTENCY. Do
     * not wait on the group db lock, return null if the databaseImpl hasn't
     * been created and we can't obtain it.
     *
     * Lock hierarchy: GroupDbLock -> sync on EnvironmentImpl
     */
    public DatabaseImpl probeGroupDb()
        throws DatabaseException {

        try {
            return openGroupDb(true /* doLockProbe */);
        } catch (DatabaseNotFoundException e) {
            /* Should never happen, DB should exist. */
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Do the work of creating the lock and then assigning the groupDbImpl
     * field, using NO_CONSISTENCY.
     *
     * @throws DatabaseException
     * @throws DatabaseNotFoundException
     */
    private DatabaseImpl openGroupDb(final boolean doLockProbe)
        throws DatabaseNotFoundException, DatabaseException {

        /* Acquire the lock. */
        try {
            if (doLockProbe) {
                if (!groupDbLock.tryLock(1, TimeUnit.MILLISECONDS)) {
                    /* Contention, try later. */
                    return null;
                }
            } else {
                groupDbLock.lockInterruptibly();
            }
        } catch(InterruptedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }

        Txn txn = null;
        try {
            if (groupDbImpl != null) {
                return groupDbImpl;
            }

            DatabaseImpl newDbImpl = null;
            TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setConsistencyPolicy(NO_CONSISTENCY);
            txn = new ReadonlyTxn(this, txnConfig);

            newDbImpl = getDbTree().getDb(txn,
                                          DbType.REP_GROUP.getInternalName(),
                                          null /* databaseHandle */,
                                          false);
            if (newDbImpl == null) {
                throw new DatabaseNotFoundException
                    (DbType.REP_GROUP.getInternalName());
            }
            txn.commit();
            txn = null;

            groupDbImpl = newDbImpl;
            return groupDbImpl;
        } finally {
            if (txn != null) {
                txn.abort();
            }
            groupDbLock.unlock();
        }
    }

    /**
     * Return true if the node has been configured as a Designated Primary.
     * This does not necessarily mean that the node is actively operating in
     * designated primary mode. See
     * {@link com.sleepycat.je.rep.arbitration.Arbiter#isActive}
     */
    public boolean isDesignatedPrimary() {
        return getConfigManager().getBoolean(RepParams.DESIGNATED_PRIMARY);
    }

    @Override
    public boolean addDbBackup(DbBackup backup) {
        synchronized (backups) {
            if (backupProhibited) {
                return false;
            }
            boolean added = backups.add(backup);
            assert added;
        }

        super.addDbBackup(backup);
        return true;
    }

    @Override
    public void removeDbBackup(DbBackup backup) {
        synchronized (backups) {
            boolean removed = backups.remove(backup);
            assert removed;
        }
        super.removeDbBackup(backup);
    }

    /* Invalidate all the on going DbBackups, used in Replay.rollback(). */
    public void invalidateBackups(long fileNumber) {
        synchronized (backups) {
            for (DbBackup backup : backups) {
                backup.invalidate(fileNumber);
            }
        }
    }

    /* Set the backupProhibited status, used in Replay.rollback(). */
    public void setBackupProhibited(boolean backupProhibited) {
        synchronized (backups) {
            this.backupProhibited = backupProhibited;
        }
    }

    /* For creating a rep exception from standalone code. */
    @Override
    public LockPreemptedException
        createLockPreemptedException(Locker locker, Throwable cause) {
        return new LockPreemptedException(locker, cause);
    }

    /* For creating a rep exception from standalone code. */
    @Override
    public DatabasePreemptedException
        createDatabasePreemptedException(String msg,
                                         String dbName,
                                         Database db) {
        return new DatabasePreemptedException(msg, dbName, db);
    }

    /* For creating a rep exception from standalone code. */
    @Override
    public LogOverwriteException createLogOverwriteException(String msg) {
        return new LogOverwriteException(msg);
    }

    @Override
    public int getReplayFreeDiskPercent() {
        return getConfigManager().getInt(REPLAY_FREE_DISK_PERCENT);
    }

    /**
     * Sets up the environment for group shutdown when the environment is
     * closed.
     *
     * @see ReplicatedEnvironment#shutdownGroup(long, TimeUnit)
     */
    public void shutdownGroupSetup(long timeoutMs) {
        final int openCount = getAppOpenCount();
        if (openCount > 1) {
            throw new IllegalStateException
                ("Environment has " + (openCount - 1) +
                 " additional open handles.");
        }

        final int backupCount = getBackupCount();
        if (backupCount > 0) {
            throw new IllegalStateException
                ("Environment has " + backupCount +
                 " DbBackups in progress.");
        }

        repNode.shutdownGroupOnClose(timeoutMs);
    }

    public String transferMaster(Set<String> replicas,
                               long timeout,
                               boolean force) {
        return repNode.transferMaster(replicas, timeout, force);
    }

    /**
     * Dump interesting aspects of the node's state. Currently for debugging
     * use, possibly useful for field support.
     */
    public String dumpState() {
        StringBuilder sb = new StringBuilder();

        sb.append(getNameIdPair());
        sb.append("[").append(getState()).append("] " );

        if (repNode != null) {
            sb.append(repNode.dumpState());
        }

        if (vlsnIndex != null) {
            sb.append("vlsnRange=");
            sb.append(vlsnIndex.getRange()).append("\n");
        }

        if (replay != null) {
            sb.append(replay.dumpState());
        }

        return sb.toString();
    }

    /**
     * Dumps the state associated with all active Feeders that supply
     * acknowledgments, along with identifying information about the node and
     * its current HA state.
     */
    public String dumpAckFeederState() {
        return getNameIdPair() + "[" + getState() + "]" +
               repNode.dumpAckFeederState() ;
    }

    /**
     * If this node was started with a hard recovery, preserve that
     * information.
     */
    public void setHardRecoveryInfo(RollbackException e) {
        hardRecoveryStat.set(true);
        hardRecoveryInfoStat.set(e.getMessage());
    }

    public StatGroup getNodeStats() {
        return nodeStats;
    }

    /**
     * Ensure that the in-memory vlsn index encompasses all logged entries
     * before it is flushed to disk. A No-Op for non-replicated systems.
     * [#19754]
     */
    @Override
    public void awaitVLSNConsistency() {
        vlsnIndex.awaitConsistency();
    }

    public void setSyncupProgress(SyncupProgress progress) {
        setSyncupProgress(progress, 0, -1);
    }

    public void setSyncupProgress(SyncupProgress progress, long n, long total) {
        if (syncupProgressListener == null) {
            return;
        }

        if (!(syncupProgressListener.progress(progress, n, total))) {
            throw new EnvironmentFailureException
              (this, EnvironmentFailureReason.PROGRESS_LISTENER_HALT,
              "ReplicatedEnvironmentConfig.syncupProgressListener: ");
        }
    }

    /**
     * Test method to create pre-DTVLSN logs. When this is turned on in a test
     * environment the dtvlsn value in the log is written as a
     * UNINITIALIZED_VLSN_SEQUENCE (zero), which is the value that
     * deserialization would assign to it if a new replica came across an older
     * version commit or abort record.
     */
    public static void setSimulatePreDTVLSNMaster(
        boolean simulatePreDTVLSNMaster) {

        RepImpl.simulatePreDTVLSNMaster = simulatePreDTVLSNMaster;
    }

    public static boolean isSimulatePreDTVLSNMaster() {
        return simulatePreDTVLSNMaster;
    }

    public LogFileRewriteListener getLogRewriteListener() {
        return logRewriteListener;
    }

    public ReplicationNetworkConfig getRepNetConfig() {
        return repNetConfig;
    }

    public DataChannelFactory getChannelFactory() {
        initializeChannelFactory();
        return channelFactory;
    }

    @Override
    public void invalidate(EnvironmentFailureException e) {
        super.invalidate(e);
        unblockTxnCompletion();
    }

    public VLSN getLastTxnEnd() {
        return vlsnIndexAccess.getLastTxnEnd();
    }

    /**
     * Private class to prevent used of the close() method by the application
     * on an internal handle.
     */
    private static class InternalReplicatedEnvironment
        extends ReplicatedEnvironment {

        public InternalReplicatedEnvironment(File environmentHome,
                                             ReplicationConfig cloneRepConfig,
                                             EnvironmentConfig cloneConfig,
                                             RepImpl envImpl) {
            super(environmentHome, cloneRepConfig, cloneConfig,
                  null /*consistencyPolicy*/, null /*initialElectionPolicy*/,
                  false /*joinGroup*/, envImpl);
        }

        @Override
        protected boolean isInternalHandle() {
            return true;
        }

        @Override
        public synchronized void close() {
            throw EnvironmentFailureException.unexpectedState
                ("close() not permitted on an internal environment handle");
        }
    }

    /**
     * Peruse the environment wide transaction table, and return a set of
     * all existing MasterTxns.
     */
    public Set<MasterTxn> getExistingMasterTxns() {
        return getTxnManager().getTxns(MasterTxn.class);
    }

    /**
     * RepImpl supplies the last txn abort or commit vlsn for use cases such as
     * determining how caught up a feeder or master transfer is. This info is
     * usually obtained from the VLSNRange via the VLSNIndex, but in some types
     * of environment shutdowns, the VLSNIndex may need to be nulled out.  When
     * that happens, VLSNIndexAccess will switch over from using the VLSNIndex
     * to obtain the range, to using a reference to the last known
     * VLSNRange. Note that the VLSNRange instance held within VLSNIndex is
     * constantly being replaced when the replication stream is active., and
     * that's why LastTxnEndAccess generally obtains the range via the
     * VLSNIndex, rather keeping a reference to a VLSNRange instance.
     */
    private class VLSNIndexAccess {

        private VLSNRange savedRange;

        synchronized VLSN getLastTxnEnd() {
            if (vlsnIndex != null) {
                return vlsnIndex.getRange().getLastTxnEnd();
            }
            return savedRange.getLastTxnEnd();
        }

        /**
         * Save the last range so the lastTxnEnd value can continue
         * to be available, and null out the vlsnIndex.
         */
        synchronized void closeVLSNIndex(boolean checkpointed) {
            if (vlsnIndex != null) {
                vlsnIndex.close(checkpointed);
                savedRange = vlsnIndex.getRange();
                vlsnIndex = null;
            }
        }

        /**
         * Save the last range so the lastTxnEnd value can continue
         * to be available, and null out the vlsnIndex.
         */
        synchronized void abnormalCloseVLSNIndex() {
            if (vlsnIndex != null) {
                vlsnIndex.abnormalClose();
                savedRange = vlsnIndex.getRange();
                vlsnIndex = null;
            }
        }
    }

    /**
     * Checks that writing records with a TTL is allowed.
     *
     * @throws IllegalStateException if any node in the group is less than
     * JE_TTL_VERSION.
     */
    @Override
    public void checkTTLAvailable() {
        if (isTTLAvailable) {
            return;
        }
        final JEVersion requiredJEVersion = TTL.getMinJEVersion();
        try {
            repNode.setMinJEVersion(requiredJEVersion);
            isTTLAvailable = true;
        } catch (MinJEVersionUnsupportedException e) {
            if (e.nodeVersion == null) {
                throw new IllegalStateException(
                    "TTL is not currently supported." +
                    " The version running on node " + e.nodeName +
                    " could not be determined," +
                    " but this feature requires version " +
                    requiredJEVersion.getNumericVersionString() +
                    " or later.");
            }
            throw new IllegalStateException(
                "TTL is not currently supported." +
                " Node " + e.nodeName + " is running version " +
                e.nodeVersion.getNumericVersionString() +
                ", but this feature requires version " +
                requiredJEVersion.getNumericVersionString() +
                " or later.");
        }
    }

    /**
     * Recovery encountered a RestoreRequired marker.
     */
    @Override
    public void handleRestoreRequired(RestoreRequired restoreRequired) {

        switch (restoreRequired.getFailureType()) {
        case NETWORK_RESTORE:
            /*
             * A network restore must be done to get a coherent copy of
             * the log files into this environment's directory.
             */
            throw new InsufficientLogException(
                restoreRequired.getProperties(),
                configManager.get(RepParams.HELPER_HOSTS));
        default:
            /* Not a type we can handle, go to the default behavior */
            super.handleRestoreRequired(restoreRequired);
        }
    }
    
    private boolean useArbiter(MasterTxn txn) {
        if (allowArbiterAck &&
            repNode.getGroup().getAckGroupSize() == 2 &&
            repNode.feederManager().activeAckArbiterCount() > 0 &&
            txn.getCommitDurability().getReplicaAck() ==
            ReplicaAckPolicy.SIMPLE_MAJORITY) {
            return true;
        }
        return false;
    }

    public void setAuthenticator(StreamAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    public StreamAuthenticator getAuthenticator() {
        return authenticator;
    }
}
