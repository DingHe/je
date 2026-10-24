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

package com.sleepycat.je.rep;

import java.io.File;
import java.io.PrintStream;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.VersionMismatchException;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbEnvPool;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.RepConfigProxy;
import com.sleepycat.je.dbi.StartupTracker.Phase;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.utilint.DatabaseUtil;

/**
 * A replicated database environment that is a node in a replication
 * group. Please read the {@link <a
 * href="{@docRoot}/../ReplicationGuide/introduction.html">Berkeley DB JE High
 * Availability Overview</a>} for an introduction to basic concepts and key
 * terminology.
 * <p>
 * Berkeley DB JE High Availability (JE HA) is a replicated, embedded database
 * management system which provides fast, reliable, and scalable data
 * management. JE HA enables replication of an environment across a Replication
 * Group. A ReplicatedEnvironment is a single node in the replication group.
 * <p>
 * ReplicatedEnvironment extends {@link Environment}. All database operations
 * are executed in the same fashion in both replicated and non replicated
 * applications, using {@link Environment} methods.  A ReplicatedEnvironment
 * must be transactional. All replicated databases created in the replicated
 * environment must be transactional as well.  However, <a
 * href="#nonRepDbs">non-replicated databases</a> may be used as well.
 * <p>
 * ReplicatedEnvironment handles are analogous to {@link Environment}
 * handles. A replicated environment handle is a ReplicatedEnvironment
 * instance; multiple ReplicatedEnvironment instances may be created for the
 * same physical directory. In other words, more than one ReplicatedEnvironment
 * handle may be open at a time for a given environment.
 * </p>
 * <p>
 * A ReplicatedEnvironment joins its replication group when it is instantiated.
 * When the constructor returns, the node will have established contact with
 * the other members of the group and will be ready to service operations. The
 * {@link <a href="{@docRoot}/../ReplicationGuide/lifecycle.html">life
 * cycle</a>} overview is useful for understanding replication group creation.
 * <p>
 * The membership of a replication group is dynamically defined. The group
 * comes into being when ReplicatedEnvironments that are configured as members
 * of a group are created and discover each other. ReplicatedEnvironments are
 * identified by a group name, a node name, and a hostname:port
 * value. Membership information for electable and monitor nodes is stored in
 * an internal, replicated database available to electable and secondary nodes.
 * <p>
 * To start a node and join a group, instantiate a ReplicatedEnvironment. The
 * very first instantiation of a node differs slightly from all future
 * instantiations. A brand new, empty node does not yet have access to the
 * membership database, so it must discover the group with the aid of a
 * helper node, which is a fellow member. If this is the very first node of the
 * entire group, there is no available helper. Instead, the helper host address
 * to use is the node's own address. The example below takes the simple
 * approach of creating a replication group by starting up a node that will act
 * as the first master, though it is not necessary to follow this order.
 * {@link <a
 * href="{@docRoot}/../ReplicationGuide/progoverview.html#configrepenv">
 * Configuring Replicated Environments</a>} describes group startup in greater
 * detail.
 * <p>
 * To create the <b>master node in a brand new group</b>, instantiate a
 * ReplicatedEnvironment this way:
 * <pre class="code">
 * EnvironmentConfig envConfig = new EnvironmentConfig();
 * envConfig.setAllowCreate(true);
 * envConfig.setTransactional(true);
 *
 * // Identify the node
 * ReplicationConfig repConfig = new ReplicationConfig();
 * repConfig.setGroupName("PlanetaryRepGroup");
 * repConfig.setNodeName("Mercury");
 * repConfig.setNodeHostPort("mercury.acme.com:5001");
 *
 * // This is the first node, so its helper is itself
 * repConfig.setHelperHosts("mercury.acme.com:5001");
 *
 * ReplicatedEnvironment repEnv =
 *     new ReplicatedEnvironment(envHome, repConfig, envConfig);
 * </pre>
 * <p>
 * To create a new node when there are <b>other existing group members</b>,
 * set a helper address which points to an existing node in the group. A simple
 * way to bring up a new group is to "chain" the new nodes by having the
 * helpers reference a previously created node.
 * <pre class="code">
 * EnvironmentConfig envConfig = new EnvironmentConfig();
 * envConfig.setAllowCreate(true);
 * envConfig.setTransactional(true);
 *
 * // Identify the node
 * ReplicationConfig repConfig =
 *     new ReplicationConfig("PlanetaryRepGroup",
 *                           "Jupiter",
 *                           "jupiter.acme.com:5002");
 *
 * // Use the node at mercury.acme.com:5001 as a helper to find the rest
 * // of the group.
 * repConfig.setHelperHosts("mercury.acme.com:5001");
 *
 * ReplicatedEnvironment repEnv =
 *     new ReplicatedEnvironment(envHome, repConfig, envConfig);
 * </pre>
 * <p>
 * In these examples, node Mercury was configured as its own helper, and
 * becomes the first master. The next nodes were configured to use Mercury as
 * their helper, and became replicas. It is also possible to start these in
 * reverse order, bringing mercury up last. In that case, the earlier nodes
 * will block until a helper is awake and can service their requests for group
 * metadata.
 * <p>
 * Creating a ReplicatedEnvironment for an <b>existing environment</b> requires
 * less configuration. The call
 * to {@code EnvironmentConfig.setAllowCreate()} is eliminated to guard
 * against the unintentional creation of a new environment. Also, there is no
 * need to set a helper host address, because the environment exists and has
 * access to the shared, persistent membership information.
 * <pre class="code">
 * EnvironmentConfig envConfig = new EnvironmentConfig();
 * envConfig.setTransactional(true);
 * ReplicationConfig repConfig =
 *     new ReplicationConfig("PlanetaryRepGroup",
 *                           "Mercury",
 *                           "mercury.acme.com:5001");
 *
 * ReplicatedEnvironment repEnv =
 *     new ReplicatedEnvironment(envHome, repConfig, envConfig);
 * </pre>
 * </p>
 * {@literal See} {@link com.sleepycat.je.rep.util.ReplicationGroupAdmin
 * ReplicationGroupAdmin} for information on how to remove nodes from the
 * replication group.
 *
 * <p>
 * ReplicatedEnvironment properties can be set via the the {@literal
 * <environmentHome>/}je.properties file, just like {@link Environment}
 * properties. They follow the same property value precedence rules.
 *
 * <p>
 * A replicated environment directory can only be accessed by a read write
 * ReplicatedEnvironment handle or a read only {@link Environment} handle.  In
 * the current release, there is an additional restriction that a read only
 * {@link Environment} is only permitted when the directory is not also
 * accessed from a different process by a read/write ReplicatedEnvironment. If
 * a read/write ReplicatedEnvironment and a read only {@link Environment} from
 * two different processes concurrently access an environment directory, there
 * is the small possibility that the read only {@link Environment} may see
 * see exceptions thrown about an inconsistent log if the ReplicatedEnvironment
 * executes certain kinds of failover. There is no problem if the {@link
 * Environment} and ReplicatedEnvironment are in the same process, or are not
 * concurrent.
 * <p>
 * JE HA prohibits opening a replicated environment directory with a read/write
 * {@link Environment} handle, because from the group's perspective,
 * unreplicated updates to a single node would cause data inconsistency.  To
 * use an existing, non-replicated environment to bootstrap a replication
 * group, use {@link com.sleepycat.je.rep.util.DbEnableReplication} to do a one
 * time conversion of the directory.
 * <p>
 * All other database objects, such as {@link com.sleepycat.je.Database} or
 * {@link com.sleepycat.je.Cursor} (when using the Base API) or {@link
 * com.sleepycat.persist.EntityStore} or {@link
 * com.sleepycat.persist.PrimaryIndex} (when using the Direct Persistence
 * Layer) should be created, used and closed before calling {@link
 * ReplicatedEnvironment#close}.
 *
 * <p>Replicated environments can be created with node type {@link
 * NodeType#ELECTABLE} or {@link NodeType#SECONDARY}. ELECTABLE nodes can be
 * masters or replicas, and participate in both master elections and commit
 * durability decisions.
 *
 * <p>SECONDARY nodes can only be replicas, not masters, and do not participate
 * in either elections or durability decisions.  SECONDARY nodes can be used to
 * increase the available number of read replicas without changing the election
 * or durability quorum of the group, and without requiring communication with
 * the secondaries during master elections or transaction commits. As a result,
 * SECONDARY nodes are a good choice for nodes that are connected to the other
 * nodes in the group by high latency network connections, for example over
 * long distance networks.  SECONDARY nodes maintain replication streams with
 * the replication group master to update the data contents of their
 * environment.
 *
 * <p>You can use SECONDARY nodes to:
 * <ul>
 * <li>Provide a copy of the data available at a distant location
 * <li>Maintain an extra copy of the data to increase redundancy
 * <li>Change the number of replicas to adjust to dynamically changing read
 *     loads
 * </ul>
 *
 * <p>Membership information for SECONDARY nodes is not stored persistently, so
 * their membership is only known to the master, and only while the nodes
 * remain connected to the master.  Because a SECONDARY node cannot become a
 * master, it will not act as master even if it is the first node created for
 * the group.
 *
 * <h3><a name="nonRepDbs">Non-replicated Databases in a Replicated
 * Environment</a></h3>
 *
 * A database or entity store in a replicated environment is replicated by
 * default, but may be explicitly configured as non-replicated using
 * {@link com.sleepycat.je.DatabaseConfig#setReplicated} or
 * {@link com.sleepycat.persist.StoreConfig#setReplicated}.  Such
 * non-replicated databases may be transactional or non-transactional
 * (including deferred-write and temporary).  The special considerations for
 * using non-replicated databases in a replicated environment are described
 * below.
 * <p>
 * The data in a non-replicated database is not guaranteed to be persistent,
 * for two reasons.
 * <ul>
 * <li>
 * When a hard recovery occurs as part of an election, some data at the end of
 * the transaction log may be lost.  For a replicated database this data is
 * automatically recovered from other members of the group, but for a
 * non-replicated database it is not.
 * </li>
 * <li>
 * When a node's contents are replaced via network restore or by otherwise
 * copying the transaction log from another node, all previously existing
 * non-replicated databases on that node are destroyed, and the non-replicated
 * databases from the source node are copied along with the replicated
 * data.  The non-replicated databases copied from the source node will be in
 * whatever state they were in at the time of the copy.
 * </li>
 * </ul>
 * <p>
 * Therefore, non-replicated databases are intended to be used primarily for
 * persistent caching and other non-critical local storage.  The application
 * is responsible for maintaining the state of the database and handling data
 * loss after one the events described above.
 * <p>
 * To perform write operations on a non-replicated database, special
 * considerations are necessary for user-supplied transactions.  Namely, the
 * transaction must be configured for
 * {@link com.sleepycat.je.TransactionConfig#setLocalWrite(boolean)
 * local-write}.  A given transaction may be used to write to either replicated
 * databases or non-replicated databases, but not both.
 * <p>
 * For auto-commit transactions (when the Transaction parameter is null), the
 * local-write setting is automatically set to correspond to whether the
 * database is replicated. With auto-commit, local-write is always true for a
 * non-replicated database, and always false for a replicated database.
 * <p>
 * A local-write transaction automatically uses
 * {@link com.sleepycat.je.Durability.ReplicaAckPolicy#NONE}.
 * A local-write transaction on a Master will thus not be held up, or
 * throw {@link com.sleepycat.je.rep.InsufficientReplicasException}, if the
 * Master is not in contact with a sufficient number of Replicas at the
 * time the transaction is initiated.
 * <p>
 * For read operations, a single transaction may be used to read any
 * combination of replicated and non-replicated databases. If only read
 * operations are performed, it is normally desirable to configure a user
 * supplied transaction as
 * {@link com.sleepycat.je.TransactionConfig#setReadOnly(boolean) read-only}.
 * Like a local-write transaction, a read-only transaction automatically uses
 * {@link com.sleepycat.je.Durability.ReplicaAckPolicy#NONE}.
 * <p>
 * For user-supplied transactions, note that even when accessing only
 * non-replicated databases, group consistency checks <em>are</em> performed by
 * default. In this case it is normally desirable to disable consistency
 * checks by calling
 * {@link com.sleepycat.je.TransactionConfig#setConsistencyPolicy} with
 * {@link NoConsistencyRequiredPolicy#NO_CONSISTENCY}.  This allows the
 * non-replicated databases to be accessed regardless of the state of the other
 * members of the group and the network connections to them.  When auto-commit
 * is used (when the Transaction parameter is null) with a non-replicated
 * database, consistency checks are automatically disabled.
 *
 * @see Environment
 * @see <a href="{@docRoot}/../ReplicationGuide/progoverview.html"
 * target="_top">Replication First Steps</a>
 * @since 4.0
 */
// ReplicatedEnvironment 是 Berkeley DB Java Edition (JE) 高可用/复制（High Availability, HA）模块中的核心类。它继承自普通单机环境类 Environment，代表复制组（Replication Group）中的一个独立节点。
// 分布式高可用节点封装：将一个本地的数据库目录包装为一个分布式复制组中的节点（可以作为 Master 主节点、Electable 可选举 Replica 副本，或 Secondary 只读副本）。
// 自动引导与加群（Join Group）：当实例化 ReplicatedEnvironment 时，该节点会自动与组内的其他成员（通过 Helper Hosts 辅助节点）建立联系、参与选举或同步数据。构造函数返回时，该节点已经处于可用状态。
// 接口与操作复用：完全继承自 Environment，因此所有的数据库操作（如打开数据库、事务读写、游标操作等）与单机应用完全一致。但它要求所有涉及的复制数据库必须是事务型的（Transactional）。
// 支持非复制数据库：允许在复制环境中创建非复制数据库（Non-replicated Databases），用于本地持久化缓存等非关键场景。
public class ReplicatedEnvironment extends Environment {

    /*
     * The canonical RepImpl associated with the environment directory,
     * accessed by different handles.
     *
     * The repEnvironmentImpl field is set to null during close to avoid OOME.
     * It should normally only be accessed via the checkOpen (which calls
     * Environment.checkOpen) and getNonNullRepImpl methods. During close, while
     * synchronized, it is safe to access it directly.
     */
    // 指向专门用于高可用复制的底层实现类（RepImpl）。
    // 是 EnvironmentImpl 的复制扩展子类，管理着复制节点的心跳、网络 Feeder、选举算法等底层逻辑。在调用 close() 时会被置为 null 以防止内存泄漏。
    private volatile RepImpl repEnvironmentImpl;

    /* The unique name and id associated with the node. */
    // 存储当前节点的唯一名称（Name）和内部 ID（Id）对，用于在复制网络通信中标识身份。
    private final NameIdPair nameIdPair;

    /*
     * The replication configuration that has been used to create this
     * handle. This is derived from the original configuration argument, after
     * cloning a copy to keep it distinct from the user's instance, applying
     * je.properties settings, and validating against the underlying node.
     */
    // 保存当前句柄所使用的复制配置（ReplicationConfig）副本（由用户传入的配置经过克隆、合并 je.properties 属性以及校验后生成）。
    private ReplicationConfig handleRepConfig;

    /**
     * Creates a replicated environment handle and starts participating in the
     * replication group as either a Master or a Replica. The node's state is
     * determined when it joins the group, and mastership is not preconfigured.
     * If the group has no current master and the node has the default node
     * type of {@link NodeType#ELECTABLE}, then creation of a handle will
     * trigger an election to determine whether this node will participate as a
     * Master or a Replica.
     * <p>
     * If the node participates as a Master, the constructor will return after
     * a sufficient number of Replicas, in accordance with the
     * {@code initialElectionPolicy} argument, have established contact with
     * the Master.
     * <p>
     * If the node participates as a Replica, it will become consistent in
     * accordance with the {@code consistencyPolicy} argument before returning
     * from the constructor.
     * <p>
     * If an election cannot be concluded in the time period defined by {@link
     * ReplicationConfig#ENV_SETUP_TIMEOUT}, by default it will throw an {@code
     * UnknownMasterException}. This behavior can be overridden via the {@link
     * ReplicationConfig#ENV_UNKNOWN_STATE_TIMEOUT} to permit the creation of
     * the handle in the {@link State#UNKNOWN} state. A handle in UNKNOWN state
     * can be used to service read operations with an appropriately relaxed
     * consistency policy. Note that these timeouts do not apply when opening
     * an environment for the very first time. In the first time case, if the
     * node is not the only group member, or if it is a SECONDARY node, the
     * constructor will wait indefinitely until it can contact an existing
     * group member.
     * <p>
     * A brand new node will always join an existing group as a Replica, unless
     * it is the very first electable node that is creating the group. In that
     * case it joins as the Master of the newly formed singleton group. A brand
     * new node must always specify one or more active helper nodes via the
     * {@link ReplicationConfig#setHelperHosts(String)} method, or via the
     * <code>&lt;environment home&gt;/je.properties</code> file. If this is the
     * very first member of a nascent group, it must specify just itself as the
     * helper.
     * <p>
     * There are special considerations to keep in mind when a replication
     * group is started and elections are first held to determine a master. The
     * default {@link com.sleepycat.je.rep.QuorumPolicy#SIMPLE_MAJORITY} calls
     * for a simple majority vote. If the group members were previously created
     * and populated, the default election policy may result in the election of
     * a master that may not have the most up to date copy of the environment.
     * This could happen if the best qualified node is slow to start up; it's
     * possible that by the time it's ready to participate in an election, the
     * election has already have completed with a simple majority.
     * <p>
     * To avoid this possibility, the method has a parameter
     * initialElectionPolicy, which can be used to specify
     * {@link com.sleepycat.je.rep.QuorumPolicy#ALL}, which will cause the
     * elections to wait until all electable nodes can vote. By ensuring that
     * all the nodes can vote, the best possible node is chosen to be the
     * master at group startup.
     * <p>
     * Note that it is the application's responsibility to ensure that all
     * electable nodes coordinate their choice of initialElectionPolicy so that
     * the very first elections held when a group is brought up use the same
     * value for this parameter. This parameter is only used for the first
     * election.  After the first election has been held and the group is
     * functioning, subsequent elections do not require participation of all
     * the nodes. A simple majority is sufficient to elect the node with the
     * most up to date environment as the master.
     * <p>
     *
     * @param envHome The environment's home directory.
     *
     * @param repConfig replication configurations. If null, the default
     * replication configurations are used.
     *
     * @param envConfig environment configurations for this node. If null, the
     * default environment configurations are used.
     *
     * @param consistencyPolicy the consistencyPolicy used by the Replica at
     * startup to make its environment current with respect to the master. This
     * differs from the consistency policy specified
     * {@link ReplicationConfig#setConsistencyPolicy} because it is used only
     * at construction, when the node joins the group for the first time. The
     * consistency policy set in {@link ReplicationConfig} is used any time a
     * policy is used after node startup, such as at transaction begins.
     *
     * @param initialElectionPolicy the policy to use when holding the initial
     * election.
     *
     * @throws RestartRequiredException if some type of corrective action is
     * required. The subclasses of this exception provide further details.
     *
     * @throws ReplicaConsistencyException if it is a Replica and cannot
     * satisfy the specified consistency policy within the consistency timeout
     * period
     *
     * @throws UnknownMasterException if the
     * {@link ReplicationConfig#ENV_UNKNOWN_STATE_TIMEOUT} has a zero value and
     * the node cannot join the group in the time period specified by the
     * {@link ReplicationConfig#ENV_SETUP_TIMEOUT} property. The node may be
     * unable to join the group because the Master could not be determined due
     * to a lack of sufficient nodes as required by the election policy, or
     * because a master was present but lacked a
     * {@link QuorumPolicy#SIMPLE_MAJORITY} needed to update the environment
     * with information about this node, if it's a new node and is joining the
     * group for the first time.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws EnvironmentLockedException when an environment cannot be opened
     * for write access because another process has the same environment open
     * for write access. <strong>Warning:</strong> This exception should be
     * handled when an environment is opened by more than one process.
     *
     * @throws VersionMismatchException when the existing log is not compatible
     * with the version of JE that is running. This occurs when a later version
     * of JE was used to create the log. <strong>Warning:</strong> This
     * exception should be handled when more than one version of JE may be used
     * to access an environment.
     *
     * @throws UnsupportedOperationException if the environment exists and has
     * not been enabled for replication.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code EnvironmentConfig} parameter.
     */
    public ReplicatedEnvironment(File envHome,
                                 ReplicationConfig repConfig,
                                 EnvironmentConfig envConfig,
                                 ReplicaConsistencyPolicy consistencyPolicy,
                                 QuorumPolicy initialElectionPolicy)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException,
               InsufficientLogException,
               ReplicaConsistencyException,
               IllegalArgumentException {

        this(envHome,
             repConfig,
             envConfig,
             consistencyPolicy,
             initialElectionPolicy,
             true /*joinGroup*/,
             null /*envImplParam*/);
    }

    /**
     * A convenience constructor that defaults the replica consistency policy
     * and the initial election policy to be used.
     *
     * <p>
     * The default replica consistency policy results in the replica being
     * consistent with the master as of the time the handle was created.
     * </p>
     *
     * <p>
     * The default initial election policy is
     * {@link QuorumPolicy#SIMPLE_MAJORITY}
     * </p>
     *
     * @throws RestartRequiredException if some type of corrective action is
     * required. The subclasses of this exception provide further details.
     *
     * @throws ReplicaConsistencyException if it is a Replica and and cannot be
     * made consistent within the timeout specified by
     * {@link ReplicationConfig#ENV_CONSISTENCY_TIMEOUT}
     *
     * @throws UnknownMasterException if the
     * {@link ReplicationConfig#ENV_UNKNOWN_STATE_TIMEOUT} has a zero value and
     * the node cannot join the group in the time period specified by the
     * {@link ReplicationConfig#ENV_SETUP_TIMEOUT} property. The node may be
     * unable to join the group because the Master could not be determined due
     * to a lack of sufficient nodes as required by the election policy, or
     * because a master was present but lacked a
     * {@link QuorumPolicy#SIMPLE_MAJORITY} needed to update the environment
     * with information about this node, if it's a new node and is joining the
     * group for the first time.
     *
     * @throws EnvironmentLockedException when an environment cannot be opened
     * for write access because another process has the same environment open
     * for write access. <strong>Warning:</strong> This exception should be
     * handled when an environment is opened by more than one process.
     *
     * @throws VersionMismatchException when the existing log is not compatible
     * with the version of JE that is running. This occurs when a later version
     * of JE was used to create the log. <strong>Warning:</strong> This
     * exception should be handled when more than one version of JE may be used
     * to access an environment.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the environment exists and has
     * not been enabled for replication.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code EnvironmentConfig} parameter.
     *
     * @see #ReplicatedEnvironment(File, ReplicationConfig, EnvironmentConfig,
     * ReplicaConsistencyPolicy, QuorumPolicy)
     */
    public ReplicatedEnvironment(File envHome,
                                 ReplicationConfig repConfig,
                                 EnvironmentConfig envConfig)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException,
               ReplicaConsistencyException,
               InsufficientLogException,
               RollbackException,
               IllegalArgumentException {

        this(envHome, repConfig, envConfig, null /*consistencyPolicy*/,
             QuorumPolicy.SIMPLE_MAJORITY);
    }

    /*
     * Joins the replication group as part of the creation of a handle.
     */
    // RepImpl repImpl 代表底层复制环境的实现对象（Replication Implementation）。它是 BDB JE 复制机制的核心控制中心，封装了节点状态机、网络通信、选举、日志同步等所有底层逻辑。
    // ReplicaConsistencyPolicy consistencyPolicy  副本一致性策略。用于指定当本节点作为 Replica（副本）时，在允许应用程序开始读取数据之前，本地数据必须与 Master（主节点）同步到什么程度（例如允许的最大事务滞后时间或滞后 LSN 范围）
    // QuorumPolicy initialElectionPolicy 初始选举策略。决定了节点在启动加入复制组、进行主节点选举（Master Election）时所需要的最少投票参与人数（如前面分析过的 SIMPLE_MAJORITY 或 ALL）。
    private void joinGroup(RepImpl repImpl,
                           ReplicaConsistencyPolicy consistencyPolicy,
                           QuorumPolicy initialElectionPolicy)
        throws DatabaseException {

        /* Just return if we don't want to join the group. */
        // 判断当前环境配置是否明确指示“不要加入复制组”
        if (dontJoinGroup()) {
            return;
        }

        State state = null;
        try {
            state =
                repImpl.joinGroup(consistencyPolicy, initialElectionPolicy);
        } finally {
            if (state == null) {

                /*
                 * Something bad happened, close the environment down with
                 * minimal activity. The environment may not actually be
                 * invalidated, but the constructor did not succeed, so it's
                 * logically invalid. We don't go to the effort of invalidating
                 * the environment, to avoid masking the original problem. Use
                 * abnormalClose() because it will remove the
                 * environment from the environment pool.
                 */
                repImpl.abnormalClose();
            }
        }
    }

    /* Return true if this node won't join the group. */

    // 返回该节点是否配置加入复制组，由je.rep.dontJoinRepGroup控制
    private boolean dontJoinGroup() {
        return new Boolean(getRepConfig().getConfigParam
                (RepParams.DONT_JOIN_REP_GROUP.getName()));
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Note that repImpl.joinGroup is a synchronized
     * method, and therefore protected against multiple concurrent attempts to
     * create a handle.
     *
     * @param envImplParam is non-null only when used by EnvironmentIml to
     * create an InternalEnvironment.
     */
    // Berkeley DB JE（Java Edition）分布式高可用/复制（Replication）模块中核心类 ReplicatedEnvironment 的构造函数
    // 主要作用是初始化复制环境、加入复制组（Replication Group）、处理同步冲突（如回滚异常 RollbackException）以及触发相关的监控事件。
    // File envHome  数据库环境的主目录（文件夹路径），存储了数据库文件、日志文件（JE Log）及复制相关的元数据。
    // ReplicationConfig repConfig  复制专用的配置对象。包含当前节点的唯一标识（Node Name）、复制组名称、群组中其他节点的地址列表（Helper Nodes）、超时时间等网络和复制行为配置。
    // ReplicaConsistencyPolicy consistencyPolicy  副本一致性策略。用于规定当一个 Replica 节点加入复制组并准备好接受读操作时，它必须落后主节点（Master）多少（例如在时间或事务数上）才算“一致”，从而决定当前节点何时可以对应用层提供服务。
    // QuorumPolicy initialElectionPolicy 初始选举策略/法定人数策略（仲裁策略）。规定了在进行选举（Election）时，需要多少个节点参与才能达成法定共识（Quorum），以选出新的主节点。
    // boolean joinGroup  控制标志。指示在创建环境句柄时是否自动尝试加入复制组。通常在正常使用时为 true；某些测试场景下可能为 false。
    // RepImpl envImplParam  仅在由 EnvironmentImpl 派生创建内部环境时使用。平时通常为 null，由父类构造函数进行底层环境初始化。
    protected ReplicatedEnvironment(File envHome,
                                    ReplicationConfig repConfig,
                                    EnvironmentConfig envConfig,
                                    ReplicaConsistencyPolicy consistencyPolicy,
                                    QuorumPolicy initialElectionPolicy,
                                    boolean joinGroup,
                                    RepImpl envImplParam)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException,
               ReplicaConsistencyException,
               IllegalArgumentException {

        super(envHome, envConfig, repConfig, envImplParam);
        // 从刚刚创建的实例中提取非空的底层环境实现对象（EnvironmentImpl）
        repEnvironmentImpl = (RepImpl) DbInternal.getNonNullEnvImpl(this);
        // 获取当前节点的名称与唯一 ID 组合标识（NameIdPair），用于内部网络寻址和日志记录。
        nameIdPair = repEnvironmentImpl.getNameIdPair();

        /*
         * Ensure that the DataChannelFactory configuration is usable
         * and initialize logging state.
         */
        // 配置并检查底层的网络通信组件（DataChannelFactory），确保配置有效，并初始化该节点用于复制通信的日志记录状态。
        repEnvironmentImpl.initializeChannelFactory();
        // 判断是否需要加入复制组
        if (joinGroup) {

            try {
                // 尝试加入复制组（核心逻辑）
                joinGroup(
                    repEnvironmentImpl, consistencyPolicy,
                    initialElectionPolicy);

            } catch (RollbackException e) {

                /*
                 * Syncup failed, a hard recovery is needed. Throwing the
                 * RollbackException closed the RepImpl and the EnvironmentImpl
                 * Redo the creation of RepImpl and retry the join once. If the
                 * second joinGroup fails, let the exception throw out to the
                 * user.
                 *
                 * Clear references to the old envImpl/repImpl, to prevent OOME
                 * during recovery when we retry below.
                 */
                // 应对回滚异常：清理旧环境引用
                DbInternal.clearEnvImpl(this);
                // 显式清除了对旧实例的引用。
                repEnvironmentImpl = null;
                // 重新创建环境实现并重试加入
                repEnvironmentImpl = (RepImpl) makeEnvironmentImpl(
                    envHome, envConfig, repConfig);

                /*
                 * Ensure that the DataChannelFactory configuration is usable
                 * and initialize logging state.
                 */
                // 重新初始化网络通道。
                repEnvironmentImpl.initializeChannelFactory();
                // 重试加入复制组（硬恢复之后的第二次尝试）。如果这次再次失败，异常将不再捕获，直接向上抛给用户。
                joinGroup(
                    repEnvironmentImpl, consistencyPolicy,
                    initialElectionPolicy);
                // 记录这次硬恢复的相关异常上下文信息。
                repEnvironmentImpl.setHardRecoveryInfo(e);
            }

            /*
             * Fire a JoinGroupEvent only when the ReplicatedEnvironment is
             * successfully created for the first time.
             */
            // 检查是否存在有效的复制节点（RepNode），若存在则通过监控事件管理器触发 notifyJoinGroup() 事件，通知监听器（Listeners）当前节点已成功加入复制组。
            if (repEnvironmentImpl.getRepNode() != null) {
                repEnvironmentImpl.getRepNode().
                    getMonitorEventManager().notifyJoinGroup();
            }
        } else {
            // else 分支：测试专用校验
            /* For testing only */
            if (repEnvironmentImpl.getRepNode() != null) {
                throw EnvironmentFailureException.unexpectedState
                    ("An earlier handle creation had resulted in the node" +
                     "joining the group");
            }
        }
    }

    /**
     * @hidden
     * For internal use only.
     *
     * Validate and resolve replication configuration params, and extract a
     * ReplicationConfig with those params for passing into environment
     * creation. Note that a copy of the ReplicationConfig argument is created
     * to insulate the application from changes made by the replication
     * implementation and vice versa.
     */
    @Override
    protected RepConfigProxy setupRepConfig(File envHome,
                                            RepConfigProxy repConfigProxy,
                                            EnvironmentConfig envConfig) {

        /**
         * If the user specified a null object, use the default. Apply the
         * je.properties file to the replication config object.
         */
        ReplicationConfig repConfig = (ReplicationConfig) repConfigProxy;
        ReplicationConfig baseConfig =
            (repConfig == null) ? ReplicationConfig.DEFAULT : repConfig;
        ReplicationConfig useConfig = baseConfig.clone();

        if (envConfig.getReadOnly()) {

            /*
             * Read-only replicated environments are not usually permitted,
             * since a RN should be able to assume master identity
             * at any moment. ReadOnly is only supported if the node is an
             * arbiter, subscriber or network backup.
             *
             * TBW: the arbiter, subscriber, and a network backup all need a
             * replicated environment handle that has pieces of the env
             * infrastructure, like info logging, service dispatching, log file
             * management. The user of XXX_USE parameters is really selecting
             * those infrastructure pieces in an implicit way. It would be nice
             * to have a way to specify which services they use in a more
             * explicit way. To do so, we probably need to do a bit of
             * refactoring of the env handle to call out those components.
             */
            boolean arbUse = useConfig.getConfigParam(
                RepParams.ARBITER_USE.getName()).equals("true");
            boolean subUse = useConfig.getConfigParam(
                    RepParams.SUBSCRIBER_USE.getName()).equals("true");
            boolean networkBackupUse = useConfig.getConfigParam(
                    RepParams.NETWORKBACKUP_USE.getName()).equals("true");

            if (!arbUse && !subUse && !networkBackupUse) {
                throw new IllegalArgumentException("A replicated environment " +
                        "may not be opened read-only");
            }
        }
        DbConfigManager.applyFileConfig(envHome,
                                        useConfig.getProps(),
                                        true); /* forReplication */
        useConfig.propagateRepNetProps();
        this.handleRepConfig = useConfig;
        return handleRepConfig;
    }

    /**
     * Returns the unique name used to identify this replicated environment.
     * @see ReplicationConfig#setNodeName
     *
     * @return the node name
     */
    public String getNodeName() {
        return nameIdPair.getName();
    }

    /**
     * Returns the current state of the node associated with this replication
     * environment. See {@link State} for a description of node states.
     * <p>
     * If the caller's intent is to track the state of the node,
     * {@link StateChangeListener} may be a more convenient and efficient
     * approach, rather than using getState() directly.
     *
     * @return the current replication state associated with this node
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    public State getState()
        throws DatabaseException {

        final RepImpl repImpl = checkOpen();

        try {
            return repImpl.getState();
        } catch (Error E) {
            repImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns a description of the replication group as known by this node.
     * The replicated group metadata is stored in a replicated database and
     * updates are propagated by the current master node to all replicas. If
     * this node is not the master, it is possible for its description of the
     * group to be out of date, and it will not include information about
     * SECONDARY nodes.
     *
     * @return the group description
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public ReplicationGroup getGroup()
        throws DatabaseException {

        final RepImpl repImpl = checkOpen();

        try {
            return new ReplicationGroup(repImpl.getRepNode().getGroup());
        } catch (Error E) {
            repImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Close this ReplicatedEnvironment and release any resources used by the
     * handle.
     *
     * <p>
     * When the last handle is closed, allocated resources are freed, and
     * daemon threads are stopped, even if they are performing work. The node
     * ceases participation in the replication group. If the node was currently
     * the master, the rest of the group will hold an election. If a quorum of
     * nodes can participate in the election, a new master will be chosen.
     * <p>
     * The ReplicatedEnvironment should not be closed while any other type of
     * handle that refers to it is not yet closed. For example, the
     * ReplicatedEnvironment should not be closed while there are open Database
     * instances, or while transactions in the environment have not yet
     * committed or aborted. Specifically, this includes {@link
     * com.sleepycat.je.Database Database}, {@link com.sleepycat.je.Cursor
     * Cursor} and {@link com.sleepycat.je.Transaction Transaction} handles.
     * </p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     */
    @Override
    synchronized public void close()
        throws DatabaseException {

        try {
            super.close();
        } catch (DatabaseException e) {
            /* Add this node's address to the exception message for clarity. */
            e.addErrorMessage("Problem closing handle " + nameIdPair);
            throw e;
        } catch (Exception e) {
            /* Add this node's address to the exception message for clarity. */
            throw new EnvironmentFailureException(
                repEnvironmentImpl,
                EnvironmentFailureReason.UNEXPECTED_EXCEPTION,
                "Problem closing handle " + nameIdPair, e);
        } finally {
            repEnvironmentImpl = null;
        }
    }

    /**
     * Sets the listener used to receive asynchronous replication node state
     * change events. Note that there is one listener per replication node, not
     * one per handle. Invoking this method replaces the previous Listener.
     *
     * Invoking this method typically results in an immediate callback to the
     * application via the {@link StateChangeListener#stateChange} method, so
     * that the application is made aware of the existing state of the
     * node at the time <code>StateChangeListener</code> is first established.
     *
     * @param listener the state change listener.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    public void setStateChangeListener(StateChangeListener listener)
        throws DatabaseException {

        final RepImpl repImpl = checkOpen();

        try {
            repImpl.setChangeListener(listener);
        } catch (Error E) {
            repImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the listener used to receive asynchronous replication node state
     * change events. A StateChangeListener provides the replication
     * application with an asynchronous mechanism for tracking the {@link
     * ReplicatedEnvironment.State State} of the replicated environment.
     * <p>
     * Note that there is one listener per replication node, not one per
     * ReplicatedEnvironment handle.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    public StateChangeListener getStateChangeListener()
        throws DatabaseException {

        final RepImpl repImpl = checkOpen();

        try {
            return repImpl.getChangeListener();
        } catch (Error E) {
            repImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    public void setRepMutableConfig(ReplicationMutableConfig mutableConfig)
        throws DatabaseException {

        final RepImpl repImpl = checkOpen();

        DatabaseUtil.checkForNullParam(mutableConfig, "mutableConfig");

        try {
            repImpl.setRepMutableConfig(mutableConfig);
        } catch (Error E) {
            repImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    public ReplicationMutableConfig getRepMutableConfig()
        throws DatabaseException {

        final RepImpl repImpl = checkOpen();

        try {
            final ReplicationMutableConfig config =
                repImpl.cloneRepMutableConfig();
            config.fillInEnvironmentGeneratedProps(repImpl);
            return config;
        } catch (Error E) {
            repImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Return the replication configuration that has been used to create this
     * handle. This is derived from the original configuration argument, after
     * cloning a copy to keep it distinct from the user's instance, applying
     * je.properties settings, and validating against the underlying
     * node.
     *
     * @return this handle's configuration.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    public ReplicationConfig getRepConfig()
        throws DatabaseException {

        checkOpen();
        // 返回环境的配置
        return handleRepConfig;
    }

    /**
     * Returns statistics associated with this environment. See {@link
     * ReplicatedEnvironmentStats} for the kind of information available.
     *
     * @param config is used to specify attributes such as whether the stats
     * should be cleared, whether the complete set of stats should be obtained,
     * etc.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    public ReplicatedEnvironmentStats getRepStats(StatsConfig config)
        throws DatabaseException {

        final RepImpl repImpl = checkOpen();

        if (config == null) {
            config = StatsConfig.DEFAULT;
        }

        return repImpl.getStats(config);
    }

    /*
     * Returns the non-null, underlying RepImpl. For internal access only.
     * Intentionally non-public; non package access must use the RepInternal
     * proxy.
     *
     * This method is used to access the repEnvironmentImpl field, to guard
     * against NPE when the environment has been closed.
     *
     * This method does not check whether the env is valid. For API method
     * calls, checkOpen is called at API entry points to check validity. The
     * validity of the env should also be checked before critical operations
     * (e.g., disk writes), after idle periods, and periodically during time
     * consuming operations.
     *
     * @throws IllegalStateException if the env has been closed.
     */
    RepImpl getNonNullRepImpl() {

        final RepImpl repImpl = repEnvironmentImpl;

        if (repImpl == null) {
            throw new IllegalStateException("Environment is closed.");
        }

        return repImpl;
    }

    /**
     * Returns the underlying RepImpl, or null if the env has been closed.
     *
     * WARNING: This method will be phased out over time and normally
     * getNonNullRepImpl should be called instead.
     */
    RepImpl getMaybeNullRepImpl() {
        return repEnvironmentImpl;
    }

    /**
     * @throws EnvironmentFailureException if the underlying environment is
     * invalid.
     * @throws IllegalStateException if the environment is not open.
     */
    // 核心作用是在执行复制（Replication）相关操作前，校验当前复制环境是否处于正常开启和可用状态
    private RepImpl checkOpen() {
        // 如果数据库环境不正常，就抛出异常
        DbInternal.checkOpen(this);

        /*
         * Will throw ISE if the environment becomes closed or invalid after
         * the above check.
         */
        //返回环境的实现
        return getNonNullRepImpl();
    }

    /**
     * Print a detailed report about the costs of different phases of
     * environment startup. This report is by default logged to the je.info
     * file if startup takes longer than je.env.startupThreshold.
     */
    @Override
    public void printStartupInfo(PrintStream out) {

        super.printStartupInfo(out);

        getNonNullRepImpl().getStartupTracker().displayStats(
            out, Phase.TOTAL_JOIN_GROUP);
    }

    /**
     * The replication node state determines the operations that the
     * application can perform against its replicated environment.
     * The method {@link #getState} returns the current state.
     * <p>
     * When the first handle to a {@link ReplicatedEnvironment} is instantiated
     * and the node is bought up, the node usually establishes
     * <code>MASTER</code> or <code>REPLICA</code> state before returning from
     * the constructor.  However, these states are actually preceeded by the
     * <code>UNKNOWN</code> state, which may be visible if the application has
     * configured a suitable {@link
     * ReplicationConfig#ENV_UNKNOWN_STATE_TIMEOUT}.
     * <p>
     * As the various remote nodes in the group become unavailable and
     * elections are held, the local node may change between
     * <code>MASTER</code> and <code>REPLICA</code> states, always with a
     * (usually brief) transition through <code>UNKNOWN</code> state.
     * <p>
     * When the last handle to the environment is closed, the node transitions
     * to the <code>DETACHED</code> state.
     * <p>
     * The state transitions visible to the application can be summarized by
     * the regular expression:
     * <blockquote>
     * <code> [ MASTER | REPLICA | UNKNOWN ]+ DETACHED</code>
     * </blockquote>
     * with the caveat that redundant "transitions" (<code>MASTER</code> to
     * <code>MASTER</code>, <code>REPLICA</code> to <code>REPLICA</code>, etc.)
     * never occur.
     */
    // 定义复制节点的生命周期状态：State 枚举决定了应用程序当前可以对复制环境（ReplicatedEnvironment）执行哪些操作（如读或写）
    // 控制应用行为分流：通过该状态，应用程序可以准确知道当前节点是处于主节点（MASTER）、副本节点（REPLICA）、过渡状态（UNKNOWN）还是断开状态（DETACHED），从而决定是将写请求路由到本地还是转发给 Master，或者暂停写操作。
    public static enum State {

        /**
         * The node is not associated with the group. Its handle has been
         * closed. No operations can be performed on the environment when it is
         * in this state.
         */
        // 表示节点未与复制组关联，其环境句柄已被关闭。
        // 当处于此状态时，无法在环境中执行任何数据库操作。
        DETACHED,

        /**
         * The node is not currently in contact with the master, but is actively
         * trying to establish contact with, or decide upon, a master. While in
         * this state the node is restricted to performing just read operations
         * on its environment. In a functioning group, this state is
         * transitory.
         */
        // 表示节点当前未与主节点取得联系，但正在积极尝试连接或参与选举以决定主节点。
        // 在此状态下，节点仅被限制执行读操作。在正常运行的复制组中，这通常是一个短暂的过渡状态。
        UNKNOWN,

        /**
         * The node is the unique master of the group and can both read and
         * write to its environment. When the node transitions to the
         * state, the application running on the node must make provisions to
         * start processing application level write requests in addition to
         * read requests.
         */
        // 表示当前节点是复制组中唯一的主节点。
        MASTER,

        /**
         * The node is a replica that is being updated by the master. It is
         * restricted to reading its environment. When the node
         * transitions to this state, the application running on the node must
         * make provisions to ensure that it does not write to the
         * environment. It must arrange for all write requests to be routed to
         * the master.
         */
        // 表示当前节点是正在由主节点进行更新的副本节点
        REPLICA;

        /**
         * @return true if the node is a Master when in this state
         */
        final public boolean isMaster() {
            return this == MASTER;
        }

        /**
         * @return true if the node is a Replica when in this state
         */
        final public boolean isReplica() {
            return this == REPLICA;
        }

        /**
         * @return true if the node is disconnected from the replication
         * group when in this state.
         */
        final public boolean isDetached() {
            return this == DETACHED;
        }

        /**
         * @return true if the node's state is unknown, and it is attempting
         * to transition to Master or Replica.
         */
        final public boolean isUnknown() {
            return this == UNKNOWN;
        }

        /**
         * @return true if the node is currently participating in the group as
         * a Replica or a Master
         */
        final public boolean isActive() {
            return (this == MASTER) || (this == REPLICA);
        }
    }

    /**
     * Closes this handle and shuts down the Replication Group by forcing all
     * active Replicas to exit.
     * <p>
     * This method must be invoked on the node that's currently the Master
     * after all other outstanding handles have been closed.
     * <p>
     * The Master waits for all active Replicas to catch up so that they have a
     * current set of logs, and then shuts them down. The Master will wait for
     * a maximum of <code>replicaShutdownTimeout</code> for a Replica to catch
     * up. If the Replica has not caught up in this time period it will force
     * the Replica to shut down before it is completely caught up. A negative
     * or zero <code>replicaShutdownTimeout</code> value will result in an
     * immediate shutdown without waiting for lagging Replicas to catch up.
     * Nodes that are currently inactive cannot be contacted by the Master, as
     * a consequence, their state is not impacted by the shutdown.
     * <p>
     * The shutdown operation will close this handle on the Master node. The
     * environments on Replica nodes will be invalidated, and attempts to use
     * those handles will result in a {@link GroupShutdownException} being
     * thrown. The application is responsible for closing the remaining handles
     * on the Replica.
     *
     * @param replicaShutdownTimeout the maximum amount of time the Master
     * waits for a Replica to shutdown.
     *
     * @param unit the time unit associated with the
     * <code>replicaShutdownTimeout</code>
     *
     * @throws IllegalStateException if the method is invoked on a node that's
     * not currently the Master, or there are other open handles to this
     * Environment.
     */
    public synchronized void shutdownGroup(long replicaShutdownTimeout,
                                           TimeUnit unit)
        throws IllegalStateException {

        final RepImpl repImpl = checkOpen();

        /*
         * Hold repImpl stable, across the setup and close. Note that close()
         * synchronizes on DbEnvPool, and synchronization order must be
         * DbEnvPool before repImpl/envImpl.
         */
        synchronized (DbEnvPool.getInstance()) {
            synchronized (repImpl) {
                repImpl.shutdownGroupSetup(
                    unit.toMillis(replicaShutdownTimeout));
                close();
            }
        }
    }

    /**
     * Registers an {@link AppStateMonitor} to receive the application state
     * which this {@link ReplicatedEnvironment} is running in. Note that there
     * is only one <code>AppStateMonitor</code> per replication node, not one
     * per handle. Invoking this method replaces the previous
     * <code>AppStateMonitor</code>.
     * <p>
     * After registration, the application state can be returned by invoking
     * {@link com.sleepycat.je.rep.util.ReplicationGroupAdmin#getNodeState}.
     *
     * @param appStateMonitor the user implemented AppStateMonitor
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has already been closed.
     */
    public void registerAppStateMonitor(AppStateMonitor appStateMonitor)
        throws IllegalStateException {

        final RepImpl repImpl = checkOpen();

        repImpl.getRepNode().registerAppStateMonitor(appStateMonitor);
    }

    /**
     * Transfers the current master state from this node to one of the
     * electable replicas supplied in the argument list.  The replica that is
     * actually chosen to be the new master is the one with which the Master
     * Transfer can be completed most rapidly.  The transfer operation ensures
     * that all changes at this node are available at the new master upon
     * conclusion of the operation.
     * <p>
     * The following sequence of steps is used to accomplish the transfer:
     * <ol>
     * <li>The master first waits for at least one replica, from
     * amongst the supplied {@code Set} of candidate replicas, to
     * become reasonably current.  It may have to wait for at least
     * one of the replicas to establish a feeder, if none of them are
     * currently connected to the master.  "Reasonably current" means
     * that the replica is close enough to the end of the transaction
     * stream that it has managed to acknowledge a transaction within
     * the time that the commit thread is still awaiting
     * acknowledgments.  If the candidate replicas are working
     * through a long backlog after having been disconnected, this can
     * take some time, so the timeout value should be chosen to allow
     * for this possibility.
     *
     * <li>The master blocks new transactions from being committed or
     * aborted.
     *
     * <li>The master now waits for one of the candidate replicas to
     * become fully current (completely caught up with the end of the
     * log on the master).  The first replica that becomes current is
     * the one that is chosen to become the new master.  This second
     * wait period is expected to be brief, since it only has to wait
     * until transactions that were committed in the interval between
     * step 1) and step 2) have been acknowledged by a replica.
     *
     * <li>The master sends messages to all other nodes announcing the chosen
     * replica as the new master. This node will eventually become a replica,
     * and any subsequent attempt commit or abort existing transactions, or to
     * do write operations will result in a {@code ReplicaWriteException}.
     *
     * <li>The current master releases the transactions that were blocked in
     * step 2) allowing them to proceed. The released transactions will fail
     * with {@code ReplicaWriteException} since the environment has become a
     * replica.
     * </ol>
     *
     * @param replicas the set of replicas to be considered when choosing the
     * new master. The method returns immediately if this node is a member of
     * the set.
     * @param timeout the amount of time to allow for the transfer to be
     * accomplished. A {@code MasterTransferFailureException} is thrown if the
     * transfer is not accomplished within this timeout period.
     * @param timeUnit the time unit associated with the timeout
     *
     * @return the name of the replica that was chosen to be the new master
     * from amongst the set of supplied replicas
     *
     * @throws MasterTransferFailureException if the master transfer operation
     * fails
     * @throws IllegalArgumentException if any of the named replicas is not a
     * member of the replication group or is not of type
     * {@link NodeType#ELECTABLE}
     * @throws IllegalStateException if this node is not currently the master,
     * or this handle or the underlying environment has already been closed.
     */
    public String transferMaster(Set<String> replicas,
                                 int timeout,
                                 TimeUnit timeUnit) {
        return transferMaster(replicas, timeout, timeUnit, false);
    }

    /**
     * Transfers the current master state from this node to one of the replicas
     * supplied in the argument list.
     *
     * @param force true if this request should supersede and cancel any
     * currently pending Master Transfer operation
     *
     * @see #transferMaster(Set, int, TimeUnit)
     */
    public String transferMaster(Set<String> replicas,
                                 int timeout,
                                 TimeUnit timeUnit,
                                 boolean force) {
        final RepImpl repImpl = checkOpen();

        if (timeUnit == null || timeout <= 0) {
            throw new IllegalArgumentException("Invalid timeout");
        }

        return repImpl.transferMaster(replicas,
                                      timeUnit.toMillis(timeout),
                                      force);
    }
}
