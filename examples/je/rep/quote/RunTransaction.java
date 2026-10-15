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

package je.rep.quote;

import java.io.PrintStream;

import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Utility class to begin and commit/abort a transaction and handle exceptions
 * according to this application's policies.  The doTransactionWork method is
 * abstract and must be implemented by callers.  The transaction is run and
 * doTransactionWork is called by the run() method of this class.  The
 * onReplicaWrite and onRetryFailure methods may optionally be overridden.
 */
// RunTransaction 是 Berkeley DB Java Edition (JE) 高可用（HA）复制示例中一个非常核心且优雅的工具/模板类。
// 主要核心作用体现在：
// 封装事务的生命周期管理：采用经典的模板方法设计模式，负责统一处理事务的开启（beginTransaction）、提交（commit）、异常时的回滚（txn.abort()）以及资源的清理。
// 健壮的重试与自愈机制：在高可用分布式复制环境中，常常会遇到网络抖动、副本不足、锁冲突（Lock Conflict）、副本一致性延迟等临时性异常。该类内置了重试循环（最多重试 10 次），能够自动应对并平滑过渡这些分布式集群中的常见故障。
// 读写分离与降级策略：支持针对只读和读写事务定制不同的配置（如只读事务优化 Durability 以避免不必要的 ACK 要求），并在副本不足时动态降低持久性要求，提升系统的可用性。
public abstract class RunTransaction {

    /* The maximum number of times to retry the transaction. */
    // 定义事务遇到可重试异常时的最大重试次数（默认最多重试 10 次）。
    private static final int TRANSACTION_RETRY_MAX = 10;

    /*
     * The number of seconds to wait between retries when a sufficient
     * number of replicas are not available for a transaction.
     */
    // 当遇到群组中没有足够可用的副本（InsufficientReplicasException）时，每次重试前需要挂起等待的秒数（1 秒）。
    private static final int INSUFFICIENT_REPLICA_RETRY_SEC = 1;

    /* Amount of time to wait to let a replica catch up before retrying. */
    // 当遇到副本一致性异常（ReplicaConsistencyException）时，等待副本追赶数据并重试前的挂起秒数（1 秒）。
    private static final int CONSISTENCY_RETRY_SEC = 1;

    /* Amount of time to wait after a lock conflict. */
    // 当遇到锁冲突异常（LockConflictException）时，等待锁释放并重试前的挂起秒数（1 秒）。
    private static final int LOCK_CONFLICT_RETRY_SEC = 1;
    // 保存高可用复制环境句柄（ReplicatedEnvironment），用于执行事务的开启。
    private final ReplicatedEnvironment env;
    // 保存输出流（通常为 System.out），用于在重试或捕获特定异常时向控制台打印日志与提示信息。
    private final PrintStream out;

    /**
     * Creates the runner.
     */
    RunTransaction(ReplicatedEnvironment repEnv, PrintStream printStream) {
        env = repEnv;
        out = printStream;
    }

    /**
     * Runs a transaction, calls the doTransactionWork method, and retries as
     * needed.
     * <p>
     * If the transaction is read only, it uses Durability.READ_ONLY_TXN for
     * the Transaction. Since this Durability policy does not call for any
     * acknowledgments, it eliminates the possibility of a {@link
     * InsufficientReplicasException} being thrown from the call to {@link
     * Environment#beginTransaction} for a read only transaction on a Master,
     * which is an overly stringent requirement. This makes the Master more
     * available for read operations.
     *
     * @param readOnly determines whether the transaction to be run is read
     * only.
     */
    // 核心模板运行方法。负责整个事务的生命周期控制、重试循环、各类分布式异常的捕获与对应策略分发。
    public void run(boolean readOnly)
        throws InterruptedException, EnvironmentFailureException {
        // 初始化状态变量：exception = null、success = false、sleepMillis = 0。
        OperationFailureException exception = null;
        boolean success = false;
        long sleepMillis = 0;
        // 根据是否只读获取初始的事务配置对象 txnConfig。
        TransactionConfig txnConfig = setupTxnConfig(readOnly);

        for (int i = 0; i < TRANSACTION_RETRY_MAX; i++) {
            /* Sleep before retrying. */
            // 重试等待：如果 sleepMillis != 0，调用 Thread.sleep(sleepMillis) 挂起线程等待一段时间。
            if (sleepMillis != 0) {
                Thread.sleep(sleepMillis);
                sleepMillis = 0;
            }
            Transaction txn = null;
            try {
                // 开启事务
                txn = env.beginTransaction(null, txnConfig);
                // 执行业务代码
                doTransactionWork(txn); /* CALL APP-SPECIFIC CODE */
                txn.commit();
                success = true;
                return;
            } catch (InsufficientReplicasException insufficientReplicas) {
                // 保存异常，打印日志，设置重试等待 1 秒。如果重试次数超过 2 次（i > 1），调用 lowerDurability() 动态降低持久性要求，以便在极端情况下提高可用性，随后 continue 重试。
                /*
                 * Retry the transaction.  Give Replicas a chance to contact
                 * this Master, in case they have not had a chance to do so
                 * following an election.
                 */
                exception = insufficientReplicas;
                out.println(insufficientReplicas.toString() + 
                            "\n  Retrying ...");
                sleepMillis = INSUFFICIENT_REPLICA_RETRY_SEC * 1000;

                if (i > 1) {

                    /*
                     * As an example of a possible application choice, 
                     * elect to execute this operation with lower durability.
                     * That makes the node more available, but puts the 
                     * data at greater risk.
                     */
                    txnConfig = lowerDurability(txnConfig);
                }
                continue;
            } catch (InsufficientAcksException insufficientReplicas) {
                // InsufficientAcksException（ACK 确认不足）：说明事务其实已经在当前主节点成功提交了，但部分副本的确认延迟了或宕机了。打印日志后直接终止重试并返回（视作完成）。
                /*
                 * Transaction has been committed at this node. The other
                 * acknowledgments may be late in arriving, or may never arrive
                 * because the replica just went down.
                 */

                /*
                 * INSERT APP-SPECIFIC CODE HERE: For example, repeat
                 * idempotent changes to ensure they went through.
                 *
                 * Note that 'success' is false at this point, although some
                 * applications may consider the transaction to be complete.
                 */
                out.println(insufficientReplicas.toString());
                txn = null;
                return;
            } catch (ReplicaWriteException replicaWrite) {
                // ReplicaWriteException（在副本上尝试写操作）：无法在当前节点完成修改，调用回调方法 onReplicaWrite(replicaWrite)（通常用于路由重定向到 Master），随后返回。
                /*
                 * Attempted a modification while in the Replica state.
                 *
                 * CALL APP-SPECIFIC CODE HERE: Cannot accomplish the changes
                 * on this node, redirect the write to the new master and retry
                 * the transaction there.  This could be done by forwarding the
                 * request to the master here, or by returning an error to the
                 * requester and retrying the request at a higher level.
                 */
                onReplicaWrite(replicaWrite);
                return;
            } catch (LockConflictException lockConflict) {
            // LockConflictException（锁冲突）：保存异常，打印日志，设置等待 1 秒后 continue 重试。
                /*
                 * Retry the transaction.  Note that LockConflictException
                 * covers the HA LockPreemptedException.
                 */
                exception = lockConflict;
                out.println(lockConflict.toString() + "\n  Retrying ...");
                sleepMillis = LOCK_CONFLICT_RETRY_SEC * 1000;
                continue;
            } catch (ReplicaConsistencyException replicaConsistency) {
            // ReplicaConsistencyException（副本一致性未满足）：保存异常，打印日志，设置等待 1 秒后 continue 重试，等待副本追赶。
                /*
                 * Retry the transaction to see if the replica becomes
                 * consistent. If consistency couldn't be satisfied, we can
                 * choose to relax the timeout associated with the
                 * ReplicaConsistencyPolicy, or to do a read with
                 * NoConsistencyRequiredPolicy.
                 */
                exception = replicaConsistency;
                out.println(replicaConsistency.toString() + 
                            "\n  Retrying ...");
                sleepMillis = CONSISTENCY_RETRY_SEC * 1000;
                continue;
            } finally {

                if (!success) {
                    if (txn != null) {
                        txn.abort();
                    }

                    /*
                     * INSERT APP-SPECIFIC CODE HERE: Perform any app-specific
                     * cleanup.
                     */
                }
            }
        }

        /*
         * CALL APP-SPECIFIC CODE HERE: Transaction failed, despite retries.
         */
        // 调用 onRetryFailure(exception) 处理最终失败。
        onRetryFailure(exception);
    }

    /**
     * Must be implemented to perform operations using the given Transaction.
     */
    public abstract void doTransactionWork(Transaction txn);

    /**
     * May be optionally overridden to handle a ReplicaWriteException.  After
     * this method is called, the RunTransaction constructor will return.  By
     * default, this method throws the ReplicaWriteException.
     */
    public void onReplicaWrite(ReplicaWriteException replicaWrite) {
        throw replicaWrite;
    }

    /**
     * May be optionally overridden to handle a failure after the
     * TRANSACTION_RETRY_MAX has been exceeded.  After this method is called,
     * the RunTransaction constructor will return.  By default, this method
     * prints the last exception.
     */
    public void onRetryFailure(OperationFailureException lastException) {
        out.println("Failed despite retries." +
                            ((lastException == null) ?
                              "" :
                              " Encountered exception:" + lastException));
    }

    /**
     * Reduce the Durability level so that we don't require any
     * acknowledgments from replicas. An example of using lower durability
     * requirements.
     */
    private TransactionConfig lowerDurability(TransactionConfig txnConfig) {

        out.println("\nLowering durability, execute update although " +
                    "replicas not available. Update may not be durable.");
        TransactionConfig useTxnConfig = txnConfig;
        if (useTxnConfig == null) {
            useTxnConfig = new TransactionConfig();
        }

        useTxnConfig.setDurability(new Durability(SyncPolicy.WRITE_NO_SYNC,
                                                  SyncPolicy.NO_SYNC,
                                                  ReplicaAckPolicy.NONE));
        return useTxnConfig;
    }

    /**
     * Create an optimal transaction configuration.
     */
     private TransactionConfig setupTxnConfig(boolean readOnly) {

         if (!readOnly) {
             /* 
              * A read/write transaction can just use the default transaction
              * configuration. A null value for the configuration param means
              * defaults should be used.
              */
             return null;
         }

         if (env.getState().isUnknown()) {

             /* 
              * This node is not in touch with the replication group master and
              * because of that, can't fulfill any consistency checks. As an *
              * example of a possible application choice, change the
              * consistency * characteristics for this specific transaction and
              * avoid a * ReplicaConsistencyException by lowering the
              * consistency requirement now.
              */
             out.println("\nLowering consistency, permit access of data " +
                         " currently on this node.");
             return new TransactionConfig().setConsistencyPolicy
                 (NoConsistencyRequiredPolicy.NO_CONSISTENCY);
         } 

         /*
          * We can optimize a read operation by specifying a lower
          * durability. Since Durability.READ_ONLY_TXN does not call for any
          * acknowledgments, it eliminates the possibility of a {@link
          * InsufficientReplicasException} being thrown from the call to {@link
          * Environment#beginTransaction} for a read only transaction on a
          * Master. 
          */
         return new TransactionConfig().setDurability
             (Durability.READ_ONLY_TXN);
     }
}
