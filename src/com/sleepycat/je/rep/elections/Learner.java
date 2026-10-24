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

package com.sleepycat.je.rep.elections;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Proposer.WinningProposal;
import com.sleepycat.je.rep.elections.Protocol.MasterQueryResponse;
import com.sleepycat.je.rep.elections.Protocol.Result;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.elections.Utils.FutureTrackingCompService;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.TextProtocol.InvalidMessageException;
import com.sleepycat.je.rep.impl.TextProtocol.MessageError;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.TextProtocol.MessageOp;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.StoppableThreadFactory;

/**
 * The Learner agent.  It runs in its own dedicated thread, listening for
 * messages announcing the results of elections and, in turn, invoking
 * Listeners within the process to propagate the result.  It also listens for
 * requests asking for elections results, and provides static methods for
 * requesting those results.
 */
// 扮演选举结果的“学习者”与广播分发器角色。
// 运行在独立的后台线程中，监听网络上通告的选举结果（胜出的 Proposal 和 Value），并通知进程内部注册的各种 Listener；
// 同时，它也负责响应其他节点对选举结果的查询请求（Master Query），并提供了用于主动探测、查询和重新广播选举结果的静态与实例工具方法。
// 选举结果的接收与广播：接收 Proposer 广播的选举胜出结果（Result），在过滤掉过时的提议后，通过观察者模式通知所有注册的 Listener（例如触发节点状态转变为 Master 或 Replica）。
// 处理对外的 Master 查询：监听并响应来自其他节点或工具的 MasterQuery 请求，安全地返回当前已知的最高胜出提议和 Master 地址。
// 支持集群状态恢复与探测：提供 queryForMaster、findMaster 以及 reinformLearners 等方法，允许节点在启动、网络分区恢复或日常运维时主动向集群中其他 Learner 探测当前主节点，或向过时的 Master 重新广播新结果。
public class Learner extends ElectionAgentThread {

    /* The service dispatcher used by the Learner */
    private final ServiceDispatcher serviceDispatcher;

    /* The listeners interested in Election outcomes. */
    private final List<Listener> listeners = new LinkedList<>();

    /* The latest winning proposal and value propagated to Listeners. */
    private Proposal currentProposal = null;
    private Value currentValue = null;

    /* Identifies the Learner Service. */
    public static final String SERVICE_NAME = "Learner";

    /**
     * Creates an instance of a Learner which will listen for election results
     * to propagate to local listeners, and for requests asking for election
     * results.
     *
     * <p>Note that this constructor does not take a repNode as an argument, so
     * that it can be used as the basis for the standalone Monitor.
     *
     * @param protocol the protocol used for message exchange
     * @param serviceDispatcher the service dispatcher used by the agent
     */
    public Learner(Protocol protocol,
                   ServiceDispatcher serviceDispatcher) {
       this(null, protocol, serviceDispatcher);
    }

    public Learner(RepImpl repImpl,
                    Protocol protocol,
                    ServiceDispatcher serviceDispatcher) {
        super(repImpl, protocol,
              "Learner Thread " + protocol.getNameIdPair().getName());
        this.serviceDispatcher = serviceDispatcher;

        /* Add a listener for logging. */
        addListener(new Listener() {
                @Override
                public void notify(Proposal proposal, Value value) {
                    LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                                       "Learner notified. Proposal:" +
                                       proposal + " Value: " + value);
                }
            });
    }

    /**
     * Adds a Listener to the existing set of listeners, so that it can be
     * informed of the outcome of election results.
     *
     * @param listener the new listener to be added
     */
    public void addListener(Listener listener) {
        synchronized (listeners) {
            if (!listeners.contains(listener)) {
                listeners.add(listener);
            }
        }
    }

    /**
     * Removes a Listeners from the existing set of listeners.
     *
     * @param listener the listener to be removed.
     */
    void removeListener(Listener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    /**
     * Processes a result message
     *
     * @param proposal the winning proposal
     * @param value the winning value
     */
    // Learner 的业务大脑。
    // 负责接收并应用新的选举胜出结果
    // 接收并处理新的选举胜出结果（Winning Proposal 与对应的 Value）。通过严格的版本比较过滤掉过时的提议，更新本地维护的最新胜出状态，并采用观察者模式安全地通知所有注册的监听器（Listener），
    // 从而驱动整个节点状态机完成向 Master 或 Replica 的演进。
    // Proposal proposal：本次选举中胜出的提议对象（封装了高阶序列号和世代信息）
    // Value value：与该提议关联的共识值（通常为 MasterValue，包含了当选主节点的网络地址、端口及名称标识）。
    synchronized public void processResult(Proposal proposal, Value value) {
        // 实现分布式一致性中的防回滚、防旧消息污染安全检查。
        // 检查本地是否已经记录了当前的胜出提议（currentProposal != null），并使用 compareTo 将新收到的 proposal 与本地记录进行比较。如果新提议的编号小于本地记录（< 0），说明这是一个“迟到”或“过时”的选举结果。
        if ((currentProposal != null) &&
            (proposal.compareTo(currentProposal) < 0)) {
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                               "Ignoring obsolete winner: " + proposal);
            return;
        }
        // 更新本地胜出状态
        currentProposal = proposal;
        currentValue = value;

        /* We have a new winning proposal and value, inform the listeners */
        // 观察者通知与异常隔离机制
        synchronized (listeners) {
            for (Listener listener : listeners) {
                try {
                    listener.notify(currentProposal, currentValue);
                } catch (Exception e) {
                    e.printStackTrace();
                    /* Report the exception and keep going. */
                    LoggerUtils.logMsg
                        (logger, envImpl, formatter, Level.SEVERE,
                         "Exception in Learner Listener: " + e.getMessage());
                    continue;
                }
            }
        }
    }

    /**
     * The main Learner loop. It accepts requests and propagates them to its
     * Listeners, if the proposal isn't out of date.
     */
    @Override
    public void run() {
        serviceDispatcher.register(SERVICE_NAME, channelQueue);
        LoggerUtils.logMsg
            (logger, envImpl, formatter, Level.FINE, "Learner started");
        DataChannel channel = null;
        try {
            while (true) {
                channel = serviceDispatcher.takeChannel
                    (SERVICE_NAME,
                     true /* blocking socket */,
                     protocol.getReadTimeout());

                if (channel == null) {
                    /* A soft shutdown. */
                   return;
                }

                BufferedReader in = null;
                PrintWriter out = null;
                try {
                    in = new BufferedReader
                        (new InputStreamReader(
                            Channels.newInputStream(channel)));
                    final String requestLine = in.readLine();
                    if (requestLine == null) {
                        continue;
                    }
                    final RequestMessage requestMessage;
                    try {
                        requestMessage = protocol.parseRequest(requestLine);
                    } catch (InvalidMessageException ime) {
                        protocol.processIME(channel, ime);
                        continue;
                    }

                    final MessageOp op = requestMessage.getOp();
                    LoggerUtils.logMsg(logger, envImpl, formatter,
                                       Level.FINEST,
                                       "learner request: " + op +
                                       " sender: " +
                                       requestMessage.getSenderId());
                    if (op == protocol.RESULT) {
                        Result result = (Result) requestMessage;
                        processResult(result.getProposal(), result.getValue());
                    } else if (op == protocol.MASTER_QUERY) {
                        processMasterQuery(channel, requestMessage);
                    } else if (op == protocol.SHUTDOWN) {
                        LoggerUtils.logMsg
                            (logger, envImpl, formatter, Level.FINE,
                             "Learner thread exiting");
                        break;
                    } else {
                        final String message =
                            "Malformed request: '" + requestLine + "'" +
                            " Unexpected op:" + op;
                        final InvalidMessageException ime =
                            new InvalidMessageException(MessageError.BAD_FORMAT,
                                                        message);
                        protocol.processIME(channel, ime);
                        continue;
                    }
                } catch (IOException e) {
                    LoggerUtils.logMsg
                        (logger, envImpl, formatter, Level.INFO,
                         "IO exception: " + e.getMessage());
                } catch (Exception e) {
                    throw EnvironmentFailureException.unexpectedException(e);
                } finally {
                    Utils.cleanup(logger, envImpl, formatter, channel, in, out);
                }
            }
        } catch (InterruptedException e) {
            if (isShutdown()) {
                /* Treat it like a shutdown, exit the thread. */
                return;
            }
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.WARNING,
                               "Learner unexpected interrupted");
            throw EnvironmentFailureException.unexpectedException(e);
       } finally {
            serviceDispatcher.cancel(SERVICE_NAME);
            cleanup();
       }
    }

    /**
     * Responds to a query for the current master. A response is only
     * generated if the node is currently in the Master or Replica state to
     * ensure that the information is reasonably current.
     */
    // 负责响应外部或其他节点的 Master 身份查询。在确保本地有合法的选举结果且节点处于活跃（Active）状态的前提下，安全地将当前胜出者信息回传给请求方。
    // 响应外部节点或管理工具发起的“谁是主节点（Master Query）”查询请求的核心方法
    // 当收到对当前 Master 的查询时，首先校验本地是否持有有效的选举结果以及节点是否处于活跃状态（防止给出过时或错误的信息），然后将当前的胜出提议（Proposal）和共识值（Value）打包成响应，跨网络回传给请求方。
    synchronized private void processMasterQuery(DataChannel channel,
                                                 RequestMessage requestMessage)
    {
        // 如果本地节点刚启动，或者还没有经历过任何成功的选举（即 currentProposal 或 currentValue 仍为 null），说明当前没有任何选举结果可以分享，直接 return 结束。
        if ((currentProposal == null) || (currentValue == null)) {
            /* Don't have any election results to share. */
            return;
        }
        // 检查本地的复制环境实现 (RepImpl) 是否为空，或者当前节点的复制状态是否处于活跃状态（isActive()，例如必须是 Master 或 Replica）。
        if ((envImpl == null) || !((RepImpl) envImpl).getState().isActive()) {
            /* Knowledge of master is potentially obsolete */
            return;
        }

        PrintWriter out = null;
        try {
            // 将 NIO 的 DataChannel 转换为标准的 Java 输出流。
            out = new PrintWriter(Channels.newOutputStream(channel), true);
            // 创建包含当前胜出结果的响应对象
            final MasterQueryResponse responseMessage = protocol.new
                MasterQueryResponse(currentProposal, currentValue);

            /*
             * The request message may be of an earlier version. If so, this
             * node transparently read the older version. JE only throws out
             * InvalidMessageException when the version of the request message
             * is newer than the current protocol. To avoid sending a response
             * that the requester cannot understand, we send a response in the
             * same version as that of the original request message.
             */
            // 将响应消息的发送版本设置为与进来的 requestMessage 完全一致，确保通信双方的协议语义完美匹配。
            responseMessage.setSendVersion(requestMessage.getSendVersion());
            out.println(responseMessage.wireFormat());
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    /**
     * Queries other learners, in parallel, to determine whether they know of
     * an existing master in the group. If one is found, the result is
     * processed via {@link #processResult} as though it were an election
     * result that was sent to the Learner, resulting in the node transitioning
     * to the master or replica state as appropriate.
     * <p>
     * Note that this node itself is not allowed to become a master as a result
     * of such a query. It must only do so via an election.
     *
     * @param learnerSockets the sockets associated with learners at other
     * nodes. The nodes are queried on these sockets.
     */
    // 主动探测机制。在节点启动或失联重连时，通过多线程并发向其他 Learner 节点广播查询，探寻集群中现存的合法 Master，帮助节点快速恢复状态。
    public void queryForMaster(Set<InetSocketAddress> learnerSockets) {
        if (learnerSockets.size() <= 0) {
                return;
        }
        int threadPoolSize = Math.min(learnerSockets.size(), 10);
        final ExecutorService pool =
            Executors.newFixedThreadPool
               (threadPoolSize, new StoppableThreadFactory("JE Learner",
                                                           logger));
        try {
            RequestMessage masterQuery = protocol.new MasterQuery();
            FutureTrackingCompService<MessageExchange> compService =
                Utils.broadcastMessage(learnerSockets,
                                       Learner.SERVICE_NAME,
                                       masterQuery,
                                       pool);
            /*
             * 2 * read timeout below to roughly cover the max time for a
             * message exchange.
             */
            new Utils.WithFutureExceptionHandler<MessageExchange>
            (compService, 2 * protocol.getReadTimeout(), TimeUnit.MILLISECONDS,
                logger, (RepImpl)envImpl, formatter) {

                @Override
                protected void processResponse(MessageExchange me) {

                    if (me.getResponseMessage().getOp() ==
                        protocol.MASTER_QUERY_RESPONSE){
                        MasterQueryResponse accept =
                            (MasterQueryResponse) me.getResponseMessage();
                        MasterValue masterValue =
                            (MasterValue) accept.getValue();
                        if ((masterValue != null) &&
                            masterValue.getNameId().
                            equals(protocol.getNameIdPair())) {

                            /*
                             * Should not transition to master as a result
                             * of a query it risks imposing a hard recovery
                             * on the replicas.
                             */
                            return;
                        }
                        processResult(accept.getProposal(), masterValue);
                    }
                }

                @Override
                protected boolean isShutdown() {
                    return Learner.this.isShutdown();
                }
            }.execute();
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Returns the socket address for the current master, or null if one
     * could not be determined from the available set of learners. This API
     * is suitable for tools which need to contact the master for a specific
     * service, e.g. to delete a replication node, or to add a monitor. This
     * method could be used in principle to establish other types of nodes as
     * well via a tool, but that is currently done by the handshake process.
     *
     * @param protocol the protocol to be used when determining the master
     *
     * @param learnerSockets the learner to be queried for the master
     * @param logger for log messages
     * @return the MasterValue identifying the master
     * @throws UnknownMasterException if no master could be established
     */
    static public MasterValue findMaster
        (final Protocol protocol,
         Set<InetSocketAddress> learnerSockets,
         final Logger logger,
         final RepImpl repImpl,
         final Formatter formatter )
        throws UnknownMasterException {

        if (learnerSockets.size() <= 0) {
                return null;
        }
        int threadPoolSize = Math.min(learnerSockets.size(), 10);
        final ExecutorService pool =
            Executors.newFixedThreadPool(threadPoolSize);
        try {
            FutureTrackingCompService<MessageExchange> compService =
                Utils.broadcastMessage(learnerSockets,
                                       Learner.SERVICE_NAME,
                                       protocol.new MasterQuery(),
                                       pool);

            final List<MasterQueryResponse> results = new LinkedList<>();
            new Utils.WithFutureExceptionHandler<MessageExchange>
            (compService, 2 * protocol.getReadTimeout(), TimeUnit.MILLISECONDS,
             logger, repImpl, formatter) {

                @Override
                protected void processResponse(MessageExchange me) {

                    final ResponseMessage response = me.getResponseMessage();

                    if (response.getOp() ==
                        protocol.MASTER_QUERY_RESPONSE){
                        results.add((MasterQueryResponse)response);
                    } else {
                        LoggerUtils.logMsg(logger, repImpl, formatter,
                                           Level.WARNING,
                                           "Unexpected MasterQuery response:" +
                                           response.wireFormat());
                    }
                }

                @Override
                protected boolean isShutdown() {
                    return (repImpl != null) && !repImpl.isValid();
                }

            }.execute();

            MasterQueryResponse bestResponse = null;
            for (MasterQueryResponse result : results) {
                if ((bestResponse == null) ||
                    (result.getProposal().
                        compareTo(bestResponse.getProposal()) > 0)) {
                    bestResponse = result;
                }
            }
            if (bestResponse == null) {
                throw new UnknownMasterException
                ("Could not determine master from helpers at:" +
                    learnerSockets.toString());
            }
            return(MasterValue) bestResponse.getValue();
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * A method to re-broadcast this Learner's notion of the master. This
     * re-broadcast is done primarily to inform an obsolete master that it's no
     * longer the current master. Obsolete master situations arise in network
     * partition scenarios, where a current master is not able to participate
     * in an election, nor is it informed about the results. The re-broadcast
     * is the mechanism for rectifying such a situation. When the obsolete
     * master receives the new results after the network partition has been
     * fixed, it will revert to being a replica.
     *
     * @param learners the learners that must be informed
     * @param threadPool the pool used to dispatch broadcast requests in
     * in parallel
     */
    public void reinformLearners(Set<InetSocketAddress> learners,
                                 ExecutorService threadPool) {

        Proposer.WinningProposal winningProposal;
        synchronized (this) {
            if ((currentProposal == null) || (currentValue == null)) {
                return;
            }
            winningProposal =
                new WinningProposal(currentProposal, currentValue, null);
        }

        final RepImpl repImpl = (RepImpl)envImpl;
        if (repImpl == null) {
            return;
        }
        informLearners(learners, winningProposal, protocol, threadPool,
                       logger, repImpl, formatter);
    }

    /**
     * A utility method used to broadcast the results of an election to
     * Listeners.
     *
     * @param learners that need to be informed.
     * @param winningProposal the result that needs to be propagated
     * @param protocol to be used for communication
     * @param threadPool used to supply threads for the broadcast
     */
    public static void informLearners(Set<InetSocketAddress> learners,
                                      Proposer.WinningProposal winningProposal,
                                      Protocol protocol,
                                      ExecutorService threadPool,
                                      final Logger logger,
                                      final RepImpl repImpl,
                                      final Formatter formatter) {

        if ((learners == null) || (learners.size() == 0)) {
            throw EnvironmentFailureException.unexpectedState
                ("There must be at least one learner");
        }

        LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                           "Informing " + learners.size() + " learners.");
        FutureTrackingCompService<MessageExchange> compService =
            Utils.broadcastMessage(learners,
                                   Learner.SERVICE_NAME,
                                   protocol.new Result
                                   (winningProposal.proposal,
                                    winningProposal.chosenValue),
                                   threadPool);

        /* Consume the futures. */

        /* Atomic to provide incrementable "final" to nested method. */
        final AtomicInteger count = new AtomicInteger(0);

        new Utils.WithFutureExceptionHandler<MessageExchange>
        (compService, 2 * protocol.getReadTimeout(), TimeUnit.MILLISECONDS,
         logger, repImpl, formatter) {

            @Override
            protected void processResponse(MessageExchange me) {
                /* Do nothing, just consume the futures. */
                count.incrementAndGet();
            }

            @Override
            protected void processNullResponse(MessageExchange me) {
                if (me.getException() == null) {
                    count.incrementAndGet();
                }
            }

            @Override
            protected boolean isShutdown() {
                return (repImpl != null) && !repImpl.isValid();
            }

        }.execute();
        LoggerUtils.logMsg
            (logger, repImpl, formatter, Level.FINE,
             "Informed learners: " + count.get());
    }

    /**
     * @see StoppableThread#getLogger
     */
    @Override
    protected Logger getLogger() {
        return logger;
    }

    /*
     * Notifies the listener that a new proposal has been accepted. Note that
     * the value may be unchanged. The proposals may be out of sequence, it's
     * up to the listener to deal with it appropriately.
     */
    public static interface Listener {
        void notify(Proposal proposal, Value value);
    }
}
