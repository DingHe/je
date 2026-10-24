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

import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PHASE1_ARBITER;
import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PHASE1_HIGHER_PROPOSAL;
import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PHASE1_NO_NON_ZERO_PRIO;
import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PHASE1_NO_QUORUM;
import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PHASE2_HIGHER_PROPOSAL;
import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PHASE2_NO_QUORUM;
import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PROMISE_COUNT;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.elections.Protocol.Accept;
import com.sleepycat.je.rep.elections.Protocol.Promise;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.elections.Utils.FutureTrackingCompService;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.TextProtocol.MessageOp;
import com.sleepycat.je.rep.impl.TextProtocol.ProtocolError;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Plays the role of a Proposer in the consensus algorithm.
 *
 * Note that the Proposer also plays the role of a distinguished learner and
 * informs all other listeners about the acceptance of a proposal.
 */
// 选举与共识模块中，Proposer 是一个核心的抽象基类，它在 Paxos 分布式共识算法中扮演提议者（Proposer）的角色。
// 同时，它也承担了特定学习者（Learner）的职责，负责向集群中的其他节点广播并通告提案被接受（Chosen）的结果。
// 实现 Paxos 共识算法的核心流程：封装了两阶段提交（Phase 1 预备/提议阶段、Phase 2 接受/确认阶段）的核心逻辑。
// 管理选举重试与仲裁机制：通过 issueProposal 方法协调整个选主过程，支持配置不同的法定人数策略（QuorumPolicy）和重试谓词（RetryPredicate）。
// 统计与监控指标收敛：内置了详尽的统计指标组（StatGroup），用于记录选举过程中的各项性能与异常指标（如无法定人数、遇到更高编号提案、收到承诺数等）。
public abstract class Proposer {

    /* The elections instance that is running this Learner agent. */
    // 当前正在运行此提议者代理的选举环境控制实例（Elections），通过它可以获取线程池、网络通道工厂、协议对象等核心上下文。
    protected final Elections elections;

    /* The statistics from one call of issueProposal. */
    // 统计指标组容器
    // 将 Proposer 在选举过程中产生的所有子指标（如各种失败、票数等）归类聚合在一起，形成一个统一的统计组。
    private final StatGroup statistics;
    // 第一阶段（Phase 1）法定人数不足计数器
    // 记录在第一阶段发送 Propose 提案后，收到的有效 Promise（承诺）票数未达到配置的 QuorumPolicy（法定人数策略）要求的发生次数。
    private final IntStat phase1NoQuorum;
    // 第一阶段无有效优先级节点计数器。用于统计在选举提议时，由于缺乏具有非零优先级的参与节点而引发的特殊分支或统计。
    protected final IntStat phase1NoNonZeroPrio;
    // 第一阶段仲裁者（Arbiter）参与计数器。记录在第一阶段中仲裁者节点参与投票或对选举结果产生影响的统计次数。
    protected final IntStat phase1Arbiter;
    // 第一阶段遇到更高编号提案计数器。记录在第一阶段中，由于 Acceptor 返回 REJECT（表明集群中已经存在更高编号的提案）而导致提议被废弃的次数。
    private final IntStat phase1HigherProposal;
    // 第二阶段（Phase 2）法定人数不足计数器。记录在第二阶段发送 Accept 请求后，收到的 Accepted 响应数未能满足法定人数要求的发生次数。
    private final IntStat phase2NoQuorum;
    // 第二阶段遇到更高编号提案计数器。记录在第二阶段交互中遭遇 REJECT 拒绝、发现存在更高编号提案的次数。
    private final IntStat phase2HigherProposal;
    // 承诺票数统计。记录在第一阶段中成功收集并确认的有效 Promise 响应的数量。
    private final IntStat promiseCount;
    // 当外部调用 shutdown() 时，该标志被置为 true，用于安全地通知并中断正在异步等待网络响应的轮询循环，实现优雅退出。
    private volatile boolean shutdown;

    protected final Logger logger;
    // 复制日志格式化器
    // 用于在输出日志时附加当前节点的唯一标识（NameIdPair），确保多节点并发日志的可读性与追踪性。
    protected final Formatter formatter;

    /**
     * Initializes a proposer with the set of acceptors identified by the
     * sockets on which they accept proposals.
     */
    public Proposer(Elections elections, NameIdPair nameIdPair) {
        this.elections = elections;

        if (elections.getRepImpl() != null) {
            logger = LoggerUtils.getLogger(getClass());
        } else {
            logger = LoggerUtils.getLoggerFormatterNeeded(getClass());
        }
        formatter = new ReplicationFormatter(nameIdPair);

        statistics = new StatGroup(ProposerStatDefinition.GROUP_NAME,
                                    ProposerStatDefinition.GROUP_DESC);
        phase1NoQuorum = new IntStat(statistics, PHASE1_NO_QUORUM);
        phase1NoNonZeroPrio = new IntStat(statistics, PHASE1_NO_NON_ZERO_PRIO);
        phase1Arbiter = new IntStat(statistics, PHASE1_ARBITER);
        phase1HigherProposal = new IntStat(statistics, PHASE1_HIGHER_PROPOSAL);
        phase2NoQuorum = new IntStat(statistics, PHASE2_NO_QUORUM);
        phase2HigherProposal = new IntStat(statistics, PHASE2_HIGHER_PROPOSAL);
        promiseCount = new IntStat(statistics, PROMISE_COUNT);
    }

    /**
     * Shut down the proposer and reclaim its resources.
     */
    public void shutdown() {
        shutdown = true;
    }

    /**
     * Returns the current proposer statistics.
     */
    public StatGroup getProposerStats() {
        return statistics;
    }

    /**
     * Predicate to determine whether we have a quorum based upon the quorum
     * policy. Note that the case of two attendees is special-cased when a
     * SIMPLE_MAJORITY is called for, to return success with just one yes vote.
     *
     * @param quorumPolicy the policy to be used for the quorum.
     * @param votes the number of yes votes.
     * @return true if the number of votes satisfies the quorum policy.
     */
    private boolean haveQuorum(QuorumPolicy quorumPolicy, int votes) {
        return elections.getElectionQuorum().
                haveQuorum(quorumPolicy, votes);
    }

    /**
     * Implements phase 1. It sends our Propose requests to all acceptors and
     * tallies up the results.
     *
     * @param proposal the new unique proposal.
     *
     * @return the results from running Phase1, or null if phase 1 did not get
     * a promise from the majority of acceptors.
     *
     * @throws HigherNumberedProposal
     */
    // 整个选主算法中第一阶段（Phase 1：Prepare / Propose 与 Promise 阶段）的具体执行方法
    // 向集群中所有的 Acceptor 节点广播 Propose 请求（即 Paxos 的准备/承诺请求），并对收集到的投票响应结果进行统计和法定人数（Quorum）校验。
    private Phase1Result phase1(QuorumPolicy quorumPolicy, Proposal proposal)
        throws HigherNumberedProposal {
        // 打印阶段一启动日志
        LoggerUtils.logMsg(logger, elections.getRepImpl(), formatter,
                           Level.FINE, "Phase 1 proposal: " + proposal);

        /* Broadcast Propose requests. */
        // 获取目标 Acceptor 地址集合
        // 向 Elections 协调中心查询当前复制组内所有合法的 Acceptor 节点的 Socket 地址集合，作为本次消息广播的目标名单
        final Set<InetSocketAddress> acceptorSockets =
            elections.getAcceptorSockets();
        // 并发广播 Propose 请求
        FutureTrackingCompService<MessageExchange> compService =
            Utils.broadcastMessage
        (acceptorSockets,
         Acceptor.SERVICE_NAME,
         elections.getProtocol().new Propose(proposal),
         elections.getThreadPool());
        // 收集并统计第一阶段的响应结果
        Phase1Result result = tallyPhase1Results(proposal, compService);
        // 收集并统计第一阶段的响应结果
        // 判断第一阶段收集到的承诺票数是否达标
        if (haveQuorum(quorumPolicy, result.promisories.size())) {
            return result;
        }
        // 如果票数未达法定要求（即第一阶段流产）
        // 将 phase1NoQuorum 统计计数器自增 1（用于监控和诊断第一阶段因为票数不够而失败的次数）。
        phase1NoQuorum.increment();

        return null;
    }

    /**
     * Tally the results from Phase 1. Terminate the tally if we see any
     * rejection messages which indicate our proposal has been obsoleted by a
     * more recent one. It also tracks the highest numbered proposal and the
     * associated value that was accepted by the Acceptors.
     *
     * @param currentProposal the proposal for this round
     * @param compService the futures resulting from Prepare messages
     *
     * @return results Summarized results from Phase 1
     * @throws HigherNumberedProposal
     */
    // 遍历并统计所有 Acceptor 对第一阶段提议的响应。
    // 如果收到任何来自更高编号提案的拒绝（Reject）则立即中止；同时，它会筛选并追踪所有承诺节点中历史上已被接受的最高编号提议及其对应的共识值。
    private Phase1Result
    tallyPhase1Results(Proposal currentProposal,
                       final FutureTrackingCompService<MessageExchange>
                       compService)
        throws HigherNumberedProposal {

        final Phase1Result result = new Phase1Result();
        // 构建并执行异步响应收集器
        // 超时设定：超时时间设为 2 * elections.getProtocol().getReadTimeout()（即两倍的读超时时间），单位为毫秒，防止因某个 Acceptor 挂掉导致 Proposer 线程无限期阻塞。
        new Utils.WithFutureExceptionHandler<MessageExchange>
                (compService, 2 * elections.getProtocol().getReadTimeout(),
                 TimeUnit.MILLISECONDS, logger, elections.getRepImpl(), null) {

            @Override
            protected void processResponse(MessageExchange me) {

                final MessageOp op = me.getResponseMessage().getOp();
                // 处理拒绝响应 (REJECT) —— 遇到竞争的高编号提案
                // 如果 Acceptor 返回 REJECT，说明该 Acceptor 已经接受或承诺了更高编号的 Prepare 请求。
                if (op == elections.getProtocol().REJECT) {

                    /*
                     * The acceptor has already received a higher numbered
                     * Prepare request. Abandon this round as an optimization.
                     */
                    // 取消剩余尚未完成的并发网络请求（优化：既然已经撞车，没必要再等其他节点响应）
                    compService.cancelFutures(true);
                    phase1HigherProposal.increment();
                    throw new HigherNumberedProposal(me.target);
                } else if (op == elections.getProtocol().PROMISE) {
                // 处理承诺响应 (PROMISE) —— 收集票数与历史提案
                    // 将该节点的 Socket 地址加入 result.promisories 集合，并将交互记录加入 result.promisoryMessages。
                    result.promisories.add(me.target);
                    result.promisoryMessages.add(me);
                    Promise accept = (Promise) me.getResponseMessage();
                    // 提取出该 Acceptor 此前曾接受过的最高提议（acceptProposal）。
                    Proposal acceptProposal = accept.getHighestProposal();
                    // 维持最高提议追踪逻辑
                    // 将当前响应中的 acceptProposal 与目前记录的 result.acceptProposal 做对比。如果当前更新（更大），则更新 result.acceptProposal 和对应的 result.acceptedValue。
                    // 这完美契合了 Paxos 核心规则：第二阶段必须采用历史上已被接受的最高编号提议所对应的共识值。
                    if ((result.acceptProposal == null) ||
                        ((acceptProposal != null) &&
                            (acceptProposal.compareTo(result.acceptProposal) >
                            0))) {
                        result.acceptProposal = acceptProposal;
                        result.acceptedValue = accept.getAcceptedValue();
                    }
                } else if (op == elections.getProtocol().PROTOCOL_ERROR){
                // 处理协议错误与未知响应
                // 如果收到协议错误（如版本/配置不匹配）或未知的操作码，记录警告日志并安全返回（不计入有效承诺票数）
                        ProtocolError protocolError =
                            (ProtocolError) me.getResponseMessage();
                        /* Possible protocol version, group, or config skew. */
                        LoggerUtils.logMsg(logger, elections.getRepImpl(),
                                           formatter, Level.WARNING,
                                           "Proposer phase 1 protocol error: " +
                                           protocolError.getMessage());
                        return;
                    } else {
                        LoggerUtils.logMsg(logger, elections.getRepImpl(),
                                           formatter, Level.WARNING,
                                           "Proposer phase 1 unknown" +
                                           " response: " + op);
                        return;
                    }
                }

            @Override
            protected boolean isShutdown() {
                return shutdown;
            }
        }.execute();
        // 如果在第一阶段收集到了承诺（promisories.size() > 0），
        // 但所有 Acceptor 此前均未接受过任何历史提议（即 result.acceptProposal == null，说明当前集群处于全新状态或历史干净），
        // 则将 acceptProposal 默认设为当前的提议 currentProposal。
        if ((result.promisories.size() > 0) &&
            (result.acceptProposal == null)) {
            result.acceptProposal = currentProposal;
        }
        promiseCount.set(result.promisories.size());

        return result;
    }

    /**
     * Runs Phase 2 for the proposal and value.
     *
     * @param proposal the proposal resulting from Phase1
     * @param phase2Value the value that we would like to be chosen.
     * @param promisories from Phase1
     * @return a summary of the result or null if proposal was not accepted
     * @throws HigherNumberedProposal
     */
    // 整个选主算法中第二阶段（Phase 2：Accept / Accepted 接受阶段）的核心执行方法
    // 针对第一阶段确立的提议（Proposal）和筛选出的共识值（Phase 2 Value），向第一阶段给出过承诺（Promise）的 Acceptor 节点广播 Accept 请求，并统计它们的接受确认，最终判定是否正式达成共识。
    // Proposal proposal：由第一阶段成功产出并沿用的唯一提议编号。
    // Value phase2Value：经过 choosePhase2Value 筛选、在第二阶段希望被集群正式采纳（Chosen）的共识值（通常代表选出的 Master 身份）。
    // Set<InetSocketAddress> promisories：第一阶段成功给出承诺（Promise）的 Acceptor 节点集合。（Paxos 协议优化点：第二阶段无需向所有节点广播，只需向这些给过承诺的节点发送 Accept 请求即可）。
    private Phase2Result phase2(QuorumPolicy quorumPolicy,
                                Proposal proposal,
                                Value phase2Value,
                                Set<InetSocketAddress> promisories)
        throws HigherNumberedProposal {
        // 打印阶段二启动日志
        LoggerUtils.logMsg
            (logger, elections.getRepImpl(), formatter, Level.FINE,
             "Phase 2 proposal: " + proposal + " Value: " + phase2Value);

        /* Broadcast Accept requests to promisories. */
        // 构造 Accept 协议消息
        // 将当前的提议编号（proposal）和待接受的共识值（phase2Value）打包，作为即将向远端 Acceptor 发送的内容载荷。
        final Accept accept =
            elections.getProtocol().new Accept(proposal, phase2Value);
        // 并发广播 Accept 请求
        FutureTrackingCompService<MessageExchange> compService =
            Utils.broadcastMessage(promisories, Acceptor.SERVICE_NAME,
                                   accept, elections.getThreadPool());
        // 统计第二阶段的响应结果
        Phase2Result result = tallyPhase2Results(quorumPolicy, compService);
        // 校验第二阶段的法定人数 (Quorum Check)
        if (haveQuorum(quorumPolicy, result.accepts.size())) {
            return result;
        }

        phase2NoQuorum.increment();
        return null;
    }

    /**
     * Tallies the results from Phase 2.
     *
     * @param compService the futures from phase2
     * @return the Phase2Result
     * @throws HigherNumberedProposal if a higher numbered proposal was
     *         encountered
     */
    // 处理第二阶段（Accept / Accepted）广播响应的核心汇总、统计与短路优化方法。
    // 遍历并统计所有 Acceptor 对第二阶段 Accept 请求的响应。如果在响应过程中收集到足够达到法定人数（Quorum）的 ACCEPTED 确认，则会立即触发短路优化、提前终止剩余等待；
    // 反之，若遇到任何来自更高编号提案的拒绝（REJECT）则立即抛出异常终止。
    private Phase2Result
        tallyPhase2Results(
            final QuorumPolicy quorumPolicy,
            final FutureTrackingCompService<MessageExchange> compService)
        throws HigherNumberedProposal {

        final Phase2Result result = new Phase2Result();
        try {
            new Utils.WithFutureExceptionHandler<MessageExchange>
            (compService, 2 * elections.getProtocol().getReadTimeout(),
                TimeUnit.MILLISECONDS, logger, elections.getRepImpl(), null) {

                @Override
                protected void processResponse(MessageExchange me) {
                    final ResponseMessage responseMessage =
                        me.getResponseMessage();
                    // 处理拒绝响应 (REJECT) —— 遭遇更高编号提案竞争
                    final Protocol protocol = elections.getProtocol();
                    final MessageOp op = responseMessage.getOp();
                    if (op == protocol.REJECT) {
                        // 强行取消所有剩余未完成的并发网络任务（优化：既然已经遭遇冲突，无需浪费时间等待其他节点）
                        compService.cancelFutures(true);
                        phase2HigherProposal.increment();
                        throw new HigherNumberedProposal(me.target);
                    } else if (op == protocol.ACCEPTED) {
                    // 处理接受确认 (ACCEPTED) 与 Quorum 短路优化
                        result.accepts.add(me.target);
                        // 关键优化（Quorum Short-circuit）：调用 haveQuorum(...) 检查当前的接受票数是否已经达标。
                        if (haveQuorum(quorumPolicy,
                                       result.accepts.size())) {
                            compService.cancelFutures(true);
                            throw new HaveQuorum();
                        }
                    } else if (op == protocol.PROTOCOL_ERROR) {
                     // 处理协议错误与未知响应
                        final Protocol.ProtocolError errorMessage =
                            (Protocol.ProtocolError)responseMessage;
                        LoggerUtils.logMsg(logger, elections.getRepImpl(),
                                           formatter, Level.WARNING,
                                           "Proposer phase 2 " +
                                               "protocol error:" +
                                               errorMessage.getMessage());
                    } else {
                        LoggerUtils.logMsg(logger, elections.getRepImpl(),
                                           formatter, Level.WARNING,
                                           "Proposer Phase 2 " +
                                               "unknown response: " + op);
                    }
                }

                @Override
                protected boolean isShutdown() {
                    return shutdown;
                }
            }.execute();

        } catch (HaveQuorum hq) {
            /* Terminated phase2 early from an early quorum. */
        }
        return result;
    }

    /**
     * Selects the value to be used during Phase2, which if it succeeds will
     * become the "chosen value".
     *
     * @param exchanges the message exchanges from Phase 1
     *
     * @return the candidate value for Phase2, or null, if no suitable value
     * was found during phase 1
     */
    protected abstract Value choosePhase2Value(Set<MessageExchange> exchanges);

    /**
     * Creates the next unique Proposal to be used
     *
     * @return the Proposal
     */
    protected abstract Proposal nextProposal();

    /**
     * Runs an election using the consensus algorithm. An election can be used
     * either to determine an existing consensus value, or to force an election
     * if the current value is not suitable.
     *
     * @param quorumPolicy the policy used to determine whether we have a
     * quorum.
     *
     * @param retryPredicate the interface which determines whether a retry
     * should be attempted.
     *
     * @return the WinningProposal encapsulating the proposal and chosen value
     *
     * @throws MaxRetriesException when a winning proposal could not be
     * established.
     * @throws InterruptedException
     */
    // 整个选主算法的核心执行驱动方法
    // 负责协调和串联标准的两阶段 Paxos 共识协议（Phase 1: Prepare/Promise 与 Phase 2: Accept/Accepted），直到成功选出 Master 或重试次数耗尽。
    public WinningProposal issueProposal(QuorumPolicy quorumPolicy,
                                         RetryPredicate retryPredicate)
        throws MaxRetriesException, InterruptedException {

        statistics.clear();
        // 每次循环代表一轮完整的 Paxos 提议尝试
        while (retryPredicate.retry()) {
            try {
                // 阶段一：生成提议与 Prepare 广播
                // 生成并获取下一个唯一的提案编号（Proposal）（通常基于时间戳和节点 ID 组合生成，确保全局唯一且单调递增）。
                final Proposal proposal = nextProposal();
                // 向集群中的所有 Acceptor 广播 Prepare 请求，并收集它们的 Promise（承诺）回复。
                final Phase1Result result1 = phase1(quorumPolicy, proposal);
                // 如果由于网络超时、票数未达法定人数（Quorum）或遇到更高编号导致失败，result1 会返回 null。此时执行 continue 放弃本轮并重试。
                if (result1 == null) {
                    continue;
                }
                // 阶段二准备：选择共识值
                // 根据第一阶段收集到的承诺消息（promisoryMessages），决定第二阶段要提议的共识值。
                // 根据 Paxos 核心规则：如果 Acceptors 在承诺时返回了以前已被接受的历史提案，Proposer 必须采纳其中编号最高者所对应的 Value；
                // 若历史记录为空，则提议本节点自身的建议值（Acceptor 提供的 Suggestion）。
                final Value phase2Value =
                    choosePhase2Value(result1.promisoryMessages);
                // 若因冲突或空值导致无法选出合法的 phase2Value，则 null 检查触发 continue 进入下一轮重试。
                if (phase2Value == null) {
                    continue;
                }
                // 阶段二：Accept 广播与确认
                // 会向集群广播 Accept 请求，携带选定的 proposal 和 phase2Value，要求 Acceptors 正式接受该决议。
                final Phase2Result result2 =
                    phase2(quorumPolicy, proposal, phase2Value,
                           result1.promisories);
                // 如果未能收集到法定数量的接受确认（Accepted），result2 返回 null，触发 continue 重试。
                if (result2 == null) {
                    continue;
                }
                // 如果未能收集到法定数量的接受确认（Accepted），result2 返回 null，触发 continue 重试。
                LoggerUtils.logMsg(logger, elections.getRepImpl(),
                                   formatter, Level.INFO,
                                   "Winning proposal: " + proposal +
                                   " Value: " + phase2Value);
                return new WinningProposal(proposal, phase2Value, statistics);
            } catch (HigherNumberedProposal eip) {
                continue;
            }
        }
        throw new MaxRetriesException(statistics);
    }

    /* Local classes */

    /**
     * Summarizes Phase 1 results
     */
    // 对 Paxos 协议第一阶段（Phase 1：Prepare / Promise 阶段）的执行结果进行汇总与封装
    // 当 Proposer 向集群中的 Acceptor 广播 Prepare 请求并收集到足够数量的响应后，会将这些响应数据整理并存储在 Phase1Result 实例中，供后续的共识值选择和第二阶段决策使用。
    // 汇总阶段一的投票承诺：收集并记录哪些 Acceptor 已经承诺（Promise）不再接受任何更低编号的提议。
    // 提取历史已接受的提议与值：顺带捕获 Acceptor 在响应时返回的、其历史上已经接受（Accepted）过的最高编号提议及其对应的共识值（Value）。这是满足 Paxos 核心约束（Phase 2 必须继承历史已被接受的值）的关键数据基础。
    private static class Phase1Result {

        /*
         * Subset of acceptors who have promised not to accept lower numbered
         * proposals.
         */
        // 给出承诺（Promise）的 Acceptor 节点 Socket 地址集合
        // 记录了在第一阶段成功回复 Promise 确认的所有 Acceptor 的网络地址。Proposer 正是通过计算这个集合的大小，并对照 QuorumPolicy（法定人数策略）来判断第一阶段收集到的票数是否已经达到了法定多数（Quorum）
        Set<InetSocketAddress> promisories = new HashSet<>();
        // 第一阶段完整的消息交换记录集合
        // 保存了 Proposer 与各个 Acceptor 之间进行 Prepare/Promise 交互的具体消息交换记录（MessageExchange）。这些消息中包含了 Acceptor 回复的详细状态，后续会被传入 choosePhase2Value(...) 方法中，用来分析和推导第二阶段应该采用哪个共识值。
        Set<MessageExchange> promisoryMessages =
            new HashSet<>();

        /*
         * The highest accepted proposal that was received from the
         * promisories.  It can be null.
         */
        // 自承诺节点中收集到的历史上已被接受的最高编号提议（可为空）
        // 如果某个 Acceptor 在回复 Promise 时，曾在此前接受过某一个提案，它会把该提案一同返回。Proposer 会从所有返回结果中挑出编号最高的一个记录在此属性中。
        // 若所有 Acceptor 此前均未接受过任何提议，则该字段保持为 null。
        Proposal acceptProposal;

        /*
         * The value associated with the proposal. It's non-null if
         * acceptProposal is not null.
         */
        // 与 acceptProposal 关联的共识值（Value）
        // 当 acceptProposal 不为 null 时，该字段存储的就是该历史提案所对应的具体共识值（例如当时被选出的 Master 价值）。根据 Paxos 规则，如果该字段非空，Proposer 在第二阶段就必须强制采用这个值，而不能随意更换。
        @SuppressWarnings("unused")
        Value acceptedValue;
    }

    /**
     * Summarizes Phase 2 results.
     */
    private static class Phase2Result {

        /* Subset of promisories who accepted the current proposal. */
        Set<InetSocketAddress> accepts = new HashSet<>();
    }

    /**
     * Summarizes the results of the election
     */
    // 对整场选举（Election）的最终成功结果进行全面总结与封装
    // 当 Proposer 顺利完成 Paxos 的第一阶段和第二阶段、成功让某个提案及共识值获得法定数量节点的接受后，
    // 便会生成一个 WinningProposal 对象，交由后台选举线程进行后续的胜选结果广播（通知 Learners）。
    // 封装选举胜利成果：将选主过程中产生的最终获胜提案、已被集群选定（Chosen）的共识值（Master 归属信息）以及整个提议生命周期中收集的性能统计指标聚合在一起。
    public static class WinningProposal {
        // 存储在选举中最终胜出的提议对象（Proposal）
        // 包含了全局唯一的提议编号（如基于时间戳和节点地址生成的编号），标识了这一轮取得共识的具体提案。
        final Proposal proposal;
        // 存储在选举中最终达成共识的共识值对象（Value）。
        // 通常代表了被选出的群组主节点（Master）的相关身份及价值信息。
        final Value chosenValue;
        // 存储 Proposer 在本轮胜选提议过程中收集的性能与状态统计指标组（StatGroup）
        final StatGroup proposerStats;

        public WinningProposal(Proposal proposal,
                               Value value,
                               StatGroup proposerStats) {
            this.proposal = proposal;
            this.chosenValue = value;
            this.proposerStats = proposerStats;
        }
    }

    /**
     * Exception to indicate that a more recent election is in progress.
     */
    @SuppressWarnings("serial")
    private static class HigherNumberedProposal extends RuntimeException {
        /* The acceptor that had a more recent proposal. */
        final InetSocketAddress acceptor;
        HigherNumberedProposal(InetSocketAddress acceptor) {
            this.acceptor = acceptor;
        }

        @Override
        public String getMessage() {
            return "More recent proposal at: " + acceptor.getHostName();
        }
    }

    /**
     * Exception thrown when a Proposal issue fails because the retry limit has
     * been exceeded.
     */
    // 当 Proposer（提议者）在发起选主提议（issueProposal）的过程中，因为重试次数耗尽而仍然未能成功达成共识时，抛出此异常以终止选举尝试
    // 通常发生在集群遭遇严重的网络分区、节点离线导致无法凑齐法定人数（Quorum）、或者频繁遭到更高编号提案竞争的极端场景下。
    @SuppressWarnings("serial")
    public static class MaxRetriesException extends Exception {
        final StatGroup proposerStats;

        MaxRetriesException(StatGroup proposerStats) {
            this.proposerStats = proposerStats;
        }
    }

    /**
     * Exception to indicate we have an early Quorum.
     */
    @SuppressWarnings("serial")
    private static class HaveQuorum extends RuntimeException {
        HaveQuorum() {}
    }

    /**
     * The Interface defining a Proposal.
     */
    public interface Proposal extends Comparable<Proposal> {

        /**
         * The string representation of the proposal. It must not contain
         * the Protocol.SEPARATOR character.
         *
         * @return the String representation of theProposal
         */
        String wireFormat();
    }

    public interface ProposalParser {

        /**
         * Converts the wire format back into a Proposal
         *
         * @param wireFormat String representation of a Proposal
         *
         * @return the de-serialized Proposal
         */
        abstract Proposal parse(String wireFormat);
    }

    /**
     * Interface to determine whether the Proposer should be retry with a new
     * Proposal or abandon the effort.
     */
    // 定义了一个策略契约，用于决定当 Proposer（提议者）的某一轮选主提议失败时，是应该携带新提案继续重试，还是彻底放弃并终止选举。
    // 解耦重试策略与 Paxos 核心算法：将“什么时候该重试、重试前要退避多久、重试次数是否超限”等业务与调度策略抽象为一个接口，使底层的 Proposer 算法能够专注于 Paxos 两阶段提交本身，而不必关心具体的重试和休眠逻辑。
    public interface RetryPredicate {
        // 核心的决策与控制方法。每当一轮提议失败（例如遇到更高编号提案、第一/二阶段法定人数不足）时，Proposer 就会调用此方法来询问是否应当继续尝试。
        boolean retry() throws InterruptedException;
        // 状态查询方法。用于获取当前已经累计尝试过的重试次数。
        int retries();
    }
}
