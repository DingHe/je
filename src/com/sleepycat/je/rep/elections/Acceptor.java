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
import java.nio.channels.Channels;
import java.util.logging.Level;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator.Ranking;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Accept;
import com.sleepycat.je.rep.elections.Protocol.Propose;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.TextProtocol.InvalidMessageException;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Plays the role of Acceptor in the consensus algorithm. It runs in its
 * own thread listening for and responding to messages sent by Proposers.
 */
// 在共识算法中扮演 Acceptor（接受者）的角色。它运行在自己的独立线程中，监听并通过 ServiceDispatcher 接收来自 Proposer（提议者）的各类网络请求（如 Propose、Accept、Shutdown 等），校验提议编号后做出承诺（Promise）或接受确认（Accepted）。
// 执行 Paxos 协议中的 Acceptor 职责：作为分布式共识的“存储与承诺节点”，负责维护本地的最高承诺提议（promisedProposal）和已接受值（acceptedValue）
// 处理对立竞争提议：当收到 Proposer 的 Propose（Prepare）或 Accept 请求时，比对提议编号。若编号小于已承诺的编号则果断拒绝（Reject），否则给予承诺（Promise）或接受（Accepted）。
// 提供建议与排名生成机制：通过内置的 SuggestionGenerator 接口，在响应 Proposer 时提供当前节点的最优建议值及其相对排名（Ranking），协助 Proposer 选出最合适的 Master。
public class Acceptor extends ElectionAgentThread {

    /*
     * The currently promised proposal. Proposals below this one are rejected.
     */
    // 当前已经许下承诺（Promised）的最高提议对象。
    // 这是 Paxos 协议中 Acceptor 的关键状态。任何编号小于此提议的后续请求都将被拒绝。初始化为空。
    private Proposal promisedProposal = null;
    // 当前节点历史上已经被正式接受（Accepted）的共识值。
    // 在 Paxos 阶段二成功接受某个提议后更新，用于在后续选举中向 Proposer 汇报历史状态。
    private Value acceptedValue = null;

    /* Used to return suggestions in response to Propose requests. */
    // 建议生成器实例。
    // 用于在响应 Proposer 的 Propose 请求时，动态生成当前节点的建议值（Suggestion）及其对应的双维权重排名（Ranking）。
    private final SuggestionGenerator suggestionGenerator;

    /* Identifies the Acceptor Service. */
    // Acceptor 服务注册名称常量。
    public static final String SERVICE_NAME = "Acceptor";
    // 选举配置管理器实例。
    private final ElectionsConfig config;

    /**
     * Creates an Acceptor
     */
    public Acceptor(Protocol protocol,
                    ElectionsConfig config,
                    SuggestionGenerator suggestionGenerator) {

        super(config.getRepImpl(), protocol,
              "Acceptor Thread " + config.getNameIdPair().getName());
        this.config = config;

        this.suggestionGenerator = suggestionGenerator;
    }

    /**
     * The Acceptor thread body.
     */
    // 责在启动时向底层的 ServiceDispatcher 注册服务通道，进入无限循环以阻塞监听、接收并解析来自 Proposer 的网络请求（如 Propose、Accept、Shutdown 等），
    // 处理并分发对应的业务逻辑，同时将响应回传，并在线程退出时安全清理资源。
    @Override
    public void run() {
        // 服务初始化与注册
        final ServiceDispatcher serviceDispatcher =
            config.getServiceDispatcher();
        // 将 SERVICE_NAME（即 "Acceptor"）与内部的通道队列（channelQueue）进行注册绑定。此后，任何发往该服务的网络连接都会被分发到这个队列中。
        serviceDispatcher.register(SERVICE_NAME, channelQueue);
        LoggerUtils.logMsg
            (logger, envImpl, formatter, Level.FINE, "Acceptor started");
        DataChannel channel = null;
        try {
            while (true) {
                // 从队列中获取连接通道
                channel = serviceDispatcher.takeChannel
                    (SERVICE_NAME, true /* block */,
                     protocol.getReadTimeout());

                if (channel == null) {
                    /* A soft shutdown. */
                    return;
                }
                // 数据流初始化与请求行读取
                BufferedReader in = null;
                PrintWriter out = null;
                try {
                    in = new BufferedReader(
                        new InputStreamReader(
                            Channels.newInputStream(channel)));
                    out = new PrintWriter(
                        Channels.newOutputStream(channel), true);
                    String requestLine = in.readLine();
                    if (requestLine == null) {
                        LoggerUtils.logMsg(logger, envImpl,
                                           formatter, Level.FINE,
                                           "Acceptor: EOF on request");
                        continue;
                    }
                    // 协议请求解析与错误防御
                    RequestMessage requestMessage = null;
                    try {
                        requestMessage = protocol.parseRequest(requestLine);
                    } catch (InvalidMessageException ime) {
                        protocol.processIME(channel, ime);
                        continue;
                    }
                    // 请求类型多路分发 (Dispatch)
                    ResponseMessage responseMessage = null;
                    if (requestMessage.getOp() == protocol.PROPOSE) {
                        responseMessage = process((Propose) requestMessage);
                    } else if (requestMessage.getOp() == protocol.ACCEPT) {
                        responseMessage = process((Accept) requestMessage);
                    } else if (requestMessage.getOp() == protocol.SHUTDOWN) {
                        break;
                    } else {
                        LoggerUtils.logMsg(logger, envImpl,
                                           formatter, Level.SEVERE,
                                           "Unrecognized request: " +
                                           requestLine);
                        continue;
                    }

                    /*
                     * The request message may be of an earlier version. If so,
                     * this node transparently read the older version. JE only
                     * throws out InvalidMesageException when the version of
                     * the request message is newer than the current protocol.
                     * To avoid sending a repsonse that the requester cannot
                     * understand, we send a response in the same version as
                     * that of the original request message.
                     */
                    responseMessage.setSendVersion
                        (requestMessage.getSendVersion());
                    out.println(responseMessage.wireFormat());
                } catch (IOException e) {
                    LoggerUtils.logMsg
                        (logger, envImpl, formatter, Level.WARNING,
                         "IO error on socket: " + e.getMessage());
                    continue;
                } finally {
                    Utils.cleanup(logger, envImpl, formatter, channel, in, out);
                    cleanup();
                }
            }
        } catch (InterruptedException e) {
            if (isShutdown()) {
                /* Treat it like a shutdown, exit the thread. */
                return;
            }
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.WARNING,
                               "Acceptor unexpected interrupted");
            throw EnvironmentFailureException.unexpectedException(e);
        } finally {
            serviceDispatcher.cancel(SERVICE_NAME);
            cleanup();
        }
    }

    /**
     * Responds to a Propose request.
     *
     * @param propose the request proposal
     *
     * @return the response: a Promise if the request was accepted, a Reject
     *         otherwise.
     */
    // Acceptor 处理来自 Proposer 的 Propose（对应 Paxos 算法第一阶段的 Prepare/Propose 请求）的核心响应方法。
    // 对接收到的提议编号进行安全性校验；若提议编号过时则予以拒绝（Reject），若合规则更新本地承诺（Promise），并附带本地的历史已接受值、当前建议值及权重排名等关键状态回复给 Proposer。
    // Propose propose：封装了 Proposer 发送过来的新提议对象的请求消息。其中包含了 Proposer 想要推进的 Proposal 编号。
    ResponseMessage process(Propose propose) {
        // 提案编号安全校验（防过时拒绝逻辑）
        // 实现 Paxos 协议的第一道安全防线——拒绝编号小于当前已承诺提议的请求
        // 使用 compareTo 比较本地已有的 promisedProposal 与当前收到的 propose.getProposal()。如果本地的更大（即 > 0），说明这个新请求的提议编号太旧了，已经被更靠后的提议超越。
        if ((promisedProposal != null) &&
            (promisedProposal.compareTo(propose.getProposal()) > 0)) {
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                               "Reject Propose: " + propose.getProposal() +
                               " Promised proposal: " + promisedProposal);
            return protocol.new Reject(promisedProposal);
        }
        // 更新本地承诺提议
        // 对新提议做出承诺。
        promisedProposal = propose.getProposal();
        // 生成建议值与排名
        // 获取当前节点针对该提议给出的建议值（suggestedValue，例如包含当前节点的 LSN 状态、Master 身份建议等）
        final Value suggestedValue = suggestionGenerator.get(promisedProposal);
        // 获取该建议值对应的双维权重排名
        final Ranking suggestionRanking =
            suggestionGenerator.getRanking(promisedProposal);
        LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                           "Promised: " + promisedProposal +
                           " Suggested Value: " + suggestedValue +
                           " Suggestion Ranking: " + suggestionRanking);
        // 组装并返回 Promise 响应
        return protocol.new Promise
                (promisedProposal,
                 acceptedValue,
                 suggestedValue,
                 suggestionRanking,
                 config.getElectionPriority(),
                 config.getLogVersion(),
                 JEVersion.CURRENT_VERSION);
    }

    /**
     * Responds to Accept request
     *
     * @param accept the request
     * @return an Accepted or Reject response as appropriate.
     */
    // Acceptor 处理来自 Proposer 的 Accept（对应 Paxos 算法第二阶段的 Accept 请求）的核心响应方法。
    // Accept accept：封装了 Proposer 发送过来的第二阶段 Accept 请求消息。其中包含了 Proposer 想要集群正式采纳的 Proposal 编号以及对应的 Value（共识值）。
    ResponseMessage process(Accept accept) {
        // 提议编号一致性校验（防跨代接受逻辑）
        // 拒绝编号与当前承诺不符的 Accept 请求。
        // 在第二阶段，请求的提议编号必须恰好等于本地此前在第一阶段中做出过承诺的那个编号（即不等于 0）。如果编号不一致（说明在此期间有更高编号的 Prepare 插入，导致当前承诺被覆盖或作废），则认为该 Accept 请求失效。
        if ((promisedProposal != null) &&
            (promisedProposal.compareTo(accept.getProposal()) != 0)) {
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                               "Reject Accept: " + accept.getProposal() +
                               " Promised proposal: " + promisedProposal);
            return protocol.new Reject(promisedProposal);
        }
        // 正式锁定并记录已接受的值
        acceptedValue = accept.getValue();
        LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                           "Promised: " + promisedProposal + " Accepted: " +
                           accept.getProposal() + " Value: " + acceptedValue);
        // 组装并返回 Accepted 确认响应
        return protocol.new Accepted(accept.getProposal(), acceptedValue);
    }
    // 为 Acceptor 提供一种生成“建议值（Suggestion）”与“权重排名（Ranking）”的策略契约。
    // 当 Acceptor 收到 Proposer 的 Propose（Prepare）请求并回复 Promise 时，会通过该接口动态获取当前节点的数据状态提示及新鲜度排名，从而协助 Proposer 在第二阶段挑选出最优的 Master 候选者。
    // 解耦业务状态与共识算法：将底层数据库的具体状态（如事务日志位置、VLSN 虚拟日志序列号等）与 Paxos 共识协议本身解耦。Acceptor 无需硬编码具体的业务逻辑，而是通过实现该接口来提供建议。
    // 状态冻结与一致性保护：正如注释中所提及的，传入的 proposal 参数可用于在选举期间“冻结”某些本地指标（例如防止 VLSN 继续推进），确保在选主尘埃落定之前，节点的数据视图保持相对稳定。
    // 统一排名度量体系：通过配合 getRanking 方法，确保集群中各个 Acceptor 采用相同的标准去衡量自身数据的重要性，使 Proposer 能够对来自不同节点的建议进行有意义的横向对比。
    public interface SuggestionGenerator {

        /**
         * Used to generate a suggested value for use by a Proposer. It's a
         * hint.  The proposal argument may be used to freeze values like the
         * VLSN number from advancing (if they were used in the ranking) until
         * an election has completed.
         *
         * @param proposal the Proposal for which the value is being suggested.
         *
         * @return the suggested value.
         */
        // 根据传入的提议对象，生成并返回一个建议值（Suggested Value），作为给 Proposer 的决策提示（Hint）。
        abstract Value get(Proposal proposal);

        /**
         * The importance associated with the above suggestion. Acceptors have
         * to agree on a common system for ranking importance so that the
         * relative importance of different suggestions can be meaningfully
         * compared.
         *
         * @param the proposal associated with the ranking
         *
         * @return the importance of the suggestion as a number
         */
        // 算并返回与上述建议值关联的双维权重排名（Ranking）
        abstract Ranking getRanking(Proposal proposal);

        /**
         * A description of the ranking used when comparing Promises to pick a
         * Master.
         */
        // 为比较来自各个 Acceptor 的 Promise 响应提供一套结构化的排名描述体系。
        // 在选主过程中，系统需要评估哪个节点的数据更新、更有资格当选 Master，Ranking 通过引入主版本/主分量（Major）与次版本/次分量（Minor）的双维权重机制，实现了精准的优先级排序。
        // 量化节点数据新鲜度与资格：将复杂的节点状态（如事务日志 LSN、版本号或序列）抽象为一个包含 major 和 minor 的复合数值指标，便于量化比较。
        // 提供标准的可比性契约：实现了 Comparable<Ranking> 接口，支持通过 compareTo 方法直接对两个排名对象进行大小判定，确保在选择 Phase 2 共识值时能够迅速挑出最优解。
        class Ranking implements Comparable<Ranking> {
            /* The major component of the ranking. */
            // 主分量（Major Component）
            // 通常代表最核心、权重最高的权威指标（例如高阶事务序列号、世代号或核心日志位置）。在两维比较中具有一票否决权，只有当 major 相等时才会去比较次要分量。
            final long major;

            /* The minor component. */
            // 次分量（Minor Component）
            // 作为辅助指标（例如较小粒度的日志偏移量或附加状态计数）。当 major 相同（打平）时，用它来决出胜负。
            final long minor;
            // 未初始化默认常量
            static Ranking UNINITIALIZED =
                new Ranking(Long.MIN_VALUE, Long.MIN_VALUE);

            public Ranking(long major, long minor) {
                this.major = major;
                this.minor = minor;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + (int) (major ^ (major >>> 32));
                result = prime * result + (int) (minor ^ (minor >>> 32));
                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                Ranking other = (Ranking) obj;
                if (major != other.major) {
                    return false;
                }
                if (minor != other.minor) {
                    return false;
                }
                return true;
            }

            @Override
            public String toString() {
                return "Ranking major:" + major + " minor:" + minor;
            }

            @Override
            public int compareTo(Ranking o) {
                int result = Long.compare(major, o.major);
                if (result != 0) {
                    return result;
                }
                return Long.compare(minor, o.minor);
            }
        }
    }
}
