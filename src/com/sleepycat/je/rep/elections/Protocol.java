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

import static com.sleepycat.je.rep.impl.RepParams.ELECTIONS_OPEN_TIMEOUT;
import static com.sleepycat.je.rep.impl.RepParams.ELECTIONS_READ_TIMEOUT;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator.Ranking;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Proposer.ProposalParser;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.TextProtocol;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannelFactory;

/**
 * Defines the request/response messages used in the implementation of
 * elections.
 *
 * From Proposer to Acceptor:
 *     Propose -> Promise | Reject
 *     Accept -> Accepted | Reject
 *
 * From Proposer initiator to Learners:
 *    Result -> none
 *
 * The following exchange is not part of the elections process, but is used by
 * the Monitor to query a Learner for the latest election result it's aware of,
 * when the Monitor first starts up.  It is also used by nodes and utilities
 * that are attempting to find the master.
 *
 * From Monitor to Learner
 *    MasterQuery -> MasterQueryResponse | None
 *
 */
// 实现节点间选举（Elections，基于 Paxos 协议）所使用的文本通信协议的具体定义类
// 完整定义了 Paxos 选举各阶段（如 Propose -> Promise/Reject、Accept -> Accepted/Reject、Result 通知、MasterQuery 查询等）所需的全部请求与响应消息格式、操作符以及相关解析器。
// 定义选举协议规范：为分布式节点选举过程中的各种交互消息（提案、承诺、接受、拒绝、结果通知及主节点查询）提供统一的序列化与反序列化支持。
// 封装 Paxos 报文体系：通过内部类形式实现了所有特定的请求（Request）与响应（Response）消息体，规范了集群内节点如何就“谁是主节点（Master）”达成共识。
// 支持外部工具和监控查询：除了核心的 Paxos 选举交互外，还支持如 MasterQuery（供 Monitor 或启动中的节点寻找当前 Master）等辅助查询协议。
public class Protocol extends TextProtocol {

    /* Protocol version string. Format: <major version>.<minor version> */
    /* It's used to ensure compatibility across versions. */
    // 当前选举协议的特定版本号（2.0），用于跨版本的兼容性校验。
    private static final String VERSION = "2.0";

    /* An instance of ProposalParser used to de-serialize proposals */
    // 提案解析器实例。
    // 用于将网络传输的字符串反序列化为结构化的 Proposal 对象。
    private final ProposalParser proposalParser;

    /* An instance of ValueParser used to de-serialize values */
    // 值解析器实例。
    // 用于将网络传输的字符串反序列化为实现了 Value 接口的对象。
    private final ValueParser valueParser;

    /* Request Operations */
    // 请求操作符常量（MessageOp）：
    // Paxos 第一阶段的提案请求。
    public final MessageOp PROPOSE;
    // Paxos 第二阶段的接受请求。
    public final MessageOp ACCEPT;
    // 向学习者（Learners）广播最终选出结果的通知。
    public final MessageOp RESULT;
    // 查询主节点的请求。
    public final MessageOp MASTER_QUERY;
    // 关闭选举参与者的请求。
    public final MessageOp SHUTDOWN;

    /* Response operations */
    // 拒绝提案的响应。
    public final MessageOp REJECT;
    // 对提案做出承诺（Promise）的响应。
    public final MessageOp PROMISE;
    // 接受提案成功的响应。
    public final MessageOp ACCEPTED;
    // 主节点查询的返回响应。
    public final MessageOp MASTER_QUERY_RESPONSE;

    /**
     * Creates an instance of the Protocol.
     *
     * @param proposalParser parses a string into a Proposal object.
     * @param valueParser parses a string into a Value object.
     * @parameter groupName the name of the group running the election process.
     * @param nameIdPair a unique identifier for this election participant.
     */
    public Protocol(ProposalParser proposalParser,
                    ValueParser valueParser,
                    String  groupName,
                    NameIdPair nameIdPair,
                    RepImpl repImpl,
                    DataChannelFactory channelFactory) {

        /* Request operations */
        super(VERSION, groupName, nameIdPair, repImpl, channelFactory);
        // 初始化所有 Paxos 请求和响应的 MessageOp 操作符
        PROPOSE = new MessageOp("P", Propose.class);
        ACCEPT = new MessageOp("A", Accept.class);
        RESULT = new MessageOp("RE", Result.class);
        MASTER_QUERY = new MessageOp("MQ", MasterQuery.class);
        SHUTDOWN = new MessageOp("X", Shutdown.class );

        REJECT = new MessageOp("R", Reject.class);
        PROMISE = new MessageOp("PR", Promise.class);
        ACCEPTED = new MessageOp("AD", Accepted.class);
        MASTER_QUERY_RESPONSE =
            new MessageOp("MQR", MasterQueryResponse.class);
        // 将所有定义好的操作符批量注册到基类的分发映射表中。
        initializeMessageOps(new MessageOp[] {
                PROPOSE,
                ACCEPT,
                RESULT,
                MASTER_QUERY,
                SHUTDOWN,

                REJECT,
                PROMISE,
                ACCEPTED,
                MASTER_QUERY_RESPONSE,
        });
        this.proposalParser = proposalParser;
        this.valueParser = valueParser;
        // 设置选举专属的打开和读取网络超时配置（ELECTIONS_OPEN_TIMEOUT 和 ELECTIONS_READ_TIMEOUT）
        setTimeouts(repImpl, ELECTIONS_OPEN_TIMEOUT, ELECTIONS_READ_TIMEOUT);
    }

    /**
     * Promise response message. It's sent in response to a Propose message.
     *
     * Note that the "minor" part of the suggestion ranking is always tagged on
     * to the end of the promise request payload. Older pre 7.1.3 nodes will
     * ignore extra tokens at the end, since they do not know about the minor
     * component of the ranking. This node will use it if it's present and
     * otherwise use a value of zero for the minor (VLSN) component.
     *
     * So when comparing rankings across old and new nodes, we are effectively
     * comparing a Ranking(VLSN, Long.MIN_VALUE) with a Ranking(DTVLSN, VLSN),
     * resulting in suboptimal election results (from a dtvlsn perspective)
     * while an upgrade is in progress, that is, it will tend to favor the
     * older node. But this inaccuracy will vanish once all nodes have been
     * upgraded.
     */
    // 代表 Paxos 分布式共识协议中对 Propose（提案）请求的承诺响应（Promise / Phase 1b 响应）。
    // 当 Acceptor（接受者）收到 Proposer 发送的 Propose 消息后，如果该提案的编号高于 Acceptor 见过的所有提案编号，Acceptor 就会向 Proposer 回复一个 Promise 消息，承诺不再接受编号小于该提案的新提案，
    // 并向 Proposer 汇报自己此前接受过的最高编号提案及相关状态。
    // 实现 Paxos 第一阶段承诺：携带 Acceptor 的状态返回给 Proposer，包括其见过的最高提案、已接受的值、建议值、排名（Ranking）、节点优先级等。
    // 支持混合版本兼容与平滑升级：源码注释中明确提到，自 7.1.3 版本起，Ranking 引入了 major（主版本/DTVLSN）和 minor（次版本/最新 VLSN）组件。该类通过版本判断和可选的 Token 读取，完美实现了新老版本节点混用时的向后兼容。
    public class Promise extends ResponseMessage {
        // Acceptor 曾经承诺或接受过的最高编号的提案（Highest Proposal）
        private Proposal highestProposal = null;
        // 与 highestProposal 关联的已被接受的值（Accepted Value）
        private Value acceptedValue = null;
        // Acceptor 向 Proposer 提供的建议值（Suggestion），用于帮助 Proposer 选取具有延续性的共识值。
        private Value suggestion = null;

        /**
         * The major and minor components of the Ranking represent the DTVLSN
         * and the latest VLSN respectively.
         */
        // 建议值的排序评级（Ranking）。
        // 由 major（通常代表 DTVLSN）和 minor（代表最新 VLSN）组成，用于在选举时判定哪个节点的数据更新、更有资格当选主节点。
        private final Ranking suggestionRanking;
        // 该节点的选举优先级权重（Priority），用于优先级的竞选 tie-breaker。
        private final int priority;
        // 日志格式版本号（在协议版本大于 1 时启用）。
        private int logVersion;
        // 节点的 JE 软件版本号（在协议版本大于 1 时启用）。
        private JEVersion jeVersion;

        public Promise(Proposal highestProposal,
                       Value value,
                       Value suggestion,
                       Ranking suggestionRanking,
                       int priority,
                       int logVersion,
                       JEVersion jeVersion) {
            this.highestProposal = highestProposal;
            this.acceptedValue = value;
            this.suggestion = suggestion;
            this.suggestionRanking = suggestionRanking ;
            this.priority = priority;
            this.logVersion = logVersion;
            this.jeVersion = jeVersion;
        }
        // 标准本地构造器。用于在 Acceptor 端组装好所有的状态参数，准备回复给 Proposer。
        // 将传入的各个核心字段（最高提案、接受值、建议值、评级、优先级、日志版本、JE 版本）直接赋值给对应的成员变量。
        public Promise(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            highestProposal = proposalParser.parse(nextPayloadToken());
            acceptedValue = valueParser.parse(nextPayloadToken());
            suggestion = valueParser.parse(nextPayloadToken());
            String weight = nextPayloadToken();
            long majorRanking =
                "".equals(weight) ?
                Ranking.UNINITIALIZED.major :
                Long.parseLong(weight);
            long minorRanking = Ranking.UNINITIALIZED.major;
            priority = Integer.parseInt(nextPayloadToken());
            if (getMajorVersionNumber(sendVersion) > 1) {
                logVersion = Integer.parseInt(nextPayloadToken());
                jeVersion = new JEVersion(nextPayloadToken());
                if (hasMoreTokens()) {
                    /*
                     * The tie breaker is appended to the end by newer versions
                     * of JE nodes >= version 7.1.3
                     */
                    minorRanking = Long.parseLong(nextPayloadToken());
                }
            }
            suggestionRanking = new Ranking(majorRanking, minorRanking);
        }

        @Override
        public MessageOp getOp() {
            return PROMISE;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + ((acceptedValue == null) ? 0 : acceptedValue.hashCode());
            result = prime
                    * result
                    + ((highestProposal == null) ? 0
                            : highestProposal.hashCode());
            result = prime * result + priority;
            result = prime * result
                    + ((suggestion == null) ? 0 : suggestion.hashCode());
            result = prime * result + suggestionRanking.hashCode();

            if (getMajorVersionNumber(sendVersion) > 1) {
                result += prime* result + logVersion + jeVersion.hashCode();
            }

            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (!super.equals(obj)) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            Promise other = (Promise) obj;
            if (!getOuterType().equals(other.getOuterType())) {
                return false;
            }

            if (acceptedValue == null) {
                if (other.acceptedValue != null) {
                    return false;
                }
            } else if (!acceptedValue.equals(other.acceptedValue)) {
                return false;
            }

            if (highestProposal == null) {
                if (other.highestProposal != null) {
                    return false;
                }
            } else if (!highestProposal.equals(other.highestProposal)) {
                return false;
            }

            if (priority != other.priority) {
                return false;
            }

            if (getMajorVersionNumber(sendVersion) > 1) {
                if (logVersion != other.logVersion) {
                    return false;
                }

                if (jeVersion.compareTo(other.jeVersion) != 0) {
                    return false;
                }
            }

            if (suggestion == null) {
                if (other.suggestion != null) {
                    return false;
                }
            } else if (!suggestion.equals(other.suggestion)) {
                return false;
            }

            if (!suggestionRanking.equals(other.suggestionRanking)) {
                return false;
            }

            return true;
        }
        // 将当前的 Promise 对象序列化为适合网络传输的文本字符串。
        // 将前缀、最高提案、接受值、建议值、主要评级、优先级用分隔符 | 串联；如果协议版本大于 1，则追加 logVersion、jeVersion 以及次要评级（minorRanking）。
        @Override
        public String wireFormat() {
            String line =
                wireFormatPrefix() +
                SEPARATOR +
                ((highestProposal != null) ?
                 highestProposal.wireFormat() :
                 "") +
                SEPARATOR +
                ((acceptedValue != null) ? acceptedValue.wireFormat() : "") +
                SEPARATOR +
                ((suggestion != null) ?  suggestion.wireFormat() : "") +
                SEPARATOR +
                ((suggestionRanking.major == Long.MIN_VALUE) ?
                 "" :
                 Long.toString(suggestionRanking.major)) +
                 SEPARATOR +
                 priority;

           if (getMajorVersionNumber(sendVersion) > 1) {
              line += SEPARATOR + logVersion +
                      SEPARATOR + jeVersion.toString() +
                      SEPARATOR + Long.toString(suggestionRanking.minor);
           }

           return line;
        }

        Proposal getHighestProposal() {
            return highestProposal;
        }

        Value getAcceptedValue() {
            return acceptedValue;
        }

        Value getSuggestion() {
            return suggestion;
        }

        Ranking getSuggestionRanking() {
            return suggestionRanking;
        }

        int getPriority() {
            return priority;
        }

        int getLogVersion() {
            return logVersion;
        }

        JEVersion getJEVersion() {
            return jeVersion;
        }

        private Protocol getOuterType() {
            return Protocol.this;
        }
    }

    /**
     * Response to a successful Accept message.
     */
    // 表示对 Paxos 协议第二阶段中成功 Accept（接受）请求的肯定响应（Accepted 响应）。
    // 当 Acceptor（接受者）成功接收并批准了 Proposer 提交的提案值后，便会向 Proposer 发送该 Accepted 消息，表明自己已经正式接受了该提案。
    // 实现 Paxos 第二阶段确认：承载并向 Proposer 回传已被接受的提案（Proposal）以及对应的值（Value）。
    // 支持双向序列化：既允许在本地 Acceptor 端根据具体的提案和值快速组装响应消息，也支持在 Proposer 端通过反射与 Token 解析将网络接收到的文本行反序列化为结构化的 Accepted 对象。
    // 保证一致性比较：重写了 hashCode 和 equals 方法，确保系统在对投票或响应进行去重、集合比较时能够准确依据提案内容进行判断。
    public class Accepted extends ResponseMessage {
        // 已被 Acceptor 成功接受的提案对象（包含提案编号、Proposer 标识等）。
        private final Proposal proposal;
        // 与该提案绑定的具体共识值（Value）。
        private final Value value;

        Accepted(Proposal proposal, Value value) {
            assert(proposal!= null);
            assert(value != null);
            this.proposal = proposal;
            this.value = value;
        }

        public Accepted(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            proposal = proposalParser.parse(nextPayloadToken());
            value  = valueParser.parse(nextPayloadToken());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result
                    + ((proposal == null) ? 0 : proposal.hashCode());
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Accepted)) {
                return false;
            }
            final Accepted other = (Accepted) obj;
            if (proposal == null) {
                if (other.proposal != null) {
                    return false;
                }
            } else if (!proposal.equals(other.proposal)) {
                return false;
            }
            if (value == null) {
                if (other.value != null) {
                    return false;
                }
            } else if (!value.equals(other.value)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
            return ACCEPTED;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR + proposal.wireFormat() +
                SEPARATOR + value.wireFormat();
        }

        public Value getValue() {
            return value;
        }

        public Proposal getProposal() {
            return proposal;
        }
    }

    /**
     * The response to a Master Query request. It simply repackages the
     * Accepted response.
     */
    // 作为对 MasterQuery（主节点查询）请求的响应。
    // 正如源码注释所言，它并没有发明新的数据结构，而是简单地对 Accepted 响应进行了重新打包（Repackages），
    // 复用了已有的提案（Proposal）和共识值（Value）结构，用来向查询者报告当前集群中已被确认的主节点信息。
    // 响应主节点查询：当 Monitor 监控进程或正在启动中的节点发送 MasterQuery 请求、试图寻找当前集群的 Master 时，由 Learner 或主节点返回该类型的响应报文。
    // 免检查前缀路由：重写了获取消息前缀的方法，使用无需严格权限校验的特殊前缀（messagePrefixNocheck），以便让尚未完全加入群组或处于引导状态的外部查询方能够顺利完成主节点发现。
    public class MasterQueryResponse extends Accepted {

        MasterQueryResponse(Proposal proposal, Value value) {
            super(proposal, value);
        }

        public MasterQueryResponse(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
        }
        @Override
        public MessageOp getOp() {
            return MASTER_QUERY_RESPONSE;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }
    }

    /**
     * Reject response to a message.
     */
    // 表示对 Paxos 提案请求（如 Propose 或 Accept）的拒绝响应（Reject 响应）。
    // 当 Acceptor（接受者）收到来自 Proposer 的提案，但发现该提案的编号不够高（即 Acceptor 已经见过或承诺过更高编号的提案）时，就会拒绝该请求，
    // 并向 Proposer 回复一个 Reject 消息，同时顺带附上自己见过的更高编号的提案（higherProposal），从而帮助 Proposer 尽快调整自己的提案编号并重新发起选举。
    public class Reject extends ResponseMessage {
        // 存储导致当前请求被拒绝的、由 Acceptor 所知的更高编号的提案对象。
        private final Proposal higherProposal;
        // 包可见（Package-private）本地构造器。用于在 Acceptor 端创建 Reject 响应实例。
        Reject(Proposal higherProposal) {
            this.higherProposal = higherProposal;
        }
        // 反序列化构造器。由 parse() 方法通过反射调用，用于将网络接收到的文本行解析为结构化的 Reject 对象。
        public Reject(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            higherProposal = proposalParser.parse(nextPayloadToken());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result +
                ((higherProposal == null) ? 0 : higherProposal.hashCode());

            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Reject)) {
                return false;
            }
            final Reject other = (Reject) obj;
            if (higherProposal == null) {
                if (other.higherProposal != null) {
                    return false;
                }
            } else if (!higherProposal.equals(other.higherProposal)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
            return REJECT;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR + higherProposal.wireFormat();
        }

        Proposal getHigherProposal() {
            return higherProposal;
        }
    }

    /**
     * Propose request used in Phase 1 of Paxos
     */
    // 核心作用是：表示 Paxos 协议第一阶段（Phase 1a）中由 Proposer 发起的提案请求（Propose 请求）。
    // 当一个节点想要竞选主节点（Master）或发起新一轮选举时，它会向集群中的 Acceptor 节点广播该 Propose 消息，以征求大家的承诺（Promise）
    // 实现 Paxos 第一阶段提案发起：承载并向 Acceptor 传递由 Proposer 生成的新提案（Proposal）。
    // 支持双向序列化：既允许在 Proposer 端根据具体的提案对象快速组装请求报文，也支持在 Acceptor 端通过反射与 Token 解析将网络接收到的文本行反序列化为结构化的 Propose 对象。
    public class Propose extends RequestMessage {
        // 存储当前提议请求所包含的提案对象（Proposal），其中包含了提案的编号、发起者的身份等核心共识数据。
        protected final Proposal proposal;

        Propose(Proposal proposal) {
            this.proposal = proposal;
        }

        public Propose(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
            proposal = proposalParser.parse(nextPayloadToken());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result
                    + ((proposal == null) ? 0 : proposal.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Propose)) {
                return false;
            }
            final Propose other = (Propose) obj;
            if (proposal == null) {
                if (other.proposal != null) {
                    return false;
                }
            } else if (!proposal.equals(other.proposal)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
            return PROPOSE;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR +  proposal.wireFormat();
        }

        Proposal getProposal() {
            return proposal;
        }
    }
    // 表示关闭选举参与者（Shutdown 请求）的控制命令消息。当集群需要安全关闭某个正在参与选举或运行的节点时，可以通过发送该请求，通知接收方终止当前的选举进程或释放相关资源。
    // 传递关闭指令：作为一种轻量级的控制信令，向远程节点发送停止运行或退出选举的请求。
    // 支持双向序列化：既允许在本地客户端或控制端直接构建一个全新的 Shutdown 请求对象，也支持接收端通过反射与 Token 解析将网络接收到的文本行反序列化为合法的 Shutdown 实例。
    public class Shutdown extends RequestMessage {

        public Shutdown() {}

        public Shutdown(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
        }

        @Override
        public MessageOp getOp() {
            return SHUTDOWN;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix();
        }

    }

    /**
     * Accept request issued in Phase 2 of paxos.
     */
    // 表示 Paxos 协议第二阶段（Phase 2a）中由 Proposer 发起的 Accept 请求。
    // 当 Proposer 在第一阶段成功收集到法定数量的 Promise 承诺后，便会进入第二阶段，向集群中的 Acceptor 节点广播该 Accept 消息，正式请求大家接受某个特定的提案编号及其对应的共识值（Value）。
    // 实现 Paxos 第二阶段接受请求：在继承第一阶段 Propose（包含提案 Proposal）的基础上，进一步携带了要达成共识的具体值（Value）。
    //
    public class Accept extends Propose {
        // 存储当前 Accept 请求所包含的共识值对象（Value）。这是 Paxos 第二阶段要求 Acceptor 确认并接受的核心业务数据。
        private final Value value;
        // 显式调用父类构造器 super(proposal) 初始化提案部分，并将传入的 value 赋值给类成员变量。
        Accept(Proposal proposal, Value value) {
            super(proposal);
            this.value = value;
        }

        public Accept(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
            value = valueParser.parse(nextPayloadToken());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Accept)) {
                return false;
            }
            final Accept other = (Accept) obj;
            if (value == null) {
                if (other.value != null) {
                    return false;
                }
            } else if (!value.equals(other.value)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
            return ACCEPT;
        }

        @Override
        public String wireFormat() {
            return super.wireFormat() + SEPARATOR + value.wireFormat();
        }

        Value getValue() {
            return value;
        }
    }

    /**
     * Used to inform Learners of a "chosen value".
     */
    // 用于向集群中的学习者（Learners）广播或通知已被选定（Chosen）的共识值。
    // 在 Paxos 协议成功达成共识（即某个提案及其值被法定数量的 Acceptor 接受）后，协调节点或 Proposer 会向集群中的 Learners 发送该 Result 消息，以便所有节点同步并确认最终的选举或决策结果。
    public class Result extends Accept {

        Result(Proposal proposal, Value value) {
            super(proposal, value);
        }

        public Result(String requestLine, String[] tokens)
            throws InvalidMessageException {
            super(requestLine, tokens);
        }

        @Override
        public MessageOp getOp() {
            return RESULT;
        }
    }

    /**
     * Used to query the Learner for a current master
     */
    // 用于向学习者（Learner）或其他节点查询当前集群中已知的主节点（Master）信息
    // 实现主节点发现与查询：提供一种轻量级的控制信令，允许外部或孤立节点向集群中的 Learner 节点打听当前的 Master 归属。
    // 支持免检查前缀路由：重写了获取消息前缀的方法，使用无需严格权限校验的特殊前缀（messagePrefixNocheck），确保处于引导阶段或未完全加入群组的外部查询方能够顺利发起请求。
    public class MasterQuery extends RequestMessage {

        public MasterQuery() {}

        public MasterQuery(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
        }

        @Override
        public MessageOp getOp() {
            return MASTER_QUERY;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix();
        }

        @Override
        public String toString() {
            return getOp() + " " + getMessagePrefix() + " " + wireFormat();
        }
    }

    /* Represents a Value in Paxos. */
    public interface Value extends WireFormatable  {
    }

    public interface ValueParser {
        /**
         * Converts the wire format back into a Value
         *
         * @param wireFormat String representation of a Value
         *
         *
         * @return the de-serialized Value
         *
         */
        abstract Value parse(String wireFormat);
    }

    /**
     * A String based value implementation used as the "default" Value
     */
    // 核心作用是：作为一个基于字符串的共识值实现，被用作 Paxos 协议中的“默认”共识值（Value）类型。
    // 当选举出的 Master 节点身份、状态或其他共识载荷可以被简单抽象为字符串文本时，系统便会使用该类来进行封装与传输。
    // 实现共识值标准契约：实现了 Value 接口，使其能够作为合法的共识值载荷参与到 Paxos 的第二阶段（Accept）以及决议通知（Result）中。
    public static class StringValue extends StringFormatable implements Value {

        StringValue() {
            super(null);
        }

        public StringValue(String s) {
            super(s);
        }

        @Override
        public String toString() {
            return "Value:" + s;
        }

        public String getString() {
            return s;
        }
    }
}
