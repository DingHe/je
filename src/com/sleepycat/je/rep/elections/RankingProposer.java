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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator.Ranking;
import com.sleepycat.je.rep.elections.Protocol.Promise;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Extends the base proposer to choose a phase 2 value based on a suggestion's
 * relative ranking.
 */
public class RankingProposer extends Proposer {

    /**
     * If non-zero, use the specified log version as the one that supports
     * writing replication data in the previous format, to control whether the
     * skipPromiseDueToVersion method should always return false, for testing.
     */
    static volatile int testLogVersionReplicatePrevious = 0;

    public RankingProposer(Elections elections,
                           NameIdPair nameIdPair) {
        super(elections, nameIdPair);
    }

    /**
     * Chooses a Value based on the relative ranking of all Promise responses.
     * The one with the highest ranking is chosen. Zero priority responses are
     * never chosen. In the case of a tie, priority is used to resolve it. If
     * priority is insufficient the socket address is used to order the choice
     * so that a consistent result is obtained across the set irrespective of
     * the iteration order over the set.
     */
    // 根据所有 Promise 响应中携带的相对排名（Ranking）、节点选举优先级（Priority）以及 Socket 地址的字典序，从众多候选建议值中挑选出最优、最具权威性的 Value（通常代表最新的 Master 候选者），同时完美处理仲裁节点（Arbiter）与普通节点之间的投票权衡。
    // Set<MessageExchange> exchanges：第一阶段（Phase 1）收集到的所有远程消息交互记录集合。其中包含了各个 Acceptor 返回的 Promise 响应内容。
    @Override
    protected Value choosePhase2Value(Set<MessageExchange> exchanges) {
        // 记录当前遇到的最高数据排名（初始化为最小长整型）
        Ranking maxRanking =
            new Ranking(Long.MIN_VALUE, Long.MIN_VALUE);
        // 记录拥有最高排名的节点所对应的选举优先级。
        int maxPriority = Integer.MIN_VALUE;
        // 记录目标节点的 Socket 地址字符串（用于同分时的仲裁排序）
        String maxTarget = null;
        // 统计投票中选举优先级为 0（即无权参与主节点竞争）的节点数量。
        int zeroPrioNodes = 0;
        // 专门用于记录来自仲裁节点（Arbiter）的建议排名
        Ranking arbRanking = null;
        // 统计普通非仲裁节点的响应数量。
        int nonArbCount = 0;

        /* Check log versions in this group. */
        // 实例化一个版本计算器，用于检查集群中各节点的日志版本，防止由于版本差异导致共识错乱。
        VersionCalculator calculator =
            new VersionCalculator(elections, exchanges);

        Value acceptorValue = null;
        for (MessageExchange me : exchanges) {
            // 遍历 exchanges 集合。如果某个响应的操作码不是 PROMISE（说明未成功给出承诺），则直接跳过。
            if (me.getResponseMessage().getOp() !=
                elections.getProtocol().PROMISE) {
                continue;
            }
            // 过滤优先级为 0 的节点
            // 如果该节点的选举优先级（Priority）为 0（意味着该节点不具备被选为 Master 的资格），则将其计入 zeroPrioNodes，并跳过该响应。
            final Promise p = (Promise) me.getResponseMessage();
            if (p.getPriority() == 0) {
                zeroPrioNodes++;
                continue;
            }
            // 根据版本计算器的校验结果，如果发现该节点的日志版本不符合当前共识要求，则安全跳过。
            if (calculator.skipPromiseDueToVersion(p.getLogVersion())) {
                continue;
            }

            /* NULL name/ID pair means value came from arbiter */
            // 判断当前的建议值（suggestion）是否来自仲裁节点（Arbiter）。
            // 仲裁节点的特征是其 MasterValue 的 NameId 为空（NameIdPair.NULL）
            final Value suggestion = p.getSuggestion();
            final boolean isArb = (suggestion instanceof MasterValue) &&
                ((MasterValue) suggestion).getNameId().equals(NameIdPair.NULL);

            /*
             * Ignore arbiter if there are replies from multiple non-arbiters.
             * Arbiters should only matter for RF=2, and should be ignored if
             * both non-arbiter nodes replied, since they have all of the
             * existing data and should be allowed to proceed even if they have
             * lost data the arbiter remembers. [#25311]
             */
            // 如果是普通节点，将 nonArbCount（非仲裁节点计数器）自增 1
            if (!isArb) {
                nonArbCount++;
            } else {
            // 如果是仲裁节点（isArb 为真），则将其建议排名记录到 arbRanking 中，并直接 continue 跳过后续的普通打擂流程（仲裁节点的建议值不直接作为 Master 候选，而是作为后续安全校验的参考）。
                 arbRanking = p.getSuggestionRanking();
                 continue;
            }
            // 三重筛选打擂逻辑 (Ranking -> Priority -> Socket Address)
            // 排名（Ranking）比较
            final int compareTo =
                p.getSuggestionRanking().compareTo(maxRanking);
            if (compareTo < 0) {
               continue;
            }

            /* Use priority as a tie breaker. */
            // 优先级（Priority）作为平局决胜局
            if (compareTo == 0) {
              if (p.getPriority() < maxPriority) {
                  continue;
              }

              /*
               * Use socket address to choose in case of a tie, so we
               * always have a consistent ordering.
               */
              // Socket 地址字典序作为终极决胜局 (确保集群全局强一致性)
              if ((p.getPriority() == maxPriority) &&
                  ((maxTarget != null) &&
                   (me.target.toString().compareTo(maxTarget) <= 0))) {
                  continue;
              }
            }
            // 刷新当前最优候选值
            acceptorValue = p.getSuggestion();
            maxRanking = p.getSuggestionRanking();
            maxPriority = p.getPriority();
            maxTarget = me.target.toString();
        }
        // 异常情况处理与 Arbiter 安全回退校验
        // 无正优先级节点响应的警告：如果最终没选出任何共识值（acceptorValue == null），但确实收到了优先级为 0 的节点响应，记录日志并将 phase1NoNonZeroPrio 计数器自增。
        if ((acceptorValue == null) && (zeroPrioNodes > 0)) {
            LoggerUtils.logMsg(logger, elections.getRepImpl(),
                               formatter, Level.INFO,
                               "No positive election priority node responded."+
                               " Zero election priority node count:" +
                               zeroPrioNodes);
            phase1NoNonZeroPrio.increment();
        // 如果存在有效选出值、同时收到了 Arbiter 的排名（arbRanking != null）、且非仲裁节点数量很少（nonArbCount <= 1，例如在 RF=2 的集群中，另一个正常数据节点失联，只剩一个节点加一个 Arbiter）。
        } else if (acceptorValue != null && arbRanking != null  && nonArbCount <= 1) {
            /* Check if we have an arbiter response.  */
            // 如果此时 Arbiter 记住的数据排名（arbRanking）高于当前唯一存活节点的排名（maxRanking.compareTo(arbRanking) < 0），说明当前存活节点可能落后了。
            // 为了安全起见，系统会丢弃该节点的建议值（将 acceptorValue = null），并触发 phase1Arbiter 计数，防止因孤儿节点强行上位导致数据丢失。
            if (maxRanking.compareTo(arbRanking) < 0) {
                phase1Arbiter.increment();
                acceptorValue = null;
            }
        }
        return acceptorValue;
    }

    /**
     * Returns a proposal number. Note that the proposal numbers must increase
     * over time, even across restarts of the proposer process.
     * @return a 24 character string representing the proposal number
     */
    @Override
    public synchronized Proposal nextProposal() {
        return proposalGenerator.nextProposal();
    }

    private final TimebasedProposalGenerator proposalGenerator =
        new TimebasedProposalGenerator();

    /* Adds versioning information as a factor for elections. */
    // 在集群滚动升级或存在不同日志版本（Log Version）节点的混合环境中，将日志版本兼容性作为选主的限制因子。
    // 在早期版本的 JE 中，由于新版本 Master 无法向下兼容向旧版本 Replica 提供复制流，系统必须确保在多数派节点升级之前，Master 只能由运行旧版本的节点担任。
    // VersionCalculator 正是负责计算和判定各个节点是否满足版本资格的工具类。
    // 分析集群内版本分布：遍历第一阶段收到的所有 Promise 响应，统计集群中存在哪些不同的日志版本，并找出其中的“最低版本（Lowest Version）”与“多数派版本（Majority Version）”。
    // 过滤不合规节点的参选资格：提供 skipPromiseDueToVersion 判断方法，决定是否因为版本不兼容而直接跳过或忽略某个节点的候选承诺，防止因版本错配导致复制协议崩溃。
    private static class VersionCalculator {
        // 第一阶段收集到的所有网络消息交互记录集合。包含了各个 Acceptor 返回的 Promise 及其携带的日志版本信息。
        private final Set<MessageExchange> exchanges;
        // 持有的选举协调中心引用。用于后续获取协议对象、复制节点实例以及群组拓扑配置。
        private final Elections elections;
        /* The majority log version in this group. */
        // 多数派日志版本。记录在整个可投票群组中，占据了半数以上节点所使用的日志版本号。初始化为整型最小值。
        private int majorityVersion = Integer.MIN_VALUE;
        /* The lowest log version in this group. */
        // 群组中的最低日志版本。在遍历所有响应时不断取较小值更新，代表当前集群中运行的“最老”软件版本。初始化为整型最大值。
        private int lowestVersion = Integer.MAX_VALUE;
        /* True if there is only one log version in this group. */
        // 单一版本标记位。如果群组中所有参与选举的节点运行的都是完全相同的日志版本，则该布尔值为 true，从而可以跳过复杂的版本限制逻辑
        private boolean singleVersion = false;

        public VersionCalculator(Elections elections,
                                 Set<MessageExchange> exchanges) {
            this.exchanges = exchanges;
            this.elections = elections;
            calculate();
        }
        // 遍历所有响应，统计日志版本分布并计算关键版本指标
        private void calculate() {

            /*
             * Calculate the lowest log version and the total nodes that take
             * part in the election, save all the log version information to
             * calculate the majority log version.
             */
            // 统计每个日志版本分别被多少个节点所采用
            Map<Integer, Integer> logFormats = new HashMap<Integer, Integer>();
            for (MessageExchange me : exchanges) {
                // 跳过所有非 PROMISE 的响应。
                // 将响应向下转型为 Promise 协议对象
                if (me.getResponseMessage().getOp() !=
                    elections.getProtocol().PROMISE) {
                    continue;
                }

                Promise p = (Promise) me.getResponseMessage();
                // 动态刷选出群组内的最低版本 lowestVersion
                if (p.getLogVersion() < lowestVersion) {
                    lowestVersion = p.getLogVersion();
                }
                // 统计版本频次
                // 将每个 logVersion 及其出现的次数存入 logFormats 映射中。
                if (!logFormats.containsKey(p.getLogVersion())) {
                    logFormats.put(p.getLogVersion(), 1);
                } else {
                    logFormats.put(p.getLogVersion(),
                                   logFormats.get(p.getLogVersion()) + 1);
                }
            }

            /*
             * If there is only log version in the whole group, return and do
             * nothing.
             */
            // 单一版本判断：若 logFormats.size() == 1（说明大家版本完全一致），则将 singleVersion 设为 true 并直接结束。
            if (logFormats.size() == 1) {
                singleVersion = true;
                return;
            }

            /*
             * If the RepNode is null, just return, so that the nodes with the
             * smallest log version can always be elected as master.
             */
            // 安全检查：若当前节点的 elections.getRepNode() 为空，则直接返回（允许版本最小的节点被选为 Master）。
            if (elections.getRepNode() == null) {
                return;
            }

            /* Calculate the majority log version. */
            // 计算多数派版本：获取可投票节点总数（electableNodeCount），遍历 logFormats。
            // 如果某个版本对应的节点数量超过了半数（entry.getValue() > (electableNodeCount / 2)），则将其确立为 majorityVersion 并跳出循环。
            int electableNodeCount =
                elections.getRepNode().getGroup().getElectableGroupSize();
            for (Map.Entry<Integer, Integer> entry : logFormats.entrySet()) {
                if (entry.getValue() > (electableNodeCount / 2)) {
                    majorityVersion = entry.getKey();
                    break;
                }
            }
        }

        /**
         * For JE 5 and earlier versions, if there are multiple log versions in
         * a replication group, only elect the nodes with the lowest log
         * version or the nodes with the majority log version to be the master.
         * This behavior is required because, in those JE versions, the master
         * can only supply replication data in the current log format, so the
         * master must be chosen from nodes running the earlier version during
         * an upgrade until a majority of the nodes have been upgraded.  This
         * restriction no longer applies as of log version 9 in JE 6 -- see
         * [#22336].
         *
         * <p>Returns true if election will ignore the promise because there
         * are multiple log versions in the group, all log versions correspond
         * to JE 5 and earlier versions, and the log version of a replica
         * satisfies one of the following rules:
         * <ol>
         * <li> If there is no majority log version in the group, and log
         *      version of this replica is not the lowest log version.
         * <li> If there exists a majority log version, and log version of this
         *      replica is not the lowest log version, nor the majority log
         *      version.
         * </ol>
         */
        // 判断是否应当因为版本兼容性限制而忽略（跳过）某个节点的 Promise 承诺。
        boolean skipPromiseDueToVersion(int logVersion) {
            // 单一版本直接通过：如果 singleVersion 为真（全员版本一致），直接返回 false（不跳过，正常参与竞选）。
            if (singleVersion) {
                return false;
            }
            // 获取版本升级阈值：从测试或系统中获取 logVersionReplicatePrevious（代表旧版本向新版本过渡的分界线常量）。
            int logVersionReplicatePrevious = testLogVersionReplicatePrevious;
            // 新版本免检：如果群组的 lowestVersion 已经高于或等于该过渡版本（说明集群已经基本完成升级），则返回 false，解除版本限制。
            if (logVersionReplicatePrevious == 0) {
                logVersionReplicatePrevious =
                    LogEntryType.LOG_VERSION_REPLICATE_OLDER;
            }
            if (lowestVersion >= logVersionReplicatePrevious - 1) {
                return false;
            }
            // 无多数派版本时：如果群组中没有形成多数派版本，且当前参与评估的节点日志版本不是最低版本，则返回 true（跳过它，只允许最老版本的节点当选 Master）。
            // 存在多数派版本时：如果群组中已经有了多数派版本，但当前节点的日志版本既不是最低版本、也不是多数派版本，则返回 true（跳过它，防止使用中间过渡版本的孤立节点当选）。
            if ((majorityVersion == Integer.MIN_VALUE &&
                 logVersion != lowestVersion) ||
                (majorityVersion != Integer.MIN_VALUE &&
                 logVersion != lowestVersion &&
                 logVersion != majorityVersion)) {
                return true;
            }

            return false;
        }
    }
}
