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

package com.sleepycat.je.rep.stream;

import com.sleepycat.je.rep.elections.Acceptor;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.utilint.VLSN;

/**
 * A Basic suggestion generator.
 *
 * A more sophisticated version may contact other replica nodes to see if it
 * has sufficient connectivity to implement the commit policy in effect for
 * the Replication Group. KIS for now.
 */
// 作为基础的建议生成器，负责在选主过程中为当前节点生成推荐的 Master 价值（建议自己当选）以及用于衡量数据新鲜度的双维权重排名（Ranking）。
// 推举自身为 Master 候选者：在响应 Proposer 的 Prepare（Propose）请求时，向集群提议“建议由当前节点担任主节点（Master）”。
// 计算并评估数据新鲜度排名（Ranking）：结合节点的 VLSN（虚拟日志序列号）、DTVLSN（持久化事务 VLSN）以及当前是否为权威主节点（Authoritative Master），计算出一个精准的双维排名。该排名直接决定了在 Paxos 阶段二中哪个节点的数据更新、更有资格胜出。
// 支持强制选主（Testing/Forced Election）：提供抢占式排名与测试钩子，允许在特定运维或测试场景下强制将当前节点推举为 Master。
public class MasterSuggestionGenerator
    implements Acceptor.SuggestionGenerator {
    // 复制节点（Replication Node）的实例引用。
    // 持有当前节点的核心句柄。通过它能够获取节点的网络地址（Host/Port）、名称标识（NameIdPair）、VLSN 索引范围以及 DTVLSN 状态，是计算建议值和排名的核心数据源。
    private final RepNode repNode;

    /* Determines whether to use pre-emptive ranking to make this
     * node the Master during the next election */
    // 是否强制当前节点成为 Master 的开关标志位。
    // 默认为 false。当在测试或特殊管理命令中被设为 true 时，该节点会在选举中获得最高优先级的抢占式排名，确保其必定当选。
    private boolean forceAsMaster = false;

    /* Used during a forced election to guarantee this proposal as a winner. */
    // 抢占式最高排名常量。
    // 利用 Long.MAX_VALUE 作为主分量（major），这是一个理论上的最大值。
    // 当 forceAsMaster 开启时返回此排名，确保在投票对比中无往而不利，必定击败所有竞争对手。
    private static final Ranking PREMPTIVE_RANKING =
        new Ranking(Long.MAX_VALUE, 0);
    /* The ranking used to ensure that a current master is reselected. */
    // 当前 Master 连任优待排名常量。
    // 次于抢占式排名（Long.MAX_VALUE - 1）。专门用于确保当前正在运行、具备权威身份的主节点（isAuthoritativeMaster()）在重新选举时能够获得极高的权重优先被选中，维持集群稳定性。
    private static final Ranking MASTER_RANKING =
        new Ranking(Long.MAX_VALUE - 1, 0);

    public MasterSuggestionGenerator(RepNode repNode) {
        this.repNode = repNode;
    }

    // 直接实例化并返回一个 MasterValue 对象，将当前节点的主机名（HostName）、端口号（Port）以及名称标识对（NameIdPair）封装进去。
    // 这向 Proposer 明确表态：“本节点建议推举自己（当前的 RepNode）成为集群的新 Master”。
    @Override
    public Value get(Proposal proposal) {
        /* Suggest myself as master */
        return new MasterValue(repNode.getHostName(),
                               repNode.getPort(),
                               repNode.getNameIdPair());
    }
    // 计算并返回当前节点数据的“新鲜度”和重要性排名（Ranking）
    @Override
    public Ranking getRanking(Proposal proposal) {
        // 强制选主检查：若 forceAsMaster 为真，直接返回最高抢占式排名 PREMPTIVE_RANKING。
        if (forceAsMaster) {
            return PREMPTIVE_RANKING;
        }
        // 冻结 VLSN 序号
        // 在选主投票期间，利用当前的 proposal 冻结 VLSN 的推进，防止在选举未完成前本地日志继续写入导致数据视图发生偏移）
        repNode.getVLSNFreezeLatch().freeze(proposal);
        // 权威主节点优待检查：若当前节点是权威主节点（repNode.isAuthoritativeMaster() 为真），
        // 直接返回 MASTER_RANKING（Long.MAX_VALUE - 1），赋予其极高的连任权重。
        if (repNode.isAuthoritativeMaster()) {
            return MASTER_RANKING;
        }
        // 获取分布式事务虚拟日志序列号
        final long dtvlsn = repNode.getDTVLSN();
        // 获取本地 VLSN 索引范围的最末尾序列号作为常规 VLSN
        final long vlsn = repNode.getVLSNIndex().getRange().
                           getLast().getSequence();
        // 说明处于旧版本流段向新版本过渡的兼容期，尚无 DTVLSN 信息），则将 vlsn 作为主分量，次分量设为 0
        if (dtvlsn == VLSN.UNINITIALIZED_VLSN_SEQUENCE) {
            /*
             * In a preDTVLSN stream segment on a postDTVLSN replica. No
             * DTVLSN information as yet.
             */
            return new Ranking(vlsn, 0);
        }

        return new Ranking(dtvlsn, vlsn);
    }

    /**
     * This entry point is for testing only.
     *
     * It will submit a Proposal with a premptive ranking so that it's
     * guaranteed to be the selected as the master at the next election.
     *
     * @param force determines whether the forced proposal is in effect
     */
    public void forceMaster(boolean force) {
        this.forceAsMaster = force;
    }
}
