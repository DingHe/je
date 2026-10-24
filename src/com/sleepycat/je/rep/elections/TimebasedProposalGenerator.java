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

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Proposer.ProposalParser;

/**
 * Generates a unique sequence of ascending proposal numbers that is unique
 * across all machines.
 *
 * Each proposal number is built as the catenation of the following components:
 *
 * ms time (8 bytes) | machineId (16 bytes) | locally unique Id (4 bytes)
 *
 * The ms time supplies the increasing number and the IP address is a number
 * unique across machines.
 *
 * The machineId is generated as described below.
 *
 * The locally unique Id is used to allow for multiple unique proposal
 * generators in a single process.
 */
public class TimebasedProposalGenerator {

    /*
     * A number that is unique for all instances of the TimeBasedGenerator on
     * this machine.
     */
    private final int locallyUniqueId;
    private static final AtomicInteger uniqueIdGenerator = new AtomicInteger(1);

    /*
     * Tracks the time (in ms) used to generate the previous proposal
     * preventing the creation of duplicate proposals.  Synchronize on this
     * instance when accessing this field.
     */
    private long prevProposalTime = System.currentTimeMillis();

    /*
     * A unique ID for this JVM, using a hex representation of the IP address
     * XOR'ed with a random value. If the IP address cannot be determined,
     * a secure random number is generated and used instead. The risk of
     * collision is very low since the number of machines in a replication
     * group is typically small, in the 10s at most.
     */
    private static final String machineId;

    /* Width of each field in the Proposal number in hex characters. */
    final static int TIME_WIDTH = 16;

    /* Allow for 16 byte ipv6 addresses. */
    final static int ADDRESS_WIDTH =32;
    final static int UID_WIDTH = 8;

    /*
     * Initialize machineId, do it just once to minimize latencies in the face
     * of misbehaving networks that slow down calls to getLocalHost()
     */
    static  {

        InetAddress localHost;
        try {
            localHost = java.net.InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            /*
             * Likely a misconfigured machine if it could not determine
             * localhost.
             */
            localHost = null;
        }
        byte[] localAddress = null;
        if (localHost != null) {
            localAddress = localHost.getAddress();

            if (localHost.isLoopbackAddress()) {
                /* Linux platforms return a loopback address, examine the
                 * interfaces individually for a suitable address.
                 */
                localAddress = null;
                try {
                    for (Enumeration<NetworkInterface> interfaces =
                        NetworkInterface.getNetworkInterfaces();
                        interfaces.hasMoreElements();) {
                        for (Enumeration<InetAddress> addresses =
                            interfaces.nextElement().getInetAddresses();
                            addresses.hasMoreElements();) {
                            InetAddress ia = addresses.nextElement();
                            if (! (ia.isLoopbackAddress() ||
                                   ia.isAnyLocalAddress() ||
                                   ia.isMulticastAddress())) {
                                /* Found one, any one of these will do. */
                                localAddress = ia.getAddress();
                                break;
                            }
                        }
                    }
                } catch (SocketException e) {
                    /* Could not get the network interfaces, give up */
                }
            }
        }

        if (localAddress != null) {
            /*
             * Convert the address to a positive integer, XOR it with a
             * random value of the right size, and format in hex
             */
            final BigInteger addrVal = new BigInteger(1, localAddress);
            final BigInteger randVal =
                new BigInteger(ADDRESS_WIDTH * 4, new SecureRandom());
            machineId = String.format("%0" + ADDRESS_WIDTH + "x",
                                      addrVal.xor(randVal));
        } else {
            /*
             * If the localAddress is null, this host is likely disconnected,
             * or localHost is misconfigured, fall back to using just a secure
             * random number.
             */
            final BigInteger randVal =
                new BigInteger(ADDRESS_WIDTH * 4, new SecureRandom());
            machineId = String.format("%0" + ADDRESS_WIDTH + "x", randVal);
        }
    }

    /**
     * Creates an instance with an application-specified locally (machine wide)
     * unique id, e.g. a port number, or a combination of a pid and some other
     * number.
     *
     * @param locallyUniqueId the machine wide unique id
     */
    TimebasedProposalGenerator(int locallyUniqueId) {
        this.locallyUniqueId = locallyUniqueId;
    }

    /**
     * Constructor defaulting the unique id so it's merely unique within the
     * process.
     */
    public TimebasedProposalGenerator() {
        this(uniqueIdGenerator.getAndIncrement());
    }

    /**
     * Returns the next Proposal greater than all previous proposals returned
     * on this machine.
     *
     * @return the next unique proposal
     */
    // 核心作用是：在当前机器上生成一个全局唯一且严格单调递增的新提案编号（Proposal），供 Proposer 在执行 Paxos 算法时使用。
    public Proposal nextProposal() {
        // 获取当前系统时间戳
        long proposalTime = System.currentTimeMillis();
        // 同步块：确保单调递增与线程安全
        synchronized (this) {
            // 保证提议时间的严格单调递增（Monotonicity）
            if (proposalTime <= prevProposalTime) {
                /* Proposals are moving faster than the clock. */
                proposalTime = ++prevProposalTime;
            }
            prevProposalTime = proposalTime;
        }
        // 将时间戳、机器标识和本地唯一 ID 按照固定格式拼接成一个十六进制字符串，并封装为 StringProposal 对象返回。
        // %016x (proposalTime)：将 64 位的毫秒时间戳转换为 16位十六进制数（不足前面补 0）。这确保了时间越靠后，生成的字符串字典序就越大，从而天然满足 Paxos 协议中对提议编号“谁大听谁的”的大小比较要求。
        // %s (machineId)：紧接着拼入当前机器的唯一标识符（字符串形式），用于区分不同机器并发生成的提议，防止分布式环境下的命名冲突。
        // %08x (locallyUniqueId)：将本地的自增唯一标识格式化为 8位十六进制数（不足前面补 0），用于区分在同一毫秒、同一机器上产生的多个不同提议。
        return new StringProposal(String.format("%016x%s%08x", proposalTime,
                                                machineId, locallyUniqueId));
    }

    /**
     * Returns the parser used to convert wire representations into Proposal
     * instances.
     *
     * @return a ProposalParser
     */
    public static ProposalParser getParser() {
        return StringProposal.getParser();
    }

    /**
     * Implements the Proposal interface for a string based proposal. The
     * string is a hex representation of the Proposal.
     */
    // 通过字符串（通常是十六进制编码格式）来封装和表达 Paxos 提案（Proposal）
    // 在分布式共识过程中，每一个提议都需要具备唯一性、可比较性以及网络传输能力，StringProposal 正是基于字符串形式对这一契约的具体落地实现。
    // 实现提案标准接口：实现了 Proposal 接口，提供统一的序列化传输格式（wireFormat）以及自然排序比较能力（Comparable）。
    // 基于字符串的轻量封装：将底层的提议内容直接映射为文本或十六进制字符串，便于在文本协议（TextProtocol）中进行传输、解析和持久化。
    //
    private static class StringProposal implements Proposal {
        // 用于存储当前提议的具体字符串内容（通常是代表时间戳、节点标识及其他参数的十六进制组合）
        private final String proposal;

        /* The canonical proposal parser. */
        private static ProposalParser theParser = new ProposalParser() {
                @Override
                public Proposal parse(String wireFormat) {
                    return ((wireFormat == null) || ("".equals(wireFormat))) ?
                        null :
                        new StringProposal(wireFormat);
                }
            };

        StringProposal(String proposal) {
            assert (proposal != null);
            this.proposal = proposal;
        }

        @Override
        public String wireFormat() {
            return proposal;
        }

        @Override
        public int compareTo(Proposal otherProposal) {
            return this.proposal.compareTo
                (((StringProposal) otherProposal).proposal);
        }

        @Override
        public String toString() {
            return "Proposal(" +
                proposal.substring(0, TIME_WIDTH) +
                ":" +
                proposal.substring(TIME_WIDTH, TIME_WIDTH + ADDRESS_WIDTH) +
                ":" + proposal.substring(TIME_WIDTH + ADDRESS_WIDTH) +
                ")";
        }

        private static ProposalParser getParser() {
            return theParser;
        }

        @Override
        public int hashCode() {
            return ((proposal == null) ? 0 : proposal.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof StringProposal)) {
                return false;
            }
            final StringProposal other = (StringProposal) obj;
            if (proposal == null) {
                if (other.proposal != null) {
                    return false;
                }
            } else if (!proposal.equals(other.proposal)) {
                return false;
            }
            return true;
        }
    }
}
