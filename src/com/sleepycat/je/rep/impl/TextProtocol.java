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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.DurationConfigParam;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.elections.Utils;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;

/**
 * TextProtocol provides the support for implementing simple low performance
 * protocols involving replication nodes. The protocol is primarily text based,
 * and checks group membership and version matches with every message favoring
 * flexibility over performance.
 *
 * The base class is primarily responsible for the message formatting and
 * message envelope validation. The subclasses define the specific messages
 * that constitute the protocol and the request/response semantics.
 *
 * Every message has the format:
 * <version>|<name>|<id>|<op>|<op-specific payload>
 *
 * <version> is the version of the protocol in use.
 * <name>    identifies a group participating in an election. It avoids
 *           accidental cross-talk across groups holding concurrent elections.
 * <id>      identifies the originator of the message within the group.
 * <op>      the operation identified by the specific message.
 * <op-specific payload> the payload associated with the particular operation.
 */
// TextProtocol 是一个用于实现基于文本的轻量级低性能复制协议基类。
// 主要用于复制节点之间的辅助通信、选举（Elections）以及群组管理交互。相比于二进制协议，它牺牲了一定的性能，但换取了极高的灵活性、可读性和版本兼容性。
// 其核心报文格式（Wire Format）定义为：
// $$\text{<version>\vert{}<name>\vert{}<id>\vert{}<op>\vert{}<op-specific payload>}$$
// <version>：协议版本号。
// <name>：参与选举或群组的集群名（用于防止不同集群并发选举时发生交叉干扰）。
// <id>：消息发送方在群组内的节点 ID。
// <op>：具体的行为操作符。
// <op-specific payload>：与特定操作关联的有效载荷（参数）。
public abstract class TextProtocol {

    /* The name of the class associated with this protocol. */
    // 当前协议子类运行时的全限定类名。用于日志记录和故障排查。
    private final String name;

    /*
     * Protocol version string. Format: <major version>.<minor version>
     * It's used to ensure compatibility across versions.
     */
    // 当前协议的版本字符串（格式如 "major.minor"）。用于确保通信双方跨版本的兼容性校验。
    private final String VERSION;

    /* The name of the group executing this protocol. */
    // 执行此协议的复制组名称，用于防止跨群组误通信。
    private final String groupName;

    /*
     * The set of ids of nodes that are permitted to communicate via this
     * protocol, or null if not restricted. It's updated as nodes enter and
     * leave the dynamic group.
     */
    // 允许通过此协议进行通信的合法成员节点 ID 集合（若为 null 则不限制）。随着动态群组的变动而更新。
    private Set<Integer> memberIds;

    /* The id associated with this protocol participant. */
    // 当前协议参与者自身的身份标识对（包含节点名称与数字 ID）
    private final NameIdPair nameIdPair;

    /*
     * The suffix of message prefix constituting the "fixed" part of the
     * message for this group and node, it does not include the version
     * information, which goes in front of this prefix.
     */
    // 消息前缀的固定后缀部分（不含版本信息），由群组名和跳过检查的特殊节点 ID 组合而成。
    protected final String messageNocheckSuffix;

    /*
     * Timeouts used for network communications. Use setTimeouts() to override
     * the defaults.
     */
    // 网络连接打开的超时时间（默认 10 秒）。
    private int openTimeoutMs = 10000; // Default to 10 sec
    // 网络数据读取的超时时间（默认 10 秒）
    private int readTimeoutMs = 10000; // Default to 10 sec

    /* The token separator in messages */
    // 文本协议中各个字段/Token 的分隔符（|）及其正则转义形式。
    public static final String SEPARATOR = "|";
    public static final String SEPARATOR_REGEXP="\\" + SEPARATOR;

    /* A message defined by the base class to deal with all errors. */
    // 内置标准消息操作符（PROTOCOL_ERROR, OK_RESP, FAIL_RESP）
    public final MessageOp PROTOCOL_ERROR =
                new MessageOp("PE", ProtocolError.class);
    public final MessageOp OK_RESP = new MessageOp("OK", OK.class);
    public final MessageOp FAIL_RESP = new MessageOp("FAIL", Fail.class);

    /* The number of message types defined by the subclass. */
    // 由具体子类定义并注册的非默认消息类型总数
    private int nonDefaultMessageCount;

    /* Maps request Ops to the corresponding enumerator. */
    // 将操作符字符串（Op ID）映射到对应的 MessageOp 枚举/对象的哈希表，用于反序列化时快速查找。
    private final Map<String,MessageOp> ops = new HashMap<>();

    protected final Logger logger;
    // 复制消息格式化器。
    protected final Formatter formatter;
    // 底层复制环境实现引用（可为 null）。
    protected final RepImpl repImpl;
    // 网络数据通道工厂，用于创建安全的套接字通道。
    protected final DataChannelFactory channelFactory;

    /**
     * Hook used to modify messages in the serialized form. The hook is invoked
     * on a serialized Request immediately before it's written to the network
     * and immediately after a response is received and before it's
     * deserialized. The hook implementation must be re-entrant.
     */
    // 专供测试使用的序列化/反序列化钩子（Test Hook），允许在网络传输前后拦截并修改序列化字符串。
    private static TestHook<String> serDeHook;

    /**
     * Creates an instance of the Protocol.
     *
     * @parameter version the protocol version number
     * @parameter groupName the name of the group executing this protocol
     * @param nameIdPair a unique identifier for this node
     * @param repImpl for logging, may be null
     * @param channelFactory the factory for channel creation
     */
    public TextProtocol(String version,
                        String  groupName,
                        NameIdPair nameIdPair,
                        RepImpl repImpl,
                        DataChannelFactory channelFactory) {
        this.VERSION = version;
        this.groupName = groupName;
        this.nameIdPair = nameIdPair;
        this.repImpl = repImpl;
        this.channelFactory = channelFactory;
        name = getClass().getName();

        messageNocheckSuffix =
            groupName + SEPARATOR + NameIdPair.NOCHECK_NODE_ID;

        if (repImpl != null) {
            this.logger = LoggerUtils.getLogger(getClass());
        } else {
            this.logger = LoggerUtils.getLoggerFormatterNeeded(getClass());
        }
        this.formatter = new ReplicationFormatter(nameIdPair);
    }

    /**
     * Sets the hook that is invoked post serialization on request messages and
     * pre deserialization on response messages.
     *
     *  The hook implementation must be re-entrant.
     */
    public static void setSerDeHook(TestHook<String> serDeHook) {
        TextProtocol.serDeHook = serDeHook;
    }

    /**
     * Set the network timeouts associated with uses of this protocol instance.
     */
    protected void setTimeouts(RepImpl repImpl,
                               DurationConfigParam openTimeoutConfig,
                               DurationConfigParam readTimeoutConfig) {
        if (repImpl == null) {
            return;
        }
        final DbConfigManager configManager = repImpl.getConfigManager();
        openTimeoutMs = configManager.getDuration(openTimeoutConfig);
        readTimeoutMs = configManager.getDuration(readTimeoutConfig);
    }

    /**
     * The messages as defined by the subclass. Note that PROTOCOL_ERROR is a
     * pre-defined message that is defined by this class. The initialization is
     * not considered until this method after been invoked typically in the
     * constructor itself. This two-step is unfortunately necessary since the
     * creation of MessageOps instances requires that this class be completely
     * initialized, otherwise the MessageOp list could have been passed in as a
     * constructor argument.
     *
     * @param protocolOps the message ops defined by the subclass.
     */
    protected void initializeMessageOps(MessageOp[] protocolOps) {
        for (MessageOp op : protocolOps) {
            ops.put(op.opId, op);
        }
        nonDefaultMessageCount = protocolOps.length;
        ops.put(PROTOCOL_ERROR.opId, PROTOCOL_ERROR);
        ops.put(OK_RESP.opId, OK_RESP);
        ops.put(FAIL_RESP.opId, FAIL_RESP);
    }

    /**
     * For testing only.
     */
    protected MessageOp replaceOp(String op, MessageOp message) {
        return ops.put(op, message);
    }

    /**
     * Returns the messages, of the specified type, used by the protocol
     */
    public Set<String> getOps(Class<? extends Message> messageType) {
        final Set<String> reqOps = new HashSet<>();
        for (Entry<String, MessageOp> e : ops.entrySet()) {
            if (messageType.
                isAssignableFrom(e.getValue().getMessageClass())) {
                reqOps.add(e.getKey());
            }
        }

        return reqOps;
    }

    public int getOpenTimeout() {
        return openTimeoutMs;
    }

    public int getReadTimeout() {
        return readTimeoutMs;
    }

    public NameIdPair getNameIdPair() {
        return nameIdPair;
    }

    /* The total number of nonDefault messages defined by the protocol. */
    public int messageCount() {
        return nonDefaultMessageCount;
    }

    /**
     * Updates the current set of nodes that are permitted to communicate via
     * this protocol, or null for unrestricted.
     *
     * @param newMemberIds
     */
    public void updateNodeIds(Set<Integer> newMemberIds) {
        memberIds = newMemberIds;
    }

    /**
     * Returns the Integer number which represents a Protocol version.
     */
    public int getMajorVersionNumber(String version) {
        return Double.valueOf(version).intValue();
    }

    /**
     * The Operations that are part of the protocol.
     */
    // 封装单个操作符及其对应的 Java 消息类
    public static class MessageOp {

        /* The string denoting the operation for the request message. */
        // 操作符字符串
        private final String opId;

        /* The class used to represent the message. */
        // 代表该消息的类对象
        private final Class<? extends Message> messageClass;

        public MessageOp(String opId, Class<? extends Message> messageClass) {
            this.opId = opId;
            this.messageClass = messageClass;
        }

        String getOpId() {
            return opId;
        }

        Class<? extends Message> getMessageClass() {
            return messageClass;
        }

        @Override
        public String toString() {
            return opId;
        }
    }

    /**
     * Represents the tokens on a message line. The order of the enumerators
     * represents the order of the tokens in the wire format.
     */
    public enum TOKENS {
        VERSION_TOKEN,
        NAME_TOKEN,
        ID_TOKEN,
        OP_TOKEN,
        FIRST_PAYLOAD_TOKEN;
    }

    /* Used to indicate that an entity is formatable and can be serialized and
     * de-serialized.
     */
    // 标记接口，规定实现类必须能够通过 wireFormat() 方法生成适合网络传输的字符串。
    protected interface WireFormatable {

        /*
         * Returns the string representation suitable for use in a network
         * request.
         */
        abstract String wireFormat();
    }

    /**
     * Parses a line into a Request/Response message.
     *
     * @param line containing the message
     * @return a message instance
     * @throws InvalidMessageException
     */
    public Message parse(String line)
        throws InvalidMessageException {

        String[] tokens = line.split(SEPARATOR_REGEXP);

        final int index = TOKENS.OP_TOKEN.ordinal();
        if (index >= tokens.length) {
            throw new InvalidMessageException(
                MessageError.BAD_FORMAT,
                "Missing message op in message: " + line);
        }
        final MessageOp op = ops.get(tokens[index]);
        if (op == null) {
            throw new InvalidMessageException(MessageError.BAD_FORMAT,
                                              "Text Protocol" +
                                              " unknown op:" + tokens[index] +
                                              " in message: " + line);
        }

        try {
            Class<? extends Message> c = op.getMessageClass();
            Constructor<? extends Message> cons =
                c.getConstructor(c.getEnclosingClass(),
                                 line.getClass(),
                                 tokens.getClass());
            Message message = cons.newInstance(this, line, tokens);
            return message;
        } catch (InstantiationException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (IllegalAccessException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (SecurityException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (NoSuchMethodException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (InvocationTargetException e) {
            /* Unwrap the exception. */
            Throwable target = e.getTargetException();
            if (target instanceof RuntimeException) {
                final String message = "message: " + line +
                  " exception:" + target.getClass().getName() +
                  " exception message:" + target.getMessage();
                throw new InvalidMessageException(MessageError.BAD_FORMAT,
                                                  message);

            } else if (target instanceof InvalidMessageException) {
                throw (InvalidMessageException) target;
            }
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Base message class for all messages exchanged in the protocol.
     */
    // 所有文本协议消息（包括请求和响应）的抽象基类。它实现了 WireFormatable 接口。
    // 定义统一的消息结构：规范了文本协议中消息头部的固定字段（版本、群组名、发送方 ID、操作符）与后续有效载荷（Payload）。
    // 提供双向序列化上下文：支持从本地对象初始化消息（用于发送），也支持从网络接收的原始文本行及 Token 数组中解析并反序列化消息
    // 提供安全校验与版本协商：在反序列化时自动校验消息版本、群组名称以及发送者的群组群员资格（Member ID 检查），并在混杂版本升级时处理版本兼容。
    public abstract class Message implements WireFormatable {
        /* The sender of the message. */
        // 记录当前消息发送方的数字节点 ID。
        private int senderId = 0;

        /*
         * The version of this message, as it's deserialized and sent across
         * the network. The default is that messages are sent in the VERSION of
         * the current protocol, but in cases of mixed-version upgrades, the
         * message may be sent in an earlier version format.
         *
         * When this message is a RequestMessage, the sender will always
         * initially send it out its own native version, but may resend it in
         * an earlier version, if the recipient can't understand the native
         * version. When the message is a ResponseMessage, the sender can reply
         * either in its native version, or in an earlier version if the
         * requester is an older version of JE.
         */
        // 消息在网络中实际发送或接收时采用的协议版本。支持混合版本升级（Mixed-version upgrades）：允许节点以降级版本格式发送响应或重发请求，以兼容较老版本的 JE 节点。
        protected String sendVersion;

        /* The line representing the message. */
        // 代表整条消息的原始字符串文本。如果是本地新建而非反序列化的消息，则为 null。
        private final String line;

        /* The tokenized form of the above line. */
        // 将原始消息文本按分隔符 | 分割后的 Token 数组，用于快速按位置索引读取各个固定或可变字段。
        private final String[] tokens;

        /* The current variable arg token */
        // 指向当前可变参数负载（Payload）的解析指针。初始位置为第一个 payload token 的索引位置，随着 nextPayloadToken() 的调用向后移动。
        private int currToken = TOKENS.FIRST_PAYLOAD_TOKEN.ordinal();
        // 带有跳过检查后缀的无条件消息前缀缓存（包含版本号和无检查后缀），用于快速生成通用响应。
        protected String messagePrefixNocheck;

        /**
         * The constructor used for the original non-serialized instance of the
         * message, which does not use the line or tokens.
         */
        // 用于在本地首次创建未序列化的全新消息实例
        // 将 line 和 tokens 设为 null，并通过 setSendVersion(VERSION) 将发送版本初始化为当前协议的默认版本。
        protected Message() {
            line = null;
            tokens = null;
            setSendVersion(VERSION);
        }

        /**
         * Every message must define a constructor of this form so that it can
         * be de-serialized. The constructor is invoked using reflection by the
         * parse() method.
         *
         * @param line the line constituting the message
         * @param tokens the line in token form
         * @throws InvalidMessageException
         * @throws EnvironmentFailureException on format errors
         */
        // 用于反序列化消息（由 parse() 方法通过反射调用）。
        protected Message(String line, String[] tokens)
            throws InvalidMessageException {

            this.line = line;
            this.tokens = tokens;

            /* Validate the leading fixed fields. */
            // 版本校验：检查消息带有的版本是否超出了当前代码所支持的范围（VERSION），若过高则抛出 VERSION_MISMATCH 异常。
            final String version = getTokenString(TOKENS.VERSION_TOKEN);
            if (new Double(VERSION) < new Double(version)) {
                throw new InvalidMessageException
                    (MessageError.VERSION_MISMATCH,
                     "Version argument mismatch." +
                     " Expected: " + VERSION + ", found: " + version +
                     ", in message: " + line);
            }

            /*
             * Set the sender version of a request message. This version
             * information will be used by the receiver to determine what
             * version should be used for the response message.
             */
            setSendVersion(version);
            // 群组名校验：比对消息中的群组名与本地群组名是否一致，不一致则抛出 GROUP_MISMATCH 异常。
            final String messageGroupName = getTokenString(TOKENS.NAME_TOKEN);
            if (!groupName.equals(messageGroupName)) {
                throw new InvalidMessageException
                (MessageError.GROUP_MISMATCH,
                 "Group name mismatch; this group name: " + groupName +
                 ", message group name: " + messageGroupName +
                 ", in message: " + line);
            }
            // 成员资格校验：解析发送方 ID。如果配置了成员白名单（memberIds），且发送方不在白名单内（且不是特权免检 ID），则抛出 NOT_A_MEMBER 异常，拒绝来自非法节点的恶意消息。
            senderId =
                new Integer(getTokenString(TOKENS.ID_TOKEN)).intValue();
            if ((memberIds != null) &&
                (memberIds.size() > 0) &&
                (nameIdPair.getId() != NameIdPair.NOCHECK_NODE_ID) &&
                (senderId != NameIdPair.NOCHECK_NODE_ID) &&
                (senderId != nameIdPair.getId()) &&
                !memberIds.contains(senderId)) {
                throw new InvalidMessageException
                   (MessageError.NOT_A_MEMBER,
                    "Sender's member id: " + senderId +
                    ", message op: " + getTokenString(TOKENS.OP_TOKEN) +
                    ", was not a member of the group: " + memberIds +
                    ", in message: " + line);
            }
        }
        // 返回当前消息发送者的数字节点 ID。
        public int getSenderId() {
            return senderId;
        }

        /*
         * Set the version of the message that we have just received. This
         * version information will be used by the receiver to determine what
         * version should be used for the response message.
         */
        // 设置并更新消息的发送版本。
        // 会先校验传入的版本不能高于当前协议版本 VERSION；如果版本发生了改变，会同步刷新内部的 messagePrefixNocheck 缓存。
        public void setSendVersion(String version) {
            if (new Double(VERSION) < new Double(version)) {
                throw new IllegalStateException
                    ("Send version: " + version + " shouldn't be larger " +
                     "than the current version: " + VERSION);
            }

            if (!version.equals(sendVersion)) {
                sendVersion = version;
                messagePrefixNocheck =
                    sendVersion + SEPARATOR + messageNocheckSuffix;
            }
        }

        /* Get the send version of a message. */
        // 返回当前消息的发送版本字符串。
        public String getSendVersion() {
            return sendVersion;
        }
        // 拼装并返回标准的消息头部前缀（格式：sendVersion | groupName | nodeId）。
        protected String getMessagePrefix() {
            return sendVersion + SEPARATOR + groupName + SEPARATOR +
                   nameIdPair.getId();
        }
        // 要求所有子类必须实现，返回当前消息对应的操作符对象（MessageOp）。
        public abstract MessageOp getOp();

        /**
         * Returns the protocol associated with this message
         */
        // 返回与该消息关联的外层 TextProtocol 实例
        public TextProtocol getProtocol() {
            return TextProtocol.this;
        }

        /**
         * Returns the token value associated with the token type.
         *
         * @param tokenType identifies the token in the message
         * @return the associated token value
         */
        // 根据固定的 TOKENS 枚举索引，从 tokens 数组中安全获取对应的字段值。如果索引越界，抛出环境异常。
        private String getTokenString(TOKENS tokenType) {
            final int index = tokenType.ordinal();
            if (index >= tokens.length) {
                throw EnvironmentFailureException.unexpectedState
                    ("Bad format; missing token: " + tokenType +
                     "at position: " + index + "in message: " + line);
            }
            return tokens[index];
        }

        /**
         * Returns the next token in the payload.
         *
         * @return the next payload token
         * @throws InvalidMessageException
         */
        // 顺序读取下一个 Payload 字段。
        protected String nextPayloadToken()
            throws InvalidMessageException {

            if (currToken >= tokens.length) {
                throw new InvalidMessageException
                (MessageError.BAD_FORMAT,
                 "Bad format; missing token at position: " + currToken +
                 ", in message: " + line);
            }
            return tokens[currToken++];
        }

        protected boolean hasMoreTokens() {
            return currToken < tokens.length;
        }

        /**
         * Returns the current token position in the payload.
         *
         * @return the current token position
         */
        protected int getCurrentTokenPosition() {
            return currToken;
        }
    }

    /**
     * Base classes for response messages.
     */
    // TextProtocol.ResponseMessage 是所有协议响应消息（Response Messages）的抽象基类，它继承自前面介绍的 Message 类
    // 规范响应消息的行为：作为诸如 OK、Fail、ProtocolError 等所有具体响应消息的共同父类。
    // 支持协议版本继承：允许在回复客户端请求时，直接继承或使用来自请求消息（RequestMessage）的发送版本（sendVersion），从而完美支持混合版本升级时的向下兼容响应。
    // 提供统一的序列化前缀与对象比较逻辑：规范了响应消息在线上网络传输时的格式前缀，并基于操作符（Op）实现了响应对象的等值比较与哈希计算。
    public abstract class ResponseMessage extends Message {

        protected ResponseMessage() {
            super();
        }

        /**
         * Create an instance with the send version specified by the request.
         *
         * @param request the request
         */
        // 基于请求消息创建响应消息的构造器（非常关键的兼容性设计）
        protected ResponseMessage(final RequestMessage request) {
            setSendVersion(request.getSendVersion());
        }
        // 用于反序列化接收到的响应消息。
        protected ResponseMessage(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
        }

        /**
         * Returns the version id and Op concatenation that starts every
         * message.
         */
        // 生成响应消息在网络传输时开头的固定协议前缀。
        // 将 getMessagePrefix()（包含版本、群组名、发送方 ID）与当前操作符的 ID（getOp().opId）通过分隔符（SEPARATOR，即 |）拼接起来。例如格式如：<version>|<groupName>|<nodeId>|<opId>。
        protected String wireFormatPrefix() {
            return getMessagePrefix() + SEPARATOR + getOp().opId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof ResponseMessage)) {
                return false;
            }
            return getOp().equals(((ResponseMessage)obj).getOp());
        }

        @Override
        public int hashCode() {
            return getOp().getOpId().hashCode();
        }
    }
    // 主要作用是封装和表示文本协议中的协议错误响应
    // 当某一方在接收或解析对方发送的文本协议消息时遇到格式错误、版本不匹配、群组不匹配或非群员等异常（即捕获到 InvalidMessageException）时，就会构造并发送该类型的响应消息给对方。
    // 标准化错误反馈：将服务器端或客户端在解析文本协议时发生的错误（包含具体的错误分类 MessageError 和文本描述）打包成标准的响应报文。
    // 支持双向序列化与反序列化：既能够从本地的异常对象快速生成错误响应，也能够在接收到远端传来的错误报文时将其解析为结构化的 Java 对象。
    public class ProtocolError extends ResponseMessage {
        // 存储引发该协议错误的详细文本描述信息（通常来自于底层异常的错误提示）。
        private final String message;
        // 存储错误的枚举分类（MessageError，例如版本不匹配、群组名不匹配、格式错误等）。
        private final MessageError errorType;

        public ProtocolError(InvalidMessageException messageException) {
            this(messageException.getErrorType(),
                 messageException.getMessage());
        }

        public ProtocolError(MessageError messageError, String message) {
            this.message = message;
            this.errorType = messageError;
        }

        public ProtocolError(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            errorType = MessageError.valueOf(nextPayloadToken());
            message = nextPayloadToken();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result
                    + ((message == null) ? 0 : message.hashCode());
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
            if (!(obj instanceof ProtocolError)) {
                return false;
            }
            final ProtocolError other = (ProtocolError) obj;
            if (message == null) {
                if (other.message != null) {
                    return false;
                }
            } else if (!message.equals(other.message)) {
                return false;
            }

            return true;
        }

        @Override
        public MessageOp getOp() {
            return PROTOCOL_ERROR;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR +
                errorType.toString() + SEPARATOR + message;
        }

        public MessageError getErrorType() {
            return errorType;
        }

        public String getMessage() {
            return message;
        }
    }

    public class OK extends ResponseMessage {

        /**
         * Create an instance with the send version specified by the request.
         *
         * @param request the request
         */
        public OK(final RequestMessage request) {
            super(request);
        }

        public OK(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
        }

        @Override
        public MessageOp getOp() {
           return OK_RESP;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
           return wireFormatPrefix();
        }
    }

    public class Fail extends ResponseMessage {
        private final String message;

        public Fail(String message) {
            this.message = sanitize(message);
        }

        /**
         * Create an instance with the send version specified by the request.
         *
         * @param request the request
         * @param message the failure message
         */
        public Fail(final RequestMessage request, final String message) {
            super(request);
            this.message = sanitize(message);
        }

        public Fail(String line, String[] tokens)
            throws InvalidMessageException {
            super(line, tokens);

            message = nextPayloadToken();
        }

        public String getMessage() {
            return message;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + ((message == null) ? 0 : message.hashCode());
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
            if (!(obj instanceof Fail)) {
                return false;
            }
            Fail other = (Fail) obj;
            if (!getOuterType().equals(other.getOuterType())) {
                return false;
            }
            if (message == null) {
                if (other.message != null) {
                    return false;
                }
            } else if (!message.equals(other.message)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
           return FAIL_RESP;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
           return wireFormatPrefix() + SEPARATOR + message;
        }

        private TextProtocol getOuterType() {
            return TextProtocol.this;
        }

        /**
         * Removes any newline characters.  Embedded newlines are not supported
         * by {@code TextProtocol}, but exception messages sometimes have them,
         * and the payload of a {@code Fail} response often comes from an
         * exception message.
         */
        private String sanitize(String msg) {
            return msg.replace("\n", "  ");
        }
    }

    /**
     * Base class for all Request messages
     */
    // TextProtocol.RequestMessage 是所有协议请求消息（Request Messages）的抽象基类，它继承自前文介绍的 Message 类
    // 规范请求消息的行为：作为文本协议中所有具体业务请求消息（例如选举投票请求、心跳请求、同步请求等）的共同父类。
    // 提供统一的请求前缀生成：规范了请求消息在线上网络传输时的头部格式前缀
    // 实现基于操作符的等值比较逻辑：重写了 equals 和 hashCode 方法，确保请求对象能够基于其携带的操作符（Op）进行准确的比对与哈希计算。
    public abstract class RequestMessage extends Message {

        protected RequestMessage() {}
        // 用于反序列化接收到的请求消息（由 parse() 方法通过反射调用）。
        protected RequestMessage(String line, String[] tokens)
            throws InvalidMessageException {
            super(line, tokens);
        }

        /**
         * Returns the version id and Op concatenation that form the prefix
         * for every message.
         */
        // 生成请求消息在网络传输时开头的固定协议前缀。
        protected String wireFormatPrefix() {
            return getMessagePrefix() + SEPARATOR + getOp().opId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof RequestMessage)) {
                return false;
            }
            return getOp().equals(((RequestMessage) obj).getOp());
        }

        @Override
        public int hashCode() {
            return getOp().getOpId().hashCode();
        }
    }

    /**
     * Converts a response line into a ResponseMessage.
     *
     * @param responseLine
     * @return the response message
     * @throws InvalidMessageException
     */
    ResponseMessage parseResponse(String responseLine)
        throws InvalidMessageException {

        return (ResponseMessage) parse(responseLine);
    }

    /**
     * Converts a request line into a requestMessage.
     *
     * @param requestLine
     * @return the request message
     * @throws InvalidMessageException
     */
    public RequestMessage parseRequest(String requestLine)
        throws InvalidMessageException {

        return (RequestMessage) parse(requestLine);
    }

    /**
     * Reads the channel and returns a read request. If the message format
     * was bad, it sends a ProtocolError response back over the channel and
     * no further action is needed by the caller.
     *
     * @param channel the channel delivering the request
     * @return null if EOF was reached or the message format was bad
     * @throws IOException
     */
    public RequestMessage getRequestMessage(DataChannel channel)
        throws IOException {

        BufferedReader in = new BufferedReader
            (new InputStreamReader(Channels.newInputStream(channel)));

        String requestLine = in.readLine();
        if (requestLine == null) {
            /* EOF */
            return null;
        }
        try {
            return parseRequest(requestLine);
        } catch (InvalidMessageException e) {
            processIME(channel, e);
            return null;
        }
    }

    /**
     * Process an IME encountered during request processing by writing a
     * ProtocolError message as a response and logging it.
     *
     * @param channel the channel used to write the ProtocolError message
     * @param ime the exception
     */
    public void processIME(DataChannel channel,
                           InvalidMessageException ime) {
        LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                           name + " format error:" +
                           LoggerUtils.exceptionTypeAndMsg(ime));
        PrintWriter out =
            new PrintWriter(Channels.newOutputStream(channel), true);
        out.println(new ProtocolError(ime).wireFormat());
        out.close();
    }

    public ResponseMessage process(Object requestProcessor,
                                   RequestMessage requestMessage) {

        Class<? extends Object> cl = requestProcessor.getClass();
        try {
            Method method =
                cl.getMethod("process", requestMessage.getClass());
            return (ResponseMessage) method.invoke
                (requestProcessor, requestMessage);
        } catch (NoSuchMethodException e) {
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.SEVERE,
                               name +
                               " Method: process(" +
                               requestMessage.getClass().getName() +
                               ") was missing");
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (Exception e) {
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.SEVERE,
                               name +
                               " Unexpected exception: " +
                                LoggerUtils.exceptionTypeAndMsg(e));
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * A single request/response interaction, targetted at a given service
     * running at a particular remote socket address.  Since it implements
     * {@code Runnable} it can be used with thread pools, {@code Future}s,
     * etc.  But its {@code run()} method can also simply be called directly.
     */
    // 封装单次完整的网络交互：将“向远端特定 Socket 地址的某个服务发送请求消息（RequestMessage）并同步接收响应消息（ResponseMessage）”
    // 这一完整的网络交互过程抽象为一个独立的对象。
    // 支持异步与多线程调度：由于实现了 Runnable 接口，它能够无缝配合 Java 的线程池（ExecutorService）、Future 等并发工具进行批量并发广播（如 Proposer 广播 Prepare 请求或 informLearners 广播结果），同时也允许直接调用其 run() 或 messageExchange() 方法进行同步执行。
    // 内置自动版本协商与重试机制：具备智能处理协议版本不匹配（Version Mismatch）的能力。当因版本问题收到错误响应时，能够自动调整发送版本并重新发起请求。
    public class MessageExchange implements Runnable {
        // 目标远端节点的 Socket 地址（IP + 端口）。指明了本次消息交互要发送给集群中的哪台机器。
        public final InetSocketAddress target;
        // 要发送的请求消息实例（如 Prepare、Accept、Propose 等请求）
        private final RequestMessage requestMessage;
        // 远端目标服务的名称（例如 Acceptor 服务、Learner 服务等）。在建立连接后的握手阶段会用到。
        private final String serviceName;
        // 从远端接收并解析后得到的响应消息实例。如果交互成功，该字段会被赋予解析后的回应；如果发生网络或协议异常，则保持为 null。
        private ResponseMessage responseMessage;
        public Exception exception;

        public MessageExchange(InetSocketAddress target,
                               String serviceName,
                               RequestMessage request) {
            this.target = target;
            this.serviceName = serviceName;
            this.requestMessage = request;
        }

        /*
         * Get the response message for a request message.
         *
         * If the response message is a ProtocolError message which caused by
         * protocol version mismatch, it resets the request message's
         * sendVersion as the ResponseMessage ProtocolError's version and send
         * again.
         */
        @Override
        public void run() {
            // 执行一次标准的网络交互尝试
            messageExchange();
            // 版本不匹配自动重试
            // 检查返回的 responseMessage 是否不为空且操作码为 PROTOCOL_ERROR。
            if (responseMessage != null &&
                responseMessage.getOp() == PROTOCOL_ERROR) {
                ProtocolError error = (ProtocolError) responseMessage;
                if (error.getErrorType() == MessageError.VERSION_MISMATCH) {
                    requestMessage.setSendVersion(error.getSendVersion());
                    messageExchange();
                    LoggerUtils.logMsg
                        (logger, repImpl, formatter, Level.INFO,
                          name +
                         " Resend message: " + requestMessage.toString() +
                         " in version: " + requestMessage.getSendVersion() +
                         " while protocol version is: " + VERSION +
                         " because of the version mismatch, the returned" +
                         " response message is: " + responseMessage);
                }
            }
        }

        /**
         * Run a message exchange. A successful exchange results in a response
         * message being set. All failures result in the response message being
         * null and an exception being set.
         */
        // 执行单次底层网络交互的核心工作流方法（包含了连接、握手、序列化发送、接收反序列化及全面的异常捕获）
        public void messageExchange() {

            DataChannel dataChannel = null;
            BufferedReader in = null;
            PrintWriter out = null;
            try {
                // 连接 target，配置了禁用 Nagle 算法（setTcpNoDelay(true)）、开启长连接/重用（setReuseAddr）、并设定了开启超时和读超时时间。
                dataChannel =
                    channelFactory.connect(
                        target,
                        new ConnectOptions().
                        setTcpNoDelay(true).
                        setOpenTimeout(openTimeoutMs).
                        setReadTimeout(readTimeoutMs).
                        setBlocking(true).
                        setReuseAddr(true));
                // 在通道上完成与远端特定服务的握手协议
                ServiceDispatcher.doServiceHandshake(dataChannel,
                                                     serviceName);

                OutputStream ostream =
                    Channels.newOutputStream(dataChannel);
                out = new PrintWriter(ostream, true);
                // 发送请求：通过输出流（PrintWriter）将 requestMessage.wireFormat() 序列化后的文本串发送出去（期间若配置了安全钩子 serDeHook 会进行相应拦截处理），并执行 flush() 刷新缓冲区。
                String wireFormat = requestMessage.wireFormat();
                if (serDeHook != null) {
                    serDeHook.doHook(wireFormat);
                    wireFormat = serDeHook.getHookValue();
                }
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                                   name +
                                   " request: " + wireFormat+ " to " + target);
                out.println(wireFormat);
                out.flush();
                // 接收响应：通过输入流（BufferedReader）读取远端返回的一行应答文本（in.readLine()）。
                in = new BufferedReader
                    (new InputStreamReader(
                        Channels.newInputStream(dataChannel)));
                String line = in.readLine();
                if (serDeHook != null) {
                    serDeHook.doHook(line);
                    line = serDeHook.getHookValue();
                }

                LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                                   name +
                                   " response: " + line + " from " + target);
                // 若读取到的行为空（line == null），说明遭遇了过早的 EOF（连接被对端意外关闭），创建并设置一个 BAD_FORMAT 类型的 ProtocolError 响应。
                if (line == null) {
                    setResponseMessage
                        (new ProtocolError(MessageError.BAD_FORMAT,
                                           "Premature EOF for request: " +
                                           wireFormat));
                } else {
                // 将文本解析为对应的 ResponseMessage 对象并通过 setResponseMessage(...) 保存。
                    setResponseMessage(parseResponse(line));
                }
            } catch (java.net.SocketTimeoutException e){
                this.exception = e;
            } catch (SocketException e) {
                this.exception = e;
            } catch (IOException e) {
                this.exception = e;
            } catch (TextProtocol.InvalidMessageException ime) {
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                                   name + " response format error:" +
                                   LoggerUtils.exceptionTypeAndMsg(ime) +
                                   " from:" + target);
                this.exception = ime;
            } catch (ServiceConnectFailedException e) {
                this.exception = e;
            } catch (Exception e) {
                this.exception = e;
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.SEVERE,
                                   name + " Unexpected exception:" +
                                   LoggerUtils.exceptionTypeAndMsg(e));
                throw EnvironmentFailureException.unexpectedException
                    ("Service: " + serviceName +
                     " failed; attempting request: " + requestMessage.getOp(),
                     e);
            } finally {
                Utils.cleanup(logger, repImpl, formatter, dataChannel, in, out);
            }
        }

        public void setResponseMessage(ResponseMessage responseMessage) {
            this.responseMessage = responseMessage;
        }

        /**
         * Returns the response message. The null may be returned as part of
         * the protocol exchange, or it may be null if an exception was
         * encountered say because of some IO problem. It's the caller's
         * responsibility to check for an exception in that circumstance.
         * <p>
         * Note: there may be some protocols (e.g., Monitor) that define null
         * to be a proper, expected response upon success.  It might be
         * preferable to redefine them to return an explicit OK response, if
         * possible.
         *
         * @return the response
         */
        public ResponseMessage getResponseMessage() {
            return responseMessage;
        }

        public RequestMessage getRequestMessage() {
            return requestMessage;
        }

        public Exception getException() {
            return exception;
        }
    }
    // 为基于字符串的各种共识值、参数或消息载荷提供一个轻量级的、标准的线上传输格式（Wire Format）封装。
    // 当某个数据对象本质上只需要以纯文本或字符串形式在网络协议中进行序列化和反序列化时，可以通过继承或组合该类来快速复用统一的规范。
    // 实现线上格式化契约：实现了 WireFormatable 接口，提供标准的方法将对象转换为网络传输格式（Wire Format）。
    //
    protected static class StringFormatable implements WireFormatable {
        // 受保护的字符串变量。
        // 用于直接存储当前对象所代表的文本内容或线上传输格式字符串
        protected String s;

        StringFormatable() {}

        protected StringFormatable(String s) {
            this.s = s;
        }

        public void init(String wireFormat) {
            s = wireFormat;
        }

        @Override
        public String wireFormat() {
            return s;
        }

        @Override
        public int hashCode() {
            return ((s == null) ? 0 : s.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof StringFormatable)) {
                return false;
            }

            final StringFormatable other = (StringFormatable) obj;
            if (s == null) {
                if (other.s != null) {
                    return false;
                }
            } else if (!s.equals(other.s)) {
                return false;
            }
            return true;
        }
    }

    /*
     * The type associated with an invalid Message. It's used by the exception
     * below and by ProtocolError.
     */
    static public enum MessageError
        {BAD_FORMAT, VERSION_MISMATCH, GROUP_MISMATCH, NOT_A_MEMBER}

    /**
     * Used to indicate a message format or invalid content exception.
     */
    @SuppressWarnings("serial")
    public static class InvalidMessageException extends Exception {
        private final MessageError errorType;

        public InvalidMessageException(MessageError errorType,
                                       String message) {
            super(message);

            this.errorType = errorType;
        }

        public MessageError getErrorType() {
            return errorType;
        }
    }
}
