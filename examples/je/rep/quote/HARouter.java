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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import je.rep.quote.Command.InvalidCommandException;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.monitor.GroupChangeEvent;
import com.sleepycat.je.rep.monitor.JoinGroupEvent;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent;
import com.sleepycat.je.rep.monitor.Monitor;
import com.sleepycat.je.rep.monitor.MonitorChangeListener;
import com.sleepycat.je.rep.monitor.MonitorConfig;
import com.sleepycat.je.rep.monitor.NewMasterEvent;

/**
 * This example illustrates use of an HA aware Router used to forward high
 * level requests to replication nodes implemented by
 * {@link RouterDrivenStockQuotes}. The router is built using the APIs provided
 * by the {@link com.sleepycat.je.rep.monitor.Monitor Monitor}; it's a
 * standalone application and does not itself access a JE Environment. The
 * router forwards logical requests, that represent some service provided by
 * the application. It only has knowledge of whether a request will potentially
 * require an write to the database, but does not have any other application
 * level logic, nor does it access a JE environment. The HARouter accepts a
 * request from the console and dispatches it to the application running on the
 * master, if it's a write request, or to one of the replicas if it's a read
 * request. The HARouter keeps track of the current Master via the events that
 * are delivered to the Monitor.
 * <p>
 * It's the HARouter instead of each individual node (as in the
 * {@link UpdateForwardingStockQuotes} example) that tracks the current Master
 * via the {@link com.sleepycat.je.rep.monitor.Monitor Monitor}. Since the
 * router ensures that writes are directed to the master node, the logic in
 * the node itself is simpler: the node simply services the requests forwarded
 * to it by the router on a port dedicated for this purpose.
 * <p>
 * The protocol used to communicate between the router and the nodes has been
 * deliberately kept very simple. In particular, it makes limited provisions
 * for error reporting back to the router.
 * <p>
 * The router requires the following arguments:
 *
 * <pre>
 * java je.rep.quote.HARouter -nodeName &lt;nodeName&gt; \
 *                            -nodeHost &lt;host:port&gt; \
 *                            -helperHost &lt;host:port&gt;&quot;
 *  The arguments are described below:
 *   -nodeName identifies the monitor name associated with this Router
 *   -nodeHost the hostname:port combination used by the Monitor to listen for
 *             election results and group level changes.
 *   -helperHost one or more nodes that may be used by the Monitor to locate the
 *               Master and register the Monitor with the Master.
 * </pre>
 *
 * Note that the arguments are similar to the ones used to start a replication
 * node. A key difference is that the -env option is absent, since the router
 * is standalone and is not associated with one.
 * <p>
 * The router can be started as follows:
 *
 * <pre>
 * java je.rep.quote.HARouter -nodeName n1 \
 *                            -nodeHost node.acme.com:6000 \
 *                            -helperHost node.acme.com:5001
 * </pre>
 *
 * The replication nodes involved in the routing can be started as described in
 * {@link RouterDrivenStockQuotes}. The Router and the nodes can be started in
 * any convenient order.
 *
 * @see RouterDrivenStockQuotes
 */
// HARouter 是 Berkeley DB Java Edition (JE) 高可用（HA）复制模块中的高可用感知路由器（HA-aware Router）示例程序。
// 核心作用体现在：
// 独立的独立路由进程：它是一个轻量级的独立客户端应用，不直接访问任何 JE 数据库环境。
// 利用 Berkeley DB JE 提供的 Monitor（监控器）API 来实时跟踪高可用复制集群的拓扑结构变化（如主备选举、节点加入/离开）。
// 智能请求分发（读写分离）：接收控制台输入的高层业务请求，如果是写操作（更新股票），则精准路由转发给当前集群的 Master 节点；如果是读操作（打印股票），则在所有活跃的副本节点间进行轮询（Round-Robin）负载均衡分发。
// 简化节点逻辑：由于路由转发和状态追踪由 HARouter 集中处理，后端的数据节点（如 RouterDrivenStockQuotes）代码变得非常纯粹，只需专注于在专用端口上处理发过来的请求即可。
public class HARouter {

    /*
     * The displacement from the node's port to the app port on which it
     * listens for messages from the router.
     */
    // 端口偏移量常量（设为 100）。
    // 用于将 BDB JE 底层复制通信端口（Rep Port）映射为应用程序专用的路由监听端口（App Port = Rep Port + 100）。
    private static final int APP_PORT_DISPLACEMENT = 100;

    /*
     * Lock used to coordinate writes to the router's notion of the current
     * master.
     */
    // 专用于同步和保护“当前 Master 状态信息”的互斥锁对象。
    private final Object masterLock = new Object();

    /* Holds the current masterId, protected by masterLock. */
    // 当前集群 Master 节点的名称（volatile 修饰以保证多线程可见性），由 masterLock 保护。
    private volatile String masterName = null;

    /*
     * The address on which the app node that is currently a master is
     * listening for requests, protected by masterLock..
     */
    // 当前 Master 节点上应用程序监听客户端请求的 Socket 地址，由 masterLock 保护。
    private InetSocketAddress appMasterAddress;

    /* 
     * Lock used to coordinate access to nodeAddressMapping, activeAppAddresses 
     * and lastReadAddress.
     */
    // 专用于同步和保护复制群组拓扑映射（如节点地址映射、活跃节点列表及负载均衡游标）的互斥锁对象。
    private final Object groupLock = new Object();

    /*
     * Map keyed by node name to yield the address at which the node is 
     * listening for routed requests. All access to these mappings must be 
     * synchronized via groupLock.
     */
    // 存储所有集群节点名称到其应用程序路由地址（App Socket Address）的映射表。受 groupLock 保护。
    private static final Map<String, InetSocketAddress> nodeAddressMapping =
        new HashMap<String, InetSocketAddress>();

    /*
     * List of addresses of active nodes. All access to this list must be 
     * synchronized via groupLock.
     */
    // 当前处于活跃状态的节点应用地址列表，用于实现读请求的轮询分发。受 groupLock 保护。
    private static final List<InetSocketAddress> activeAppAddresses = 
        new LinkedList<InetSocketAddress>();

    /*
     * Tracks the position in the activeAppAddresses list to which a read 
     * request was last dispatched.
     */
    // 记录上一次读请求分发到的活跃地址列表索引位置，用于实现 Round-Robin（轮询）负载均衡算法。
    private int lastReadAddress = 0;

    /* The configuration for the replication group */
    // 用于配置 Monitor 监控器节点网络及群组信息的复制配置对象。
    private static final ReplicationConfig repConfig = new ReplicationConfig();

    /**
     * Round robins through the list of App dispatch addresses returning each
     * address in sequence.
     *
     * @return the next dispatch address
     */
    // 以轮询（Round-Robin）方式从活跃应用地址列表中获取下一个分发地址，用于读请求的负载均衡。
    private InetSocketAddress getNextDispatchAddress() {
        synchronized (groupLock) {
            lastReadAddress++;
            if (lastReadAddress == activeAppAddresses.size()) {
                lastReadAddress = 0;
            }
            return activeAppAddresses.get(lastReadAddress);
        }
    }

    /**
     * Routes a quit request to all the nodes in the replication group, thus
     * shutting them down.
     */
    // 将退出（quit）请求广播路由给复制群组中的所有节点，促使它们安全关闭。
    private void routeQuit() {
        synchronized (groupLock) {
            for (InetSocketAddress address : nodeAddressMapping.values()) {
                try {
                    QuoteUtil.forwardRequest(address, "quit", System.err);
                } catch (IOException e) {
                    // Ignore exceptions during shutdown
                }
            }
        }
    }

    /**
     * Routes a read request in round-robin fashion to active nodes in the
     * replication group. It gives up if none of the nodes is available.
     *
     * @param line the text of the read request
     */
    // 以轮询方式将读请求分发给集群中活跃的节点，如果所有节点均不可用则放弃。
    private void routeReadCommand(String line) {
        IOException lastException = null;
        int retries = 0;
        synchronized (groupLock) {
            retries = activeAppAddresses.size();
        }
        while (retries-- > 0) {
            try {
                QuoteUtil.forwardRequest(getNextDispatchAddress(),
                                         line,
                                         System.err);
                return;
            } catch (IOException e) {
                lastException = e;

                /*
                 * A more realistic implementation would remove the node
                 * from the list and arrange for it to be returned when it
                 * was up again. But this code is only intended to be a demo.
                 */
                continue;
            }
        }
        System.err.println("None of the nodes were available to " +
                           "service the request " +
                           " Sample exception: " + lastException);
    }

    /**
     * Routes an update command to the master node if one is available.
     *
     * @param line the text of the update request.
     */
    // 将更新/写请求精准路由转发给当前可用的 Master 节点
    private void routeUpdateCommand(String line) {
        final InetSocketAddress targetAddress;
        final String targetNodeName;
        synchronized (masterLock) {
            /* Copy a consistent pair. */
            targetAddress = appMasterAddress;
            targetNodeName = masterName;
        }
        try {
            QuoteUtil.forwardRequest(targetAddress, line, System.err);
        } catch (IOException e) {
            /* Group could be in the midst of a transition to a new master. */
            System.err.println("Could not connect to master: " +
                                targetNodeName  + " Exception: " + e);
        }
    }

    /**
     * The central method used to sort out and dispatch individual requests.
     *
     * @throws Exception
     */
    // 控制台交互主循环。接收用户的输入，根据命令类型分发给对应的路由方法。
    void doWork() throws Exception {
        BufferedReader stdin =
            new BufferedReader(new InputStreamReader(System.in));

        while (true) {

            /**
             * Generate prompt, read input. Valid inputs are: 1) quit
             * (tells this local node to quit) 2) print 3) stockSymbol
             * stockValue
             */
            String line = QuoteUtil.promptAndRead(
                    "HARouter", null, false, System.out, stdin);
            if (line == null) {
                return;
            }
            try {
                switch (Command.getCommand(line)) {
                case NONE:
                    break;
                case PRINT:
                    routeReadCommand(line);
                    break;
                case QUIT:
                    /* Broadcast it to all the nodes. */
                    routeQuit();
                    return;
                case UPDATE:
                    routeUpdateCommand(line);
                    break;
                }
            } catch (InvalidCommandException e) {
                System.err.println(e.getMessage());
                continue;
            }
        }
    }

    /**
     * Creates a Router instance and initializes it using the command line
     * arguments passed into it.
     *
     * @param argv command line arguments.
     */
    HARouter(String[] argv) {
        parseParams(argv);
    }

    /**
     * Parse the command line parameters and initialize the router with
     * configuration information about the replication group and the
     * <code>Monitor</code> that is running as part of this Router.
     */
    // 解析启动路由器时通过命令行传入的各项参数，配置底层的复制与监控属性，并对必填参数进行严格校验。
    void parseParams(String[] argv)
        throws IllegalArgumentException {

        int argc = 0;
        int nArgs = argv.length;
        // 首先检查传入的命令行参数数组长度是否为 0，若为空则直接调用 usage(...) 报错并提示必填参数
        if (nArgs == 0) {
            usage("-nodeName, -nodeHost, and -helperHost are required.");
        }
        String nodeName = null;
        while (argc < nArgs) {
            String thisArg = argv[argc++];
            // -nodeName（路由器监控节点名称）
            // 检查后面是否还有后续参数，若有则取出并赋值给 nodeName，同时通过 repConfig.setNodeName(nodeName) 设置到复制配置中。若缺失参数则报错。
            if (thisArg.equals("-nodeName"))  {
                /* the node id */
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    repConfig.setNodeName(nodeName);
                } else {
                    usage("-nodeName requires an argument");
                }
            // -nodeHost（监控器主机与端口对）：
            // 取出下一个参数，通过 repConfig.setNodeHostPort(...) 设置 Monitor 用于监听选举和群组变动的网络地址。
            }  else if (thisArg.equals("-nodeHost")) {
                /* The node hostname, port pair. */
                if (argc < nArgs) {
                    repConfig.setNodeHostPort(argv[argc++]);
                } else {
                    usage("-nodeHost requires an argument");
                }
            // -helperHost（辅助节点主机与端口对）：
            // 取出下一个参数，通过 repConfig.setHelperHosts(...) 设置辅助节点，以便 Monitor 首次启动时能够联系集群并定位 Master。
            } else if (thisArg.equals("-helperHost")) {
                /* The helper node hostname, port pair. */
                if (argc < nArgs) {
                    repConfig.setHelperHosts(argv[argc++]);
                } else {
                    usage("-helperHost requires an argument");
                }
            } else {
                usage("Invalid parameter: " + thisArg);
            }
        }
        // 将复制节点的类型显式设置为监控器（Monitor）。
        // 这意味着该路由器虽然属于复制群组的一部分，但它不参与数据的存储和选举投票，其唯一职责是监控主备状态。
        repConfig.setNodeType(NodeType.MONITOR);
        // 将复制群组名称统一设置为 StockQuotes.REP_GROUP_NAME（"StockQuotesRepGroup"），确保它能够加入正确的股票报价高可用集群。
        repConfig.setGroupName(StockQuotes.REP_GROUP_NAME);

        if (nodeName == null) {
            usage("-nodeName is a required parameter");
        }
    }

     /**
      * Provides invocation usage help information in response to an error
      * condition.
      *
      * @param message an explanation of the condition that provoked the
      *                display of usage information.
      */
     void usage(String message){
         System.out.println();
         System.out.println(message);
         System.out.println();
         System.out.print("usage: " + getClass().getName());
         System.out.println(" -nodeName <nodeName> -nodeHost <host:port> " +
                            " -helperHost <host:port>");
         System.out.
         println("\t -nodeName the unique node name for this monitor\n" +
                 "\t -nodeHost the unique hostname and port pair\n" +
                 "\t -helperHost the hostname and port pair associated with " +
                 " the helper node\n" );
         System.exit(0);
     }

     // 实现了 MonitorChangeListener 接口，用于异步接收来自 BDB HA 监控器的集群状态变更事件。
     class RouterChangeListener implements MonitorChangeListener {
         // 当检测到新 Master 当选时，调用 updateMaster(...) 更新主节点信息。
         public void notify(NewMasterEvent newMasterEvent) {
             updateMaster(newMasterEvent.getNodeName(),
                          newMasterEvent.getSocketAddress());
         }
         // 当群组拓扑发生变动（节点加入/移除群组）时，调用 updateGroup(...) 更新成员映射。
         public void notify(GroupChangeEvent groupChangeEvent) {
             updateGroup(groupChangeEvent);
         }
         // 当节点上线加入群组时，调用 addActiveNode(...) 将其加入活跃列表。
         public void notify(JoinGroupEvent joinGroupEvent) {
             addActiveNode(joinGroupEvent);
         }
         // 当节点离线离开群组时，调用 removeActiveNode(...) 将其从活跃列表中移除。
         public void notify(LeaveGroupEvent leaveGroupEvent) {
             removeActiveNode(leaveGroupEvent);
         }
     }

     /**
      * Maps a socket address used by a replication node to the corresponding
      * socket address used by the application to listen for routed messages.
      * This method achieves the mapping by reusing a port some fixed distance
      * away from the replication port in use.
      *
      * @param nodeAddress the socket address for the replication port
      * @return the corresponding socket address used by the application.
      */
     // 将底层的 BDB 复制端口转换为对应的应用程序路由监听端口。
     static InetSocketAddress getAppSocketAddress(InetSocketAddress
                                                  nodeAddress) {
         return new InetSocketAddress(nodeAddress.getHostName(),
                                      nodeAddress.getPort()+
                                      APP_PORT_DISPLACEMENT);
     }

     /**
      * Initialize the routers knowledge regarding the set of nodes in the
      * replication group.
      *
      * @param electableNodes set of electable nodes
      */
     // 初始化群组中所有可选举节点的地址映射字典。
     private void initGroup(Set<ReplicationNode> electableNodes) {
         System.err.println("Group size: " + electableNodes.size());
         synchronized (groupLock) {
             nodeAddressMapping.clear();
             for (ReplicationNode node : electableNodes) {
                 nodeAddressMapping.put
                     (node.getName(), 
                      getAppSocketAddress(node.getSocketAddress()));
             }
         }
     }

     /**
      * Update the electable nodes list of the group when a GroupChangeEvent
      * happens. Also remove the node in the active nodes list if the node is
      * active when its GroupChangeType is REMOVE.
      *
      * @param event the GroupChangeEvent fired
      */
     // 处理群组变更事件（GroupChangeEvent），动态维护节点地址映射及活跃状态。
     private void updateGroup(GroupChangeEvent event) {
         synchronized (groupLock) {
             ReplicationGroup group = event.getRepGroup();
             String nodeName = event.getNodeName();

             switch (event.getChangeType()) {
                 case REMOVE:
                     System.err.println("Node: " + nodeName + 
                                        " is removed from the group.");
                     removeActiveNode(nodeName);
                     nodeAddressMapping.remove(nodeName);
                     break;
                 case ADD:
                     System.err.println("Node: " + nodeName + 
                                        " is added to the group.");
                     ReplicationNode node = group.getMember(nodeName);
                     nodeAddressMapping.put
                         (nodeName,
                          getAppSocketAddress(node.getSocketAddress()));
                     break;
                 default:
                     throw new IllegalStateException("Unknown event: " + 
                                                     event.getChangeType());
             }
         }
     }
        
     /**
      * Updates the active nodes list when a node joins the group, so that the 
      * routers know the current active nodes in the group.
      *
      * @param event the JoinGroupEvent notifying a node joins the group.
      */
     // 当节点加入并激活时，将其对应的应用地址添加到活跃分发列表中。
     private void addActiveNode(JoinGroupEvent event) {
         System.err.println("Node: " + event.getNodeName() + 
                            " with current master: " + event.getMasterName() +
                            " joins the group at: " + event.getJoinTime());
         synchronized (groupLock) {
             InetSocketAddress address = 
                 nodeAddressMapping.get(event.getNodeName());
             activeAppAddresses.add(address);
         }
     }

     /**
      * Updates the active nodes list when a node leaves the group, so that the
      * routers know the current active nodes in the group.
      *
      * @param event the LeaveGroupEvent notifying a node leaves the group.
      */
     // 当节点离开群组时触发的回调包装方法。
     private void removeActiveNode(LeaveGroupEvent event) {
         System.err.println("Node: " + event.getNodeName() + 
                            " with current master: " + event.getMasterName() +
                            " joins the group at: " + event.getJoinTime() +
                            ", leaves the group at: " + event.getLeaveTime() +
                            ", because of: " + event.getLeaveReason());
         synchronized (groupLock) {
             removeActiveNode(event.getNodeName());
         }
     }

     /**
      * Remove a node from the active nodes list.
      *
      * @param nodeName name of the node which leaves the group.
      */
     // 将指定名称的节点从活跃分发地址列表中移除。
     private void removeActiveNode(String nodeName) {
         InetSocketAddress address =
             nodeAddressMapping.get(nodeName);
         activeAppAddresses.remove(address);
     }

     /**
      * Updates the routers's notion of the master.
      *
      * @param newMasterName the new master
      * @param masterNodeAddress the socket address
      */
     // 更新路由器当前记录的 Master 节点身份与通信地址。
     private void updateMaster(String newMasterName,
                               InetSocketAddress masterNodeAddress){
         System.err.println("Current Master node: " + newMasterName);
         synchronized(masterLock) {
             masterName = newMasterName;
             appMasterAddress = getAppSocketAddress(masterNodeAddress);
         }
     }
    // 负责初始化路由器、配置并启动 Monitor 监控器、注册群组监听器，最后进入控制台工作循环。
     public static void main(String[] argv) throws Exception {
         final HARouter router = new HARouter(argv);

         MonitorConfig monConfig = new MonitorConfig();
         monConfig.setNodeName(repConfig.getNodeName());
         monConfig.setGroupName(repConfig.getGroupName());
         monConfig.setNodeHostPort(repConfig.getNodeHostPort());
         monConfig.setHelperHosts(repConfig.getHelperHosts());

         final Monitor monitor = new Monitor(monConfig);
         // 计算最大重试次数（默认 5 分钟超时重试）
         final int retrySleepMs = 2000;
         final int retryPeriodMins = 5;
         int maxRetries = (retryPeriodMins*60*1000)/retrySleepMs;

         while (true) {
             try {

                 /*
                  * Register this monitor, so it's a member of the replication
                  * group.
                  */
                 // 注册 Monitor 并获取当前群组的 Master。
                 ReplicationNode master = monitor.register();

                 /*
                  * Set up the initial state by explicitly querying the helper
                  * nodes. Once this is accomplished, the Router is kept current
                  * via the Monitor APIs.
                  */
                 // 获取群组信息
                 ReplicationGroup repGroup = monitor.getGroup();
                 router.initGroup(repGroup.getElectableNodes());
                 // 初始化主节点
                 router.updateMaster(master.getName(),
                                     master.getSocketAddress());

                 /* Start the listener, so it can listen for group changes. */
                 // 启动后台事件监听线程
                 monitor.startListener(router.new RouterChangeListener());

                 break;
             } catch (UnknownMasterException unknownMasterException) {
                 if (maxRetries-- == 0) {
                     /* Don't have a functioning group. */
                     throw unknownMasterException;
                 }
                 System.err.println
                     (new Date() +
                     " Waiting for a new master to be established." +
                     unknownMasterException);
                 Thread.sleep(retrySleepMs);
             }
         }
         router.doWork();
     }
}
