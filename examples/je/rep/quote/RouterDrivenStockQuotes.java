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
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class is based on {@link StockQuotes} and illustrates use of an
 * HA-aware router (implemented by {@link HARouter}), in conjunction with the
 * {@link com.sleepycat.je.rep.monitor.Monitor Monitor} class, to direct
 * application requests, based upon the type of request (read or write) and the
 * state (Master or Replica) of a node in the replication group.  This example
 * is meant to illustrate how a software load balancer might be integrated with
 * JE HA, where {@code HARouter} plays the role of the load balancer for
 * purposes of the example.
 * <p>
 * Be sure to read the {@link je.rep.quote Example Overview} first to put this
 * example into context.
 * <p>
 * In this example, unlike <code>StockQuotes</code>, only the HARouter has a
 * console associated with it. It accepts commands typed into its console and
 * forwards them as appropriate to the Master and Replicas in the group. The
 * logic for tracking the Master resides in <code>HARouter</code>, and
 * information about the state of the replication group is supplied by the
 * {@link com.sleepycat.je.rep.monitor.Monitor Monitor}. While this example
 * uses just one HARouter instance for the entire group, production
 * applications could use multiple router instances to avoid single points of
 * failure.
 * <p>
 * Each node, which in this example is an instance of
 * <code>RouterDrivenStockQuotes</code>, establishes a server socket on which
 * it can listen for requests from HARouter. The node that is currently the
 * Master will expect both write and read requests from HARouter, while nodes
 * that are Replicas will only expect read requests from the router.
 * <p>
 * The request flow between nodes in this example is shown below.
 * <pre>
 * ------------               Read requests
 * | HARouter |------------------------------------||
 * | Instance |---------------------||             ||
 * ------------                     ||             ||
 *  ||                              ||             ||
 *  || Write requests               ||             ||
 *  \/                              ||             ||
 * ---------------------------      ||             ||
 * | RouterDrivenStockQuotes |      ||             ||
 * | Instance 1: Master      |      ||             ||
 * ---------------------------      \/             ||
 *                ---------------------------      ||
 *                | RouterDrivenStockQuotes |      ||
 *                | Instance 2: Replica     |      ||
 *                ---------------------------      \/
 *                               ---------------------------
 *                               | RouterDrivenStockQuotes |
 *                               | Instance 3: Replica     |
 *                               ---------------------------
 *
 *                                       ...more Replica instances...
 * </pre>
 * <p>
 * This example is intended to be illustrative. It forwards requests as text,
 * and receives responses in text form. Actual applications may for example,
 * forward HTTP requests, or use some other application level network protocol
 * to forward such requests.
 * <p>
 * Please review the javadoc in {@link StockQuotes} for a detailed description
 * of the arguments that must be supplied at startup. The only difference is
 * that you must use the name of this class when invoking the JVM. For example,
 * the first node can be started as follows:
 *
 * <pre>
 * java je.rep.quote.RouterDrivenStockQuotes -env /tmp/stockQuotes1 \
 *                                           -nodeName n1 \
 *                                           -nodeHost node.acme.com:5001 \
 *                                           -helperHost node.acme.com:5001
 * </pre>
 *
 * In addition to starting the nodes, you will also need to start the
 * {@link HARouter} as described in its javadoc.
 *
 * @see HARouter
 */
// RouterDrivenStockQuotes 是 Berkeley DB Java Edition (JE) 高可用复制模块中的路由驱动型股票报价节点示例程序。它继承自基础的 StockQuotes 类。
// 核心作用体现在：
// 结合外部智能路由器（HARouter）实现请求自动导向：与 StockQuotes 不同，此类自身不再提供本地控制台交互界面。它通过在专用端口上监听来自 HARouter 的网络请求，实现对读写请求的自动化分配与处理。
// 完美演示负载均衡与高可用架构解耦：展示了如何在实际生产环境中将“路由/负载均衡层”（由 HARouter 担任）与“数据存储计算层”（由 RouterDrivenStockQuotes 节点集群担任）进行解耦，使后端节点专注于高可用数据服务的支撑。
public class RouterDrivenStockQuotes extends StockQuotes {
    // 负责初始化高可用复制环境配置参数
    RouterDrivenStockQuotes(String[] params) throws Exception {
        super(params);
    }

    /**
     * Overrides the method in the base class to receive requests using the
     * socket connection established by the router instead of prompting the
     * user for console input.
     */
    // 废弃了本地控制台输入，转而通过启动 ServerSocket 在专用应用端口上监听并处理来自 HARouter 路由器的网络文本请求
    @Override
    void doWork()
        throws IOException, InterruptedException {

        /*
         * Get socket address on which it can be contacted with requests by
         * the Router.
         */
        // 计算应用端口并启动 ServerSocket
        // 计算出当前节点专用的应用监听地址（即底层复制端口号 + 100 偏移量），并基于此端口创建 ServerSocket。
        InetSocketAddress appSocketAddress =
            HARouter.getAppSocketAddress(repEnv.getRepConfig().
                                         getNodeSocketAddress());
        final ServerSocket serverSocket =
            new ServerSocket(appSocketAddress.getPort());
        // 打印就绪日志
        System.err.println("Node: " + repEnv.getNodeName() +
                           " joined replication group: " +
                           repEnv.getGroup().getName() +
                           " in state: " + repEnv.getState() + ".\n" +
                           this.getClass().getSimpleName() +
                           " ready to service Router requests at: " +
                           serverSocket + "\n");
        try {
            // 循环监听与请求分发
            for (boolean done = false; !done;) {
 
                Socket socket = null;
                BufferedReader in = null;
                PrintStream out = null;

                try {
                    // 阻塞等待并接受来自 HARouter 的客户端 Socket 连接。
                    socket = serverSocket.accept();
                    // 读取路由器发过来的命令文本。
                    in = new BufferedReader
                        (new InputStreamReader(socket.getInputStream()));

                    String line = getCommandLine(null,
                                                 socket.getInputStream());

                    out = new PrintStream(socket.getOutputStream(), true);
                    // 执行相应的读写命令（若是写请求且当前是 Replica 则会返回错误提示，由路由器返回给客户端
                    done = doCommand(line, out);
                } finally {
                    QuoteUtil.closeSocketAndStreams(socket, in, out);
                }
            }
        } finally {
            serverSocket.close();
        }
    }
 
    public static void main(String[] argv) throws Exception {
        StockQuotes stockQuotes = new RouterDrivenStockQuotes(argv);
        stockQuotes.runExample();
    }
}
