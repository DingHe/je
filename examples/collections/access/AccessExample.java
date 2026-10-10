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

package collections.access;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.bind.ByteArrayBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * AccesssExample mirrors the functionality of a class by the same name used to
 * demonstrate the com.sleepycat.je Java API. This version makes use of the new
 * com.sleepycat.collections.* collections style classes to make life easier.
 *
 * @author Gregory Burd <gburd@sleepycat.com>
 */
// 提供的集合接口（com.sleepycat.collections）示例程序。
// 主要核心作用体现在：
// 演示 Java 集合框架与 Berkeley DB 的集成：展示如何通过 StoredSortedMap（基于标准 Java SortedMap 接口）像操作普通内存集合一样，直接对磁盘上的 Berkeley DB 数据库进行读写。
// 事务管理示例：演示如何结合使用 TransactionRunner 和 TransactionWorker 来编写符合 ACID 规范的事务性数据库操作。
// 交互式终端应用：提供一个命令行交互界面，接收用户输入的字符串，将其原样作为 Key、反转后的字符串作为 Value 存储到数据库中；在输入终止后，再遍历并打印数据库中的所有键值对。
public class AccessExample
    implements Runnable {
    // 控制标志。
    // 指示在打开数据库环境（Environment）或数据库（Database）时，如果它们不存在，是否允许自动创建。默认为 true。
    private static boolean create = true;
    // 程序异常退出时的状态码（通常传给 System.exit(EXIT_FAILURE)）。
    private static final int EXIT_FAILURE = 1;

    public static void usage() {

        System.out.println("usage: java " + AccessExample.class.getName() +
            " [-r] [database]\n");
        System.exit(EXIT_FAILURE);
    }

    public static void main(String[] argv) {

        boolean removeExistingDatabase = false;
        String databaseName = "access.db";

        for (int i = 0; i < argv.length; i++) {
            // 支持 -r（删除现有同名数据库）
            if (argv[i].equals("-r")) {
                removeExistingDatabase = true;
            } else if (argv[i].equals("-?")) {
                usage();
            } else if (argv[i].startsWith("-")) {
                usage();
            } else {
                if ((argv.length - i) != 1)
                    usage();
                databaseName = argv[i];
                break;
            }
        }

        try {
            // 配置并打开环境
            // 创建 EnvironmentConfig
            EnvironmentConfig envConfig = new EnvironmentConfig();
            // 开启事务支持（setTransactional(true)）
            envConfig.setTransactional(true);
            if (create) {
                // 自动创建（setAllowCreate(true)）
                envConfig.setAllowCreate(true);
            }
            // 在当前目录（new File(".")）初始化 Environment
            Environment env = new Environment(new File("."), envConfig);
            if (removeExistingDatabase) {
                env.removeDatabase(null, databaseName);
            }

            // create the app and run it
            // 实例化 AccessExample
            // 调用其 run() 方法执行核心逻辑，
            AccessExample app = new AccessExample(env, databaseName);
            app.run();
            app.close();
        } catch (DatabaseException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
    // Berkeley DB 的数据库实例对象，代表具体的某一个数据表/数据存储（Data Store）。
    private Database db;
    // 基于 Java SortedMap 接口的数据库映射视图（使用 StoredSortedMap 实现）。
    // 它将底层的 Database 包装成标准的字节数组映射，使得对 map 的增删改查操作直接映射到磁盘数据库中。
    // 与普通 Map 最大的区别是：SortedMap 中的键（Key）始终按照指定的顺序排列。
    // SortedMap 的核心特性
    // 键有序：默认按键的自然顺序排列，也可以通过 Comparator 自定义排序。
    // Key 唯一：同一个 Key 再次写入时，会替换原来的 Value。
    // 支持范围操作：可以获取指定键范围内的映射。
    private SortedMap<byte[], byte[]> map;
    // Berkeley DB 的环境（Environment）实例对象。一个 Environment 管理一组数据库文件、日志文件及共享缓存，提供事务、并发和恢复机制。
    private Environment env;

    /**
     * Constructor for the AccessExample object
     */
    public AccessExample(Environment env, String databaseName)
        throws Exception {

        this.env = env;

        /*
         * Lets mimic the db.AccessExample 100% and use plain old byte arrays
         * to store the key and data strings.
         */
        // 实例化两个 ByteArrayBinding（键和值的绑定器），用于在 Java 字节数组（byte[]）和底层存储格式之间转换。
        ByteArrayBinding keyBinding = new ByteArrayBinding();
        ByteArrayBinding dataBinding = new ByteArrayBinding();

        /* Open a data store. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        if (create) {
            dbConfig.setAllowCreate(true);
        }
        this.db = env.openDatabase(null, databaseName, dbConfig);

        /*
         * Now create a collection style map view of the data store so that it
         * is easy to work with the data in the database.
         */
        // 通过 StoredSortedMap 将底层 Database 和绑定器包装成 SortedMap<byte[], byte[]> 类型的集合视图 map。
        this.map = new StoredSortedMap<byte[], byte[]>
            (db, keyBinding, dataBinding, true);
    }

    /**
     * Close the database and environment.
     */
    // 关闭数据库和数据库环境，释放相关资源。
    void close()
        throws DatabaseException {

        db.close();
        env.close();
    }

    /**
     * Main processing method for the AccessExample object
     */
    public void run() {

        /*
         * Insert records into a Stored Sorted Map DatabaseImpl, where the key
         * is the user input and the data is the user input in reverse order.
         */
        // 监听标准输入
        final InputStreamReader reader = new InputStreamReader(System.in);

        for (; ; ) {
            // 获取用户输入的非空字符串 line
            final String line = askForLine(reader, System.out, "input> ");
            if (line == null) {
                break;
            }

            final String reversed =
                (new StringBuilder(line)).reverse().toString();

            log("adding: \"" +
                line + "\" : \"" +
                reversed + "\"");

            /* Do the work to add the key/data to the HashMap here. */
            // 使用 TransactionRunner 开启事务，并在其内部的 TransactionWorker.doWork() 回调中执行数据库写入：
            TransactionRunner tr = new TransactionRunner(env);
            try {
                tr.run(new TransactionWorker() {
                        public void doWork() {
                            try {
                                if (!map.containsKey(line.getBytes("UTF-8")))
                                    map.put(line.getBytes("UTF-8"),
                                            reversed.getBytes("UTF-8"));
                                else
                                    System.out.println("Key " + line +
                                                       " already exists.");
                            } catch (Exception e) {
                                System.err.println("doWork: " + e);
                            }
                        }
                    });
            } catch (com.sleepycat.je.DatabaseException e) {
                System.err.println("AccessExample: " + e);
                System.exit(1);
            } catch (java.lang.Exception e) {
                System.err.println("AccessExample: " + e);
                System.exit(1);
            }
        }
        System.out.println("");

        /*
         * Do the work to traverse and print the HashMap key/data pairs here
         * get iterator over map entries.
         */
        Iterator<Map.Entry<byte[], byte[]>> iter = map.entrySet().iterator();
        System.out.println("Reading data");
        while (iter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = iter.next();
            log("found \"" +
                new String(entry.getKey()) +
                "\" key with data \"" +
                new String(entry.getValue()) + "\"");
        }
    }

    /**
     * Prompts for a line, and keeps prompting until a non blank line is
     * returned. Returns null on error.
     *
     * @param  reader  stream from which to read user input
     * @param  out     stream on which to prompt for user input
     * @param  prompt  prompt to use to solicit input
     * @return         the string supplied by the user
     */
    String askForLine(InputStreamReader reader,
                      PrintStream out,
                      String prompt) {

        String result = "";
        while (result != null && result.length() == 0) {
            out.print(prompt);
            out.flush();
            result = getLine(reader);
        }
        return result;
    }

    /**
     * Read a single line. Gets the line attribute of the AccessExample object
     * Not terribly efficient, but does the job. Works for reading a line from
     * stdin or a file.
     *
     * @param reader stream from which to read the line
     *
     * @return either a String or null on EOF, if EOF appears in the middle of
     * a line, returns that line, then null on next call.
     */
    String getLine(InputStreamReader reader) {

        StringBuilder b = new StringBuilder();
        int c;
        try {
            while ((c = reader.read()) != -1 && c != '\n') {
                if (c != '\r') {
                    b.append((char) c);
                }
            }
        } catch (IOException ioe) {
            c = -1;
        }

        if (c == -1 && b.length() == 0) {
            return null;
        } else {
            return b.toString();
        }
    }

    /**
     * A simple log method.
     *
     * @param s The string to be logged.
     */
    private void log(String s) {

        System.out.println(s);
        System.out.flush();
    }
}
