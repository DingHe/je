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

package collections.hello;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * @author Mark Hayes
 */
// 提供的入门级集合示例程序
// 核心作用体现在：
// 演示 Java 集合框架与 Berkeley DB 的集成：展示如何使用 StoredSortedMap（将底层的 Berkeley DB 映射为标准的 Java SortedMap），并配合 Tuple（元组）绑定 和 Serial（序列化）绑定 来存储非简单的类型（如 Integer 作为键、String 作为值）。
// 类目录（ClassCatalog）的使用：演示当需要存储复杂对象或 Java 序列化对象时，如何通过独立的 ClassCatalog 数据库来管理对象的类定义信息。
// 事务性工作单元模式（TransactionWorker）：该类实现了 TransactionWorker 接口，展示了如何通过 TransactionRunner 干净、规范地在事务边界内执行数据库操作。
public class HelloDatabaseWorld implements TransactionWorker {
    // 用作要写入数据库的文本数据源（分别对应索引 0、1、2）。
    private static final String[] INT_NAMES = {
        "Hello", "Database", "World",
    };
    // 控制标志。指示在创建环境或数据库时，如果它们不存在，是否允许自动创建。默认为 true。
    private static boolean create = true;
    // Berkeley DB 的环境（Environment）实例对象，管理当前应用目录下的数据库文件及缓存。
    private Environment env;
    // 类目录实例。在使用 Java 序列化绑定（SerialBinding）时，JE 需要一个 ClassCatalog 来记录和存储序列化对象的类元数据结构。
    private ClassCatalog catalog;
    //Berkeley DB 数据库实例，代表名为 "helloworld" 的实际数据存储文件。
    private Database db;
    // 基于 Java SortedMap 接口的数据库映射视图（由 StoredSortedMap 实现）。
    // 将 Key（Integer）和 Value（String）自动转换为底层字节存储，使开发者能像操作标准 Java 内存 Map 一样操作磁盘数据库。
    private SortedMap<Integer, String> map;

    /** Creates the environment and runs a transaction */
    public static void main(String[] argv)
        throws Exception {

        String dir = "./tmp";

        // environment is transactional
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        if (create) {
            envConfig.setAllowCreate(true);
        }
        Environment env = new Environment(new File(dir), envConfig);

        // create the application and run a transaction
        HelloDatabaseWorld worker = new HelloDatabaseWorld(env);
        TransactionRunner runner = new TransactionRunner(env);
        try {
            // open and access the database within a transaction
            runner.run(worker);
        } finally {
            // close the database outside the transaction
            worker.close();
        }
    }

    /** Creates the database for this application */
    private HelloDatabaseWorld(Environment env)
        throws Exception {

        this.env = env;
        open();
    }

    /** Performs work within a transaction. */
    public void doWork() {
        writeAndRead();
    }

    /** Opens the database and creates the Map. */
    private void open()
        throws Exception {

        // use a generic database configuration
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        if (create) {
            dbConfig.setAllowCreate(true);
        }

        // catalog is needed for serial bindings (java serialization)
        Database catalogDb = env.openDatabase(null, "catalog", dbConfig);
        catalog = new StoredClassCatalog(catalogDb);

        // use Integer tuple binding for key entries
        TupleBinding<Integer> keyBinding =
            TupleBinding.getPrimitiveBinding(Integer.class);

        // use String serial binding for data entries
        SerialBinding<String> dataBinding =
            new SerialBinding<String>(catalog, String.class);

        this.db = env.openDatabase(null, "helloworld", dbConfig);

        // create a map view of the database
        this.map = new StoredSortedMap<Integer, String>
            (db, keyBinding, dataBinding, true);
    }

    /** Closes the database. */
    private void close()
        throws Exception {

        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    /** Writes and reads the database via the Map. */
    private void writeAndRead() {

        // check for existing data
        Integer key = new Integer(0);
        String val = map.get(key);
        if (val == null) {
            System.out.println("Writing data");
            // write in reverse order to show that keys are sorted
            for (int i = INT_NAMES.length - 1; i >= 0; i -= 1) {
                map.put(new Integer(i), INT_NAMES[i]);
            }
        }
        // get iterator over map entries
        Iterator<Map.Entry<Integer, String>> iter = map.entrySet().iterator();
        System.out.println("Reading data");
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            System.out.println(entry.getKey().toString() + ' ' +
                               entry.getValue());
        }
    }
}
