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

package collections.ship.basic;

import java.io.File;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * SampleDatabase defines the storage containers, indices and foreign keys
 * for the sample database.
 *
 * @author Mark Hayes
 */
// SampleDatabase 是 Berkeley DB Java Edition (JE) 官方提供的供应链示例程序（collections.ship.basic 包）中的核心基础设施类。
// 主要核心作用体现在：
// 统一管理数据库环境与表结构：它负责初始化和配置 Berkeley DB 环境（Environment），并集中打开多个相关的数据库表（如零件表、供应商表、发货表）以及用于序列化存储的类目录库。
// 构建面向对象的领域存储基础：为整个供应链示例程序提供底层的数据库连接和容器句柄，确保所有相关的持久化存储都在统一的事务模式下安全创建和关闭。
public class SampleDatabase {
    // 定义类目录（Class Catalog）数据库的存储名称。当使用 Java 序列化绑定时，该数据库负责存储所有持久化对象的类元数据结构。
    private static final String CLASS_CATALOG = "java_class_catalog";
    // 定义供应商（Supplier）数据存储表的名字。
    private static final String SUPPLIER_STORE = "supplier_store";
    // 定义零件（Part）数据存储表的名字。
    private static final String PART_STORE = "part_store";
    // 定义发货/出货（Shipment）数据存储表的名字。
    private static final String SHIPMENT_STORE = "shipment_store";
    // Berkeley DB 的环境实例对象。
    // 管理指定目录下的所有数据库文件、日志文件和共享缓存。
    private Environment env;
    // 零件数据表对应的 Database 实例。
    private Database partDb;
    // 供应商数据表对应的 Database 实例。
    private Database supplierDb;
    // 发货数据表对应的 Database 实例。
    private Database shipmentDb;
    // 类目录实例。通过包装 java_class_catalog 数据库，为 Java 对象序列化绑定（Serial Binding）提供类结构信息的存取支持。
    private StoredClassCatalog javaCatalog;

    /**
     * Open all storage containers, indices, and catalogs.
     */
    // 负责根据指定的根目录路径初始化 Berkeley DB 环境、开启事务支持、打开类目录及所有核心业务数据库。
    public SampleDatabase(String homeDirectory)
        throws DatabaseException {

        // Open the Berkeley DB environment in transactional mode.
        //
        System.out.println("Opening environment in: " + homeDirectory);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File(homeDirectory), envConfig);

        // Set the Berkeley DB config for opening all stores.
        //
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class
        // format for all database records of serial format.
        //
        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);

        // Open the Berkeley DB database for the part, supplier and shipment
        // stores.  The stores are opened with no duplicate keys allowed.
        //
        partDb = env.openDatabase(null, PART_STORE, dbConfig);

        supplierDb = env.openDatabase(null, SUPPLIER_STORE, dbConfig);

        shipmentDb = env.openDatabase(null, SHIPMENT_STORE, dbConfig);
    }

    /**
     * Return the storage environment for the database.
     */
    public final Environment getEnvironment() {

        return env;
    }

    /**
     * Return the class catalog.
     */
    public final StoredClassCatalog getClassCatalog() {

        return javaCatalog;
    }

    /**
     * Return the part storage container.
     */
    public final Database getPartDatabase() {

        return partDb;
    }

    /**
     * Return the supplier storage container.
     */
    public final Database getSupplierDatabase() {

        return supplierDb;
    }

    /**
     * Return the shipment storage container.
     */
    public final Database getShipmentDatabase() {

        return shipmentDb;
    }

    /**
     * Close all databases and the environment.
     */
    public void close()
        throws DatabaseException {

        partDb.close();
        supplierDb.close();
        shipmentDb.close();
        // And don't forget to close the catalog and the environment.
        javaCatalog.close();
        env.close();
    }
}
