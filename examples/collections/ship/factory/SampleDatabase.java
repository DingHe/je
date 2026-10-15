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

package collections.ship.factory;

import java.io.File;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.TupleSerialFactory;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ForeignKeyDeleteAction;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

/**
 * SampleDatabase defines the storage containers, indices and foreign keys
 * for the sample database.
 *
 * @author Mark Hayes
 */
// SampleDatabase 是 Berkeley DB Java Edition (JE) 官方提供的工厂模式示例程序（collections.ship.factory 包）中的核心基础设施类。
// 与前几个模块（basic 和 entity）相比，该版本的最大不同与核心增强在于：引入了 TupleSerialFactory（元组-序列化工厂）机制。
// 主要核心作用体现在：
// 统一管理绑定与键提取工厂：通过初始化 TupleSerialFactory，简化了实体绑定器（Entity Bindings）以及二级索引键创建器（Key Creators）的创建过程，使代码更加优雅和模块化。
// 集中管理主表、二级索引、外键与级联删除：同样配置了零件表、供应商表、发货表以及三个二级索引（按城市、按零件、按供应商），并设置了外键约束和 CASCADE 级联删除动作。
public class SampleDatabase {
    // 类目录数据库的名字，用于存储序列化对象的元数据。
    private static final String CLASS_CATALOG = "java_class_catalog";
    // 供应商主数据表的名字。
    private static final String SUPPLIER_STORE = "supplier_store";
    // 零件主数据表的名字。
    private static final String PART_STORE = "part_store";
    // 发货主数据表的名字。
    private static final String SHIPMENT_STORE = "shipment_store";
    // 发货按零件关联的二级索引表名字。
    private static final String SHIPMENT_PART_INDEX = "shipment_part_index";
    // 发货按供应商关联的二级索引表名字。
    private static final String SHIPMENT_SUPPLIER_INDEX =
                                    "shipment_supplier_index";
    // 供应商按城市分类的二级索引表名字。
    private static final String SUPPLIER_CITY_INDEX = "supplier_city_index";
    // Berkeley DB 环境实例，管理底层的存储文件、日志和缓存。
    private Environment env;
    // 零件主表的 Database 实例。
    private Database partDb;
    // 供应商主表的 Database 实例。
    private Database supplierDb;
    // 发货主表的 Database 实例。
    private Database shipmentDb;
    // 供应商按城市分类的二级索引数据库实例。
    private SecondaryDatabase supplierByCityDb;
    // 发货按零件分类的二级索引数据库实例。
    private SecondaryDatabase shipmentByPartDb;
    // 发货按供应商分类的二级索引数据库实例。
    private SecondaryDatabase shipmentBySupplierDb;
    // 类目录实例，管理序列化类结构。
    private StoredClassCatalog javaCatalog;
    // 核心新增属性。元组-序列化工厂实例，负责自动为实体对象生成对应的实体绑定（Entity Binding）以及二级索引键提取器（Key Creator）。
    private TupleSerialFactory factory;

    /**
     * Open all storage containers, indices, and catalogs.
     */
    public SampleDatabase(String homeDirectory)
        throws DatabaseException {

        // Open the Berkeley DB environment in transactional mode.
        // 初始化环境：创建 EnvironmentConfig，开启事务与自动创建，加载 Environment。
        System.out.println("Opening environment in: " + homeDirectory);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File(homeDirectory), envConfig);

        // Set the Berkeley DB config for opening all stores.
        // 初始化主表配置：创建 DatabaseConfig，开启事务与自动创建。
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class
        // format for all database records of serial format.
        //
        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);

        // Use the TupleSerialDbFactory for a Serial/Tuple-based database
        // where marshalling interfaces are used.
        // 初始化工厂（核心增强）：利用类目录创建 TupleSerialFactory 实例（factory = new TupleSerialFactory(javaCatalog)），
        // 后续可直接通过该工厂产出绑定器和键提取器，避免手动编写繁琐的内部类。
        factory = new TupleSerialFactory(javaCatalog);

        // Open the Berkeley DB database for the part, supplier and shipment
        // stores.  The stores are opened with no duplicate keys allowed.
        //
        partDb = env.openDatabase(null, PART_STORE, dbConfig);

        supplierDb = env.openDatabase(null, SUPPLIER_STORE, dbConfig);

        shipmentDb = env.openDatabase(null, SHIPMENT_STORE, dbConfig);

        // Open the SecondaryDatabase for the city index of the supplier store,
        // and for the part and supplier indices of the shipment store.
        // Duplicate keys are allowed since more than one supplier may be in
        // the same city, and more than one shipment may exist for the same
        // supplier or part.  A foreign key constraint is defined for the
        // supplier and part indices to ensure that a shipment only refers to
        // existing part and supplier keys.  The CASCADE delete action means
        // that shipments will be deleted if their associated part or supplier
        // is deleted.
        // 配置并打开二级索引：
        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setTransactional(true);
        secConfig.setAllowCreate(true);
        secConfig.setSortedDuplicates(true);
        // 1. 供应商按城市索引：通过 factory.getKeyCreator(Supplier.class, Supplier.CITY_KEY) 自动获取键提取器，打开 supplierByCityDb。
        secConfig.setKeyCreator(factory.getKeyCreator(Supplier.class,
                                                      Supplier.CITY_KEY));
        supplierByCityDb = env.openSecondaryDatabase(null, SUPPLIER_CITY_INDEX,
                                                     supplierDb, secConfig);

        // 2. 发货按零件索引：设置外键库为 partDb，级联删除动作为 CASCADE，通过 factory.getKeyCreator(Shipment.class, Shipment.PART_KEY) 获取键提取器，打开 shipmentByPartDb。
        secConfig.setForeignKeyDatabase(partDb);
        secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.CASCADE);
        secConfig.setKeyCreator(factory.getKeyCreator(Shipment.class,
                                                      Shipment.PART_KEY));
        shipmentByPartDb = env.openSecondaryDatabase(null, SHIPMENT_PART_INDEX,
                                                     shipmentDb, secConfig);

        secConfig.setForeignKeyDatabase(supplierDb);
        secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.CASCADE);
        secConfig.setKeyCreator(factory.getKeyCreator(Shipment.class,
                                                      Shipment.SUPPLIER_KEY));
        shipmentBySupplierDb = env.openSecondaryDatabase(null,
                                                     SHIPMENT_SUPPLIER_INDEX,
                                                     shipmentDb, secConfig);
    }

    /**
     * Return the tuple-serial factory.
     */
    public final TupleSerialFactory getFactory() {

        return factory;
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
     * Return the shipment-by-part index.
     */
    public final SecondaryDatabase getShipmentByPartDatabase() {

        return shipmentByPartDb;
    }

    /**
     * Return the shipment-by-supplier index.
     */
    public final SecondaryDatabase getShipmentBySupplierDatabase() {

        return shipmentBySupplierDb;
    }

    /**
     * Return the supplier-by-city index.
     */
    public final SecondaryDatabase getSupplierByCityDatabase() {

        return supplierByCityDb;
    }

    /**
     * Close all databases and the environment.
     */
    public void close()
        throws DatabaseException {

        // Close secondary databases, then primary databases.
        supplierByCityDb.close();
        shipmentByPartDb.close();
        shipmentBySupplierDb.close();
        partDb.close();
        supplierDb.close();
        shipmentDb.close();
        // And don't forget to close the catalog and the environment.
        javaCatalog.close();
        env.close();
    }
}
