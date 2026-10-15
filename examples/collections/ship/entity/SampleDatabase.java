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

package collections.ship.entity;

import java.io.File;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialSerialKeyCreator;
import com.sleepycat.bind.serial.StoredClassCatalog;
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
// SampleDatabase 是 Berkeley DB Java Edition (JE) 官方提供的供应链实体示例程序（collections.ship.entity 包）中的核心基础设施类。
// 与之前基础模块的 SampleDatabase 相比，该版本的最大不同与核心增强在于：引入了二级索引（Secondary Databases）与外键约束（Foreign Key Constraints）机制。
// 主要核心作用体现在：
// 集中管理主存储表与索引：不仅初始化了核心的零件表、供应商表、发货表和类目录，还额外创建了三个二级索引表：供应商按城市索引、发货按零件索引、发货按供应商索引。
// 定义外键关系与级联删除：通过 SecondaryConfig 配置了外键约束，确保发货记录（Shipment）引用的零件或供应商必须存在；同时配置了级联删除（CASCADE），当主表中的零件或供应商被删除时，关联的发货记录会自动被删除。
// 自定义键提取器（Key Creators）：内置了三个静态内部类，用于从序列化的主键/值中动态提取二级索引键。
public class SampleDatabase {
    // 类目录（Class Catalog）数据库的存储名称，用于保存序列化对象的类元数据。
    private static final String CLASS_CATALOG = "java_class_catalog";
    // 供应商（Supplier）主数据表的名字。
    private static final String SUPPLIER_STORE = "supplier_store";
    // 零件（Part）主数据表的名字。
    private static final String PART_STORE = "part_store";
    // 发货（Shipment）主数据表的名字。
    private static final String SHIPMENT_STORE = "shipment_store";
    // 发货表按零件关联的二级索引表名字。
    private static final String SHIPMENT_PART_INDEX = "shipment_part_index";
    // 发货表按供应商关联的二级索引表名字。
    private static final String SHIPMENT_SUPPLIER_INDEX =
        "shipment_supplier_index";
    // 供应商按城市分类的二级索引表名字。
    private static final String SUPPLIER_CITY_INDEX = "supplier_city_index";
    // Berkeley DB 环境实例，统一管理目录下的所有底层文件与缓存。
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
    // 类目录实例，为序列化绑定提供类结构支持。
    private StoredClassCatalog javaCatalog;

    /**
     * Open all storage containers, indices, and catalogs.
     */
    // 负责初始化 Berkeley DB 环境、打开类目录、创建三个主数据表，并配置外键与二级索引后打开对应的 SecondaryDatabase。
    public SampleDatabase(String homeDirectory)
        throws DatabaseException {

        // Open the Berkeley DB environment in transactional mode.
        // 创建 EnvironmentConfig，开启事务支持与自动创建，在指定目录加载 Environment。
        System.out.println("Opening environment in: " + homeDirectory);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File(homeDirectory), envConfig);

        // Set the Berkeley DB config for opening all stores.
        // 初始化主表：创建通用的 DatabaseConfig（支持事务和自动创建），依次打开 partDb、supplierDb 和 shipmentDb。
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

        // Open the SecondaryDatabase for the city index of the supplier store,
        // and for the part and supplier indices of the shipment store.
        // Duplicate keys are allowed since more than one supplier may be in
        // the same city, and more than one shipment may exist for the same
        // supplier or part.  A foreign key constraint is defined for the
        // supplier and part indices to ensure that a shipment only refers to
        // existing part and supplier keys.  The CASCADE delete action means
        // that shipments will be deleted if their associated part or supplier
        // is deleted.
        // 配置并打开二级索引（核心逻辑）：
        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setTransactional(true);
        secConfig.setAllowCreate(true);
        // 设置允许排序重复键（setSortedDuplicates(true)）
        // 因为一个城市可能有多个供应商，一个供应商或零件也可能有多个发货记录。
        secConfig.setSortedDuplicates(true);
        // 1. 供应商按城市索引：通过 SupplierByCityKeyCreator 从 SupplierData 中提取 city 作为二级索引键，打开 supplierByCityDb。
        secConfig.setKeyCreator(
            new SupplierByCityKeyCreator(javaCatalog,
                                         SupplierKey.class,
                                         SupplierData.class,
                                         String.class));
        supplierByCityDb = env.openSecondaryDatabase(null, SUPPLIER_CITY_INDEX,
                                                     supplierDb, secConfig);
        // 2. 发货按零件索引：将外键库关联到 partDb，设置级联删除（ForeignKeyDeleteAction.CASCADE），通过 ShipmentByPartKeyCreator 从 ShipmentKey 中提取零件编号作为索引键，打开 shipmentByPartDb。
        secConfig.setForeignKeyDatabase(partDb);
        secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.CASCADE);
        secConfig.setKeyCreator(
            new ShipmentByPartKeyCreator(javaCatalog,
                                         ShipmentKey.class,
                                         ShipmentData.class,
                                         PartKey.class));
        shipmentByPartDb = env.openSecondaryDatabase(null, SHIPMENT_PART_INDEX,
                                                     shipmentDb, secConfig);
        // 3. 发货按供应商索引：将外键库关联到 supplierDb，同样设置级联删除，通过 ShipmentBySupplierKeyCreator 从 ShipmentKey 中提取供应商编号作为索引键，打开 shipmentBySupplierDb。
        secConfig.setForeignKeyDatabase(supplierDb);
        secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.CASCADE);
        secConfig.setKeyCreator(
            new ShipmentBySupplierKeyCreator(javaCatalog,
                                             ShipmentKey.class,
                                             ShipmentData.class,
                                             SupplierKey.class));
        shipmentBySupplierDb = env.openSecondaryDatabase(null,
                                                     SHIPMENT_SUPPLIER_INDEX,
                                                     shipmentDb, secConfig);
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
     * Close all stores (closing a store automatically closes its indices).
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

    /**
     * The SecondaryKeyCreator for the SupplierByCity index.  This is an
     * extension of the abstract class SerialSerialKeyCreator, which implements
     * SecondaryKeyCreator for the case where the data keys and value are all
     * of the serial format.
     */
    // 用于 SupplierByCity 索引的键提取器。
    private static class SupplierByCityKeyCreator
        extends SerialSerialKeyCreator {

        /**
         * Construct the city key extractor.
         * @param catalog is the class catalog.
         * @param primaryKeyClass is the supplier key class.
         * @param valueClass is the supplier value class.
         * @param indexKeyClass is the city key class.
         */
        private SupplierByCityKeyCreator(ClassCatalog catalog,
                                         Class<SupplierKey> primaryKeyClass,
                                         Class<SupplierData> valueClass,
                                         Class<String> indexKeyClass) {

            super(catalog, primaryKeyClass, valueClass, indexKeyClass);
        }

        /**
         * Extract the city key from a supplier key/value pair.  The city key
         * is stored in the supplier value, so the supplier key is not used.
         */
        // 从供应商的 Value（即 SupplierData）中提取出城市名称（String）作为二级索引的键返回（在此场景下不使用主键）。
        public Object createSecondaryKey(Object primaryKeyInput,
                                         Object valueInput) {

            SupplierData supplierData = (SupplierData) valueInput;
            return supplierData.getCity();
        }
    }

    /**
     * The SecondaryKeyCreator for the ShipmentByPart index.  This is an
     * extension of the abstract class SerialSerialKeyCreator, which implements
     * SecondaryKeyCreator for the case where the data keys and value are all
     * of the serial format.
     */
    private static class ShipmentByPartKeyCreator
        extends SerialSerialKeyCreator {

        /**
         * Construct the part key extractor.
         * @param catalog is the class catalog.
         * @param primaryKeyClass is the shipment key class.
         * @param valueClass is the shipment value class.
         * @param indexKeyClass is the part key class.
         */
        private ShipmentByPartKeyCreator(ClassCatalog catalog,
                                         Class<ShipmentKey> primaryKeyClass,
                                         Class<ShipmentData> valueClass,
                                         Class<PartKey> indexKeyClass) {

            super(catalog, primaryKeyClass, valueClass, indexKeyClass);
        }

        /**
         * Extract the part key from a shipment key/value pair.  The part key
         * is stored in the shipment key, so the shipment value is not used.
         */
        public Object createSecondaryKey(Object primaryKeyInput,
                                         Object valueInput) {

            ShipmentKey shipmentKey = (ShipmentKey) primaryKeyInput;
            return new PartKey(shipmentKey.getPartNumber());
        }
    }

    /**
     * The SecondaryKeyCreator for the ShipmentBySupplier index.  This is an
     * extension of the abstract class SerialSerialKeyCreator, which implements
     * SecondaryKeyCreator for the case where the data keys and value are all
     * of the serial format.
     */
    private static class ShipmentBySupplierKeyCreator
        extends SerialSerialKeyCreator {

        /**
         * Construct the supplier key extractor.
         * @param catalog is the class catalog.
         * @param primaryKeyClass is the shipment key class.
         * @param valueClass is the shipment value class.
         * @param indexKeyClass is the supplier key class.
         */
        private ShipmentBySupplierKeyCreator(ClassCatalog catalog,
                                             Class<ShipmentKey> primaryKeyClass,
                                             Class<ShipmentData> valueClass,
                                             Class<SupplierKey> indexKeyClass) {

            super(catalog, primaryKeyClass, valueClass, indexKeyClass);
        }

        /**
         * Extract the supplier key from a shipment key/value pair.  The part
         * key is stored in the shipment key, so the shipment value is not
         * used.
         */
        public Object createSecondaryKey(Object primaryKeyInput,
                                         Object valueInput) {

            ShipmentKey shipmentKey = (ShipmentKey) primaryKeyInput;
            return new SupplierKey(shipmentKey.getSupplierNumber());
        }
    }
}
