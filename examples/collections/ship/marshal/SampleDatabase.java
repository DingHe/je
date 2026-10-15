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

package collections.ship.marshal;

import java.io.File;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.serial.TupleSerialKeyCreator;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
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
// 该版本的核心特征与增强在于：引入了 MarshalledEntity（封送实体接口）与 TupleSerialKeyCreator 机制。
// 主要核心作用体现在：
// 支持元组-序列化混合格式的二级索引：该模块的底层主键采用元组（Tuple）格式，而数据采用序列化（Serial）格式。
// 统一的键提取器（MarshalledKeyCreator）：通过自定义的内部类 MarshalledKeyCreator 继承自 TupleSerialKeyCreator，利用统一的接口方法来动态反序列化主键并提取二级索引键，从而极大地简化了多表、多索引的键抽取逻辑。
// 管理主表、索引与级联删除：同样负责初始化零件、供应商、发货三大主表，以及对应的三个二级索引，并配置了外键约束和 CASCADE 级联删除。
public class SampleDatabase {

    private static final String CLASS_CATALOG = "java_class_catalog";
    private static final String SUPPLIER_STORE = "supplier_store";
    private static final String PART_STORE = "part_store";
    private static final String SHIPMENT_STORE = "shipment_store";
    private static final String SHIPMENT_PART_INDEX = "shipment_part_index";
    private static final String SHIPMENT_SUPPLIER_INDEX =
        "shipment_supplier_index";
    private static final String SUPPLIER_CITY_INDEX = "supplier_city_index";

    private Environment env;
    private Database partDb;
    private Database supplierDb;
    private Database shipmentDb;
    private SecondaryDatabase supplierByCityDb;
    private SecondaryDatabase shipmentByPartDb;
    private SecondaryDatabase shipmentBySupplierDb;
    private StoredClassCatalog javaCatalog;

    /**
     * Open all storage containers, indices, and catalogs.
     */
    public SampleDatabase(String homeDirectory)
        throws DatabaseException {

        // Open the Berkeley DB environment in transactional mode.
        // 创建 EnvironmentConfig，开启事务支持与自动创建，加载 Environment。
        System.out.println("Opening environment in: " + homeDirectory);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File(homeDirectory), envConfig);

        // Set the Berkeley DB config for opening all stores.
        // 创建 DatabaseConfig，开启事务支持与自动创建。
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class
        // format for all database records of serial format.
        // 打开 catalogDb 并初始化 javaCatalog
        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);

        // Open the Berkeley DB database for the part, supplier and shipment
        // stores.  The stores are opened with no duplicate keys allowed.
        // 依次打开 partDb、supplierDb 和 shipmentDb
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
        // 配置并打开二级索引（核心逻辑）
        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setTransactional(true);
        secConfig.setAllowCreate(true);
        secConfig.setSortedDuplicates(true);
        // 1. 供应商按城市索引：通过 new MarshalledKeyCreator(javaCatalog, Supplier.class, Supplier.CITY_KEY) 创建键提取器，打开 supplierByCityDb。
        secConfig.setKeyCreator(new MarshalledKeyCreator(javaCatalog,
                                                         Supplier.class,
                                                         Supplier.CITY_KEY));
        supplierByCityDb = env.openSecondaryDatabase(null, SUPPLIER_CITY_INDEX,
                                                     supplierDb, secConfig);
        // 2. 发货按零件索引：设置外键关联到 partDb、级联删除动作为 CASCADE，通过 MarshalledKeyCreator 提取零件编号索引键，打开 shipmentByPartDb。
        secConfig.setForeignKeyDatabase(partDb);
        secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.CASCADE);
        secConfig.setKeyCreator(new MarshalledKeyCreator(javaCatalog,
                                                         Shipment.class,
                                                         Shipment.PART_KEY));
        shipmentByPartDb = env.openSecondaryDatabase(null, SHIPMENT_PART_INDEX,
                                                     shipmentDb, secConfig);
        // 3. 发货按供应商索引：设置外键关联到 supplierDb、级联删除动作为 CASCADE，通过 MarshalledKeyCreator 提取供应商编号索引键，打开 shipmentBySupplierDb。
        secConfig.setForeignKeyDatabase(supplierDb);
        secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.CASCADE);
        secConfig.setKeyCreator(new MarshalledKeyCreator(javaCatalog,
                                                         Shipment.class,
                                                     Shipment.SUPPLIER_KEY));
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
     * The SecondaryKeyCreator for MarshalledEntity objects.  This is an
     * extension of the abstract class TupleSerialKeyCreator, which implements
     * SecondaryKeyCreator for the case where the data keys are of the format
     * TupleFormat and the data values are of the format SerialFormat.
     */
    // 通用的二级索引键提取器。继承自 TupleSerialKeyCreator（适用于键为 Tuple 格式、值为 Serial 格式的场景），通过泛型化和接口回调机制，避免为每个索引编写单独的 KeyCreator。
    private static class MarshalledKeyCreator
        extends TupleSerialKeyCreator {

        private String keyName;

        /**
         * Construct the key creator.
         * @param catalog is the class catalog.
         * @param valueClass is the supplier value class.
         * @param keyName is the key name passed to the marshalling methods.
         */
        private MarshalledKeyCreator(ClassCatalog catalog,
                                     Class valueClass,
                                     String keyName) {

            super(catalog, valueClass);
            this.keyName = keyName;
        }

        /**
         * Extract the city key from a supplier key/value pair.  The city key
         * is stored in the supplier value, so the supplier key is not used.
         */
        public boolean createSecondaryKey(TupleInput primaryKeyInput,
                                          Object valueInput,
                                          TupleOutput indexKeyOutput) {

            // the primary key is unmarshalled before marshalling the index
            // key, to account for cases where the index key is composed of
            // data elements from the primary key
            MarshalledEntity entity = (MarshalledEntity) valueInput;
            entity.unmarshalPrimaryKey(primaryKeyInput);
            return entity.marshalSecondaryKey(keyName, indexKeyOutput);
        }
    }
}
