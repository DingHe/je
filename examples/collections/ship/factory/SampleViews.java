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

import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.StoredSortedValueSet;
import com.sleepycat.collections.TupleSerialFactory;

/**
 * SampleViews defines the data bindings and collection views for the sample
 * database.
 *
 * @author Mark Hayes
 */
// SampleViews 是 Berkeley DB Java Edition (JE) 官方提供的工厂模式示例程序（collections.ship.factory 包）中的视图管理类。
// 与前几个模块（basic 和 entity）相比，该版本的最大不同与核心简化在于：借助了 TupleSerialFactory（工厂机制）。
// 主要核心作用体现在：
// 极大简化绑定与视图创建：不再需要手动编写冗长的 EntityBinding、SerialBinding 内部类，而是直接通过 TupleSerialFactory.newSortedMap(...) 一行代码自动生成底层绑定并构建有序映射视图（StoredSortedMap）。
// 集中提供有序映射与有序实体集视图：统一管理所有主表及二级索引的 StoredSortedMap 视图，并提供便捷的 StoredSortedValueSet（有序实体集）方法，方便上层业务代码进行面向对象的查询和遍历。
public class SampleViews {

    private StoredSortedMap partMap;
    private StoredSortedMap supplierMap;
    private StoredSortedMap shipmentMap;
    private StoredSortedMap shipmentByPartMap;
    private StoredSortedMap shipmentBySupplierMap;
    private StoredSortedMap supplierByCityMap;

    /**
     * Create the data bindings and collection views.
     */
    public SampleViews(SampleDatabase db) {

        // Use the TupleSerialFactory for a Serial/Tuple-based database
        // where marshalling interfaces are used.
        //
        TupleSerialFactory factory = db.getFactory();

        // Create map views for all stores and indices.
        // StoredSortedMap is used since the stores and indices are ordered
        // (they use the DB_BTREE access method).
        //
        partMap =
            factory.newSortedMap(db.getPartDatabase(),
                                 PartKey.class, Part.class, true);
        supplierMap =
            factory.newSortedMap(db.getSupplierDatabase(),
                                 SupplierKey.class, Supplier.class, true);
        shipmentMap =
            factory.newSortedMap(db.getShipmentDatabase(),
                                 ShipmentKey.class, Shipment.class, true);
        shipmentByPartMap =
            factory.newSortedMap(db.getShipmentByPartDatabase(),
                                 PartKey.class, Shipment.class, true);
        shipmentBySupplierMap =
            factory.newSortedMap(db.getShipmentBySupplierDatabase(),
                                 SupplierKey.class, Shipment.class, true);
        supplierByCityMap =
            factory.newSortedMap(db.getSupplierByCityDatabase(),
                                 String.class, Supplier.class, true);
    }

    // The views returned below can be accessed using the java.util.Map or
    // java.util.Set interfaces, or using the StoredMap and StoredValueSet
    // classes, which provide additional methods.  The entity sets could be
    // obtained directly from the Map.values() method but convenience methods
    // are provided here to return them in order to avoid down-casting
    // elsewhere.

    /**
     * Return a map view of the part storage container.
     */
    public StoredSortedMap getPartMap() {

        return partMap;
    }

    /**
     * Return a map view of the supplier storage container.
     */
    public StoredSortedMap getSupplierMap() {

        return supplierMap;
    }

    /**
     * Return a map view of the shipment storage container.
     */
    public StoredSortedMap getShipmentMap() {

        return shipmentMap;
    }

    /**
     * Return an entity set view of the part storage container.
     */
    public StoredSortedValueSet getPartSet() {

        return (StoredSortedValueSet) partMap.values();
    }

    /**
     * Return an entity set view of the supplier storage container.
     */
    public StoredSortedValueSet getSupplierSet() {

        return (StoredSortedValueSet) supplierMap.values();
    }

    /**
     * Return an entity set view of the shipment storage container.
     */
    public StoredSortedValueSet getShipmentSet() {

        return (StoredSortedValueSet) shipmentMap.values();
    }

    /**
     * Return a map view of the shipment-by-part index.
     */
    public StoredSortedMap getShipmentByPartMap() {

        return shipmentByPartMap;
    }

    /**
     * Return a map view of the shipment-by-supplier index.
     */
    public StoredSortedMap getShipmentBySupplierMap() {

        return shipmentBySupplierMap;
    }

    /**
     * Return a map view of the supplier-by-city index.
     */
    public StoredSortedMap getSupplierByCityMap() {

        return supplierByCityMap;
    }
}
