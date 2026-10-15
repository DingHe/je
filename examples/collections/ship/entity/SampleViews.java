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

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.SerialSerialBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.StoredValueSet;

/**
 * SampleViews defines the data bindings and collection views for the sample
 * database.
 *
 * @author Mark Hayes
 */
// SampleViews 是 Berkeley DB Java Edition (JE) 官方提供的供应链实体示例程序（collections.ship.entity 包）中的视图与实体绑定管理类。
// 与基础模块的 SampleViews 相比，该版本的核心增强在于：引入了实体绑定（Entity Bindings，即 EntityBinding / SerialSerialBinding）。
// 主要核心作用体现在：
// 实现底层键值对与高层组合实体的双向转换：通过自定义的 PartBinding、SupplierBinding 和 ShipmentBinding，将底层分离存储的 Key（如 PartKey）与 Data（如 PartData）无缝聚合成统一的业务实体对象（如 Part），从而更符合面向对象的开发习惯。
// 构建全面的集合映射与值集视图：为所有主表（零件、供应商、发货）以及二级索引表（按零件、按供应商、按城市）统一构建了 StoredSortedMap 映射视图，并提供了便捷的 StoredValueSet（实体集）访问方法。
public class SampleViews {
    // 零件（Part）主表的有序映射视图。键为 PartKey，值为组合实体 Part。
    private StoredSortedMap partMap;
    // 供应商（Supplier）主表的有序映射视图。键为 SupplierKey，值为组合实体 Supplier。
    private StoredSortedMap supplierMap;
    // 发货（Shipment）主表的有序映射视图。键为 ShipmentKey，值为组合实体 Shipment。
    private StoredSortedMap shipmentMap;
    // “发货按零件”二级索引的有序映射视图。键为 PartKey，值为发货实体 Shipment。
    private StoredSortedMap shipmentByPartMap;
    // “发货按供应商”二级索引的有序映射视图。键为 SupplierKey，值为发货实体 Shipment。
    private StoredSortedMap shipmentBySupplierMap;
    // “供应商按城市”二级索引的有序映射视图。键为城市字符串（String），值为供应商实体 Supplier。
    private StoredSortedMap supplierByCityMap;

    /**
     * Create the data bindings and collection views.
     */
    // 负责创建所有的序列化绑定器（SerialBinding）与实体绑定器（EntityBinding），并基于底层的数据库和索引初始化对应的 StoredSortedMap 视图。
    public SampleViews(SampleDatabase db) {

        // Create the data bindings.
        // In this sample, EntityBinding classes are used to bind the stored
        // key/data entry pair to a combined data object.  For keys, however,
        // the stored entry is used directly via a SerialBinding and no
        // special binding class is needed.
        //
        ClassCatalog catalog = db.getClassCatalog();
        // 实例化各个键的序列化绑定：partKeyBinding、supplierKeyBinding、shipmentKeyBinding 以及城市键的 cityKeyBinding
        // 实例化三个组合实体绑定器：partDataBinding（使用 PartBinding）、supplierDataBinding（使用 SupplierBinding）、shipmentDataBinding（使用 ShipmentBinding）。
        SerialBinding partKeyBinding =
            new SerialBinding(catalog, PartKey.class);
        EntityBinding partDataBinding =
            new PartBinding(catalog, PartKey.class, PartData.class);
        SerialBinding supplierKeyBinding =
            new SerialBinding(catalog, SupplierKey.class);
        EntityBinding supplierDataBinding =
            new SupplierBinding(catalog, SupplierKey.class,
                                SupplierData.class);
        SerialBinding shipmentKeyBinding =
            new SerialBinding(catalog, ShipmentKey.class);
        EntityBinding shipmentDataBinding =
            new ShipmentBinding(catalog, ShipmentKey.class,
                                ShipmentData.class);
        SerialBinding cityKeyBinding =
            new SerialBinding(catalog, String.class);

        // Create map views for all stores and indices.
        // StoredSortedMap is not used since the stores and indices are
        // ordered by serialized key objects, which do not provide a very
        // useful ordering.
        // 依次创建六个 StoredSortedMap 实例，将底层主表与二级索引分别与对应的绑定器关联（最后一个参数设为 true 表示支持通过 Map 修改数据）。
        partMap =
            new StoredSortedMap(db.getPartDatabase(),
                                partKeyBinding, partDataBinding, true);
        supplierMap =
            new StoredSortedMap(db.getSupplierDatabase(),
                                supplierKeyBinding, supplierDataBinding, true);
        shipmentMap =
            new StoredSortedMap(db.getShipmentDatabase(),
                                shipmentKeyBinding, shipmentDataBinding, true);
        shipmentByPartMap =
            new StoredSortedMap(db.getShipmentByPartDatabase(),
                                partKeyBinding, shipmentDataBinding, true);
        shipmentBySupplierMap =
            new StoredSortedMap(db.getShipmentBySupplierDatabase(),
                                supplierKeyBinding, shipmentDataBinding, true);
        supplierByCityMap =
            new StoredSortedMap(db.getSupplierByCityDatabase(),
                                cityKeyBinding, supplierDataBinding, true);
    }

    // The views returned below can be accessed using the java.util.Map or
    // java.util.Set interfaces, or using the StoredSortedMap and
    // StoredValueSet classes, which provide additional methods.  The entity
    // sets could be obtained directly from the Map.values() method but
    // convenience methods are provided here to return them in order to avoid
    // down-casting elsewhere.

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
    public StoredValueSet getPartSet() {

        return (StoredValueSet) partMap.values();
    }

    /**
     * Return an entity set view of the supplier storage container.
     */
    public StoredValueSet getSupplierSet() {

        return (StoredValueSet) supplierMap.values();
    }

    /**
     * Return an entity set view of the shipment storage container.
     */
    public StoredValueSet getShipmentSet() {

        return (StoredValueSet) shipmentMap.values();
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
    public final StoredSortedMap getSupplierByCityMap() {

        return supplierByCityMap;
    }

    /**
     * PartBinding is used to bind the stored key/data entry pair for a part
     * to a combined data object (entity).
     */
    private static class PartBinding extends SerialSerialBinding {

        /**
         * Construct the binding object.
         */
        private PartBinding(ClassCatalog classCatalog,
                            Class keyClass,
                            Class dataClass) {

            super(classCatalog, keyClass, dataClass);
        }

        /**
         * Create the entity by combining the stored key and data.
         */
        public Object entryToObject(Object keyInput, Object dataInput) {

            PartKey key = (PartKey) keyInput;
            PartData data = (PartData) dataInput;
            return new Part(key.getNumber(), data.getName(), data.getColor(),
                            data.getWeight(), data.getCity());
        }

        /**
         * Create the stored key from the entity.
         */
        public Object objectToKey(Object object) {

            Part part = (Part) object;
            return new PartKey(part.getNumber());
        }

        /**
         * Create the stored data from the entity.
         */
        public Object objectToData(Object object) {

            Part part = (Part) object;
            return new PartData(part.getName(), part.getColor(),
                                 part.getWeight(), part.getCity());
        }
    }

    /**
     * SupplierBinding is used to bind the stored key/data entry pair for a
     * supplier to a combined data object (entity).
     */
    private static class SupplierBinding extends SerialSerialBinding {

        /**
         * Construct the binding object.
         */
        private SupplierBinding(ClassCatalog classCatalog,
                                Class keyClass,
                                Class dataClass) {

            super(classCatalog, keyClass, dataClass);
        }

        /**
         * Create the entity by combining the stored key and data.
         */
        public Object entryToObject(Object keyInput, Object dataInput) {

            SupplierKey key = (SupplierKey) keyInput;
            SupplierData data = (SupplierData) dataInput;
            return new Supplier(key.getNumber(), data.getName(),
                                data.getStatus(), data.getCity());
        }

        /**
         * Create the stored key from the entity.
         */
        public Object objectToKey(Object object) {

            Supplier supplier = (Supplier) object;
            return new SupplierKey(supplier.getNumber());
        }

        /**
         * Create the stored data from the entity.
         */
        public Object objectToData(Object object) {

            Supplier supplier = (Supplier) object;
            return new SupplierData(supplier.getName(), supplier.getStatus(),
                                     supplier.getCity());
        }
    }

    /**
     * ShipmentBinding is used to bind the stored key/data entry pair for a
     * shipment to a combined data object (entity).
     */
    private static class ShipmentBinding extends SerialSerialBinding {

        /**
         * Construct the binding object.
         */
        private ShipmentBinding(ClassCatalog classCatalog,
                                Class keyClass,
                                Class dataClass) {

            super(classCatalog, keyClass, dataClass);
        }

        /**
         * Create the entity by combining the stored key and data.
         */
        public Object entryToObject(Object keyInput, Object dataInput) {

            ShipmentKey key = (ShipmentKey) keyInput;
            ShipmentData data = (ShipmentData) dataInput;
            return new Shipment(key.getPartNumber(), key.getSupplierNumber(),
                                data.getQuantity());
        }

        /**
         * Create the stored key from the entity.
         */
        public Object objectToKey(Object object) {

            Shipment shipment = (Shipment) object;
            return new ShipmentKey(shipment.getPartNumber(),
                                   shipment.getSupplierNumber());
        }

        /**
         * Create the stored data from the entity.
         */
        public Object objectToData(Object object) {

            Shipment shipment = (Shipment) object;
            return new ShipmentData(shipment.getQuantity());
        }
    }
}
