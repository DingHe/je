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

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredEntrySet;
import com.sleepycat.collections.StoredMap;

/**
 * SampleViews defines the data bindings and collection views for the sample
 * database.
 *
 * @author Mark Hayes
 */
// SampleViews 是 Berkeley DB Java Edition (JE) 官方提供的供应链示例程序（collections.ship.basic 包）中的视图与绑定转换管理类。
// 核心作用体现在：
// 定义数据绑定（Data Bindings）：利用 SerialBinding 结合 ClassCatalog，将底层 Berkeley DB 的二进制字节流与领域模型对象（如零件、供应商、发货记录的 Key 和 Data 对象）进行双向序列化与反序列化转换。
// 构建 Java 集合映射视图（Collection Views）：将 SampleDatabase 中打开的各个底层 Database 包装成标准的 Java 集合类型（StoredMap），使得开发者能够完全通过操作标准 Java 内存 Map 的方式来对底层磁盘数据库进行增删改查。
// 提供便捷的视图访问接口：集中管理所有的 StoredMap 和 StoredEntrySet 视图，避免在其他业务类中重复编写绑定和转换逻辑。
public class SampleViews {
    // 零件（Part）数据的集合映射视图。它将底层的 partDb 数据库与 PartKey / PartData 对象的序列化绑定关联起来，提供键值对形式的零件数据访问。
    private StoredMap partMap;
    // 供应商（Supplier）数据的集合映射视图。它将底层的 supplierDb 数据库与 SupplierKey / SupplierData 对象的序列化绑定关联起来。
    private StoredMap supplierMap;
    // 发货（Shipment）数据的集合映射视图。它将底层的 shipmentDb 数据库与 ShipmentKey / ShipmentData 对象的序列化绑定关联起来。
    private StoredMap shipmentMap;

    /**
     * Create the data bindings and collection views.
     */
    // 负责为各类业务对象创建对应的 SerialBinding（序列化绑定器），并基于底层的 Database 构建 StoredMap 映射视图。
    public SampleViews(SampleDatabase db) {

        // In this sample, the stored key and data entries are used directly
        // rather than mapping them to separate objects. Therefore, no binding
        // classes are defined here and the SerialBinding class is used.
        //
        ClassCatalog catalog = db.getClassCatalog();
        EntryBinding partKeyBinding =
            new SerialBinding(catalog, PartKey.class);
        EntryBinding partDataBinding =
            new SerialBinding(catalog, PartData.class);
        EntryBinding supplierKeyBinding =
            new SerialBinding(catalog, SupplierKey.class);
        EntryBinding supplierDataBinding =
            new SerialBinding(catalog, SupplierData.class);
        EntryBinding shipmentKeyBinding =
            new SerialBinding(catalog, ShipmentKey.class);
        EntryBinding shipmentDataBinding =
            new SerialBinding(catalog, ShipmentData.class);

        // Create map views for all stores and indices.
        // StoredSortedMap is not used since the stores and indices are
        // ordered by serialized key objects, which do not provide a very
        // useful ordering.
        //
        partMap =
            new StoredMap(db.getPartDatabase(),
                          partKeyBinding, partDataBinding, true);
        supplierMap =
            new StoredMap(db.getSupplierDatabase(),
                          supplierKeyBinding, supplierDataBinding, true);
        shipmentMap =
            new StoredMap(db.getShipmentDatabase(),
                          shipmentKeyBinding, shipmentDataBinding, true);
    }

    // The views returned below can be accessed using the java.util.Map or
    // java.util.Set interfaces, or using the StoredMap and StoredEntrySet
    // classes, which provide additional methods.  The entry sets could be
    // obtained directly from the Map.entrySet() method, but convenience
    // methods are provided here to return them in order to avoid down-casting
    // elsewhere.

    /**
     * Return a map view of the part storage container.
     */
    public final StoredMap getPartMap() {

        return partMap;
    }

    /**
     * Return a map view of the supplier storage container.
     */
    public final StoredMap getSupplierMap() {

        return supplierMap;
    }

    /**
     * Return a map view of the shipment storage container.
     */
    public final StoredMap getShipmentMap() {

        return shipmentMap;
    }

    /**
     * Return an entry set view of the part storage container.
     */
    public final StoredEntrySet getPartEntrySet() {

        return (StoredEntrySet) partMap.entrySet();
    }

    /**
     * Return an entry set view of the supplier storage container.
     */
    public final StoredEntrySet getSupplierEntrySet() {

        return (StoredEntrySet) supplierMap.entrySet();
    }

    /**
     * Return an entry set view of the shipment storage container.
     */
    public final StoredEntrySet getShipmentEntrySet() {

        return (StoredEntrySet) shipmentMap.entrySet();
    }
}
