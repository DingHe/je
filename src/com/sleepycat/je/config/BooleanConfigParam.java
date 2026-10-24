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

package com.sleepycat.je.config;

/**
 * A JE configuration parameter with an boolean value.
 */
// 专门用于表示和处理布尔型（Boolean）配置参数（即取值为 true 或 false 的配置项）
// 布尔参数专属容器：继承自 ConfigParam，专门为布尔类型的配置项提供元数据支持。
// 类型转换与校验：由于底层的 ConfigParam 统一使用字符串（String）来存储和传递配置值，该类负责将布尔型的默认值转换为字符串传给父类，并提供专门针对布尔值的严格校验逻辑，确保配置项只能是有效的 true 或 false。
public class BooleanConfigParam extends ConfigParam {

    private static final String DEBUG_NAME =
        BooleanConfigParam.class.getName();

    /**
     * Set a boolean parameter w/default.
     * @param configName
     * @param defaultValue
     * @param forReplication true if param is for replication
     */
    public BooleanConfigParam(String configName,
                              boolean defaultValue,
                              boolean mutable,
                              boolean forReplication) {
        /* defaultValue must not be null. */
        super(configName,
              Boolean.valueOf(defaultValue).toString(),
              mutable,
              forReplication);
    }

    /**
     * Make sure that value is a valid string for booleans.
     */
    @Override
    public void validateValue(String value)
        throws IllegalArgumentException {

        if (!value.trim().equalsIgnoreCase(Boolean.FALSE.toString()) &&
            !value.trim().equalsIgnoreCase(Boolean.TRUE.toString())) {
            throw new IllegalArgumentException
                (DEBUG_NAME + ": " +  value + " not valid boolean " + name);
        }
    }
}
