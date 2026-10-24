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

import com.sleepycat.je.EnvironmentFailureException;

/**
 * A ConfigParam embodies the metadata about a JE configuration parameter:
 * the parameter name, default value, and a validation method.
 *
 * Validation can be done in the scope of this parameter, or as a function of
 * other parameters.
 */
// 配置参数的元数据容器，用于定义和描述 JE 中每一个具体配置项的元信息
// 封装配置元数据：ConfigParam 封装了单个配置参数的核心属性，包括参数名称 (name)、默认值 (defaultValue)、是否支持运行时动态修改 (mutable)、是否属于复制/HA模块 (forReplication) 以及是否为多值参数 (isMultiValueParam)。
// 统一校验与注册：在创建 ConfigParam 实例时，它会自动对参数名和默认值进行合法性校验，并将其自动注册到 JE 全局的支持参数列表（EnvironmentParams.SUPPORTED_PARAMS）中，供整个配置管理系统（如 DbConfigManager）统一调用和校验。
public class ConfigParam {
    // 存储配置参数的名称（例如 "je.maxMemory" 或 "je.rep.groupName"）。如果是多值参数，这里存储的是截断后的基础名称。
    protected String name;
    // 存储该配置项的默认值（以字符串形式保存）。当应用程序未显式设置该参数时，系统将采用此默认值。
    private String defaultValue;
    // 标识该配置参数是否是可变参数。如果为 true，表示该参数在环境创建后（运行期间）可以通过 setMutableConfig 动态修改；如果为 false，则属于不可变参数（只能在环境初始化时指定）。
    private boolean mutable;
    // 标识该配置参数是否属于高可用/复制（Replication）模块的参数。用于在解析和校验时区分普通单机配置与复制相关配置。
    private boolean forReplication;
    // 标识该参数是否为多值参数（Multi-Value Param，即名称中带有 .# 标识的参数，允许配置多个同类项，如多个远程地址）。
    private boolean isMultiValueParam;

    /*
     * Create a String parameter.
     */
    public ConfigParam(String configName,
                       String configDefault,
                       boolean mutable,
                       boolean forReplication)
        throws IllegalArgumentException {

        if (configName == null) {
            name = null;
        } else {

            /*
             * For Multi-Value params (i.e. those whose names end with ".#"),
             * strip the .# off the end of the name before storing and flag it
             * with isMultiValueParam=true.
             */
            int mvFlagIdx = configName.indexOf(".#");
            if (mvFlagIdx < 0) {
                name = configName;
                isMultiValueParam = false;
            } else {
                name = configName.substring(0, mvFlagIdx);
                isMultiValueParam = true;
            }
        }

        defaultValue = configDefault;
        this.mutable = mutable;
        this.forReplication = forReplication;

        /* Check that the name and default value are valid */
        validateName(name);
        validateValue(configDefault);

        /* Add it the list of supported environment parameters. */
        EnvironmentParams.addSupportedParam(this);
    }

    /*
     * Return the parameter name of a multi-value parameter.  e.g.
     * "je.rep.remote.address.foo" => "je.rep.remote.address"
     */
    public static String multiValueParamName(String paramName) {
        int mvParamIdx = paramName.lastIndexOf('.');
        if (mvParamIdx < 0) {
            return null;
        }
        return paramName.substring(0, mvParamIdx);
    }

    /*
     * Return the label of a multi-value parameter.  e.g.
     * "je.rep.remote.address.foo" => foo.
     */
    public static String mvParamIndex(String paramName) {

        int mvParamIdx = paramName.lastIndexOf('.');
        return paramName.substring(mvParamIdx + 1);
    }

    public String getName() {
        return name;
    }

    public String getDefault() {
        return defaultValue;
    }

    public boolean isMutable() {
        return mutable;
    }

    public boolean isForReplication() {
        return forReplication;
    }

    public void setForReplication(boolean forReplication) {
        this.forReplication = forReplication;
    }

    public boolean isMultiValueParam() {
        return isMultiValueParam;
    }

    /**
     * A param name can't be null or 0 length
     */
    private void validateName(String name)
        throws IllegalArgumentException {

        if ((name == null) || (name.length() < 1)) {
            throw EnvironmentFailureException.unexpectedState
                ("A configuration parameter name can't be null or 0 length");
        }
    }

    /**
     * Validate your value. (No default validation for strings.)
     * May be overridden for (e.g.) Multi-value params.
     *
     * @throws IllegalArgumentException via XxxConfig.setXxx methods and
     * XxxConfig(Properties) ctor.
     */
    public void validateValue(String value)
        throws IllegalArgumentException {

    }

    @Override
    public String toString() {
        return name;
    }
}
