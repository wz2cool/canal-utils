package com.github.wz2cool.canal.utils.model;

/**
 * canal 列数据
 *
 * @author Frank
 */
public class CanalColumnData {
    /**
     * 列名
     */
    private String name;
    /**
     * 是否是key
     */
    private boolean isKey;
    /**
     * 是否是被更新
     */
    private boolean updated;
    /**
     * 值
     */
    private String value;
    /**
     * 老的值
     */
    private String oldValue;
    /**
     * mysql 字段类型
     */
    private String mysqlType;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }

    public String getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(String mysqlType) {
        this.mysqlType = mysqlType;
    }
}
