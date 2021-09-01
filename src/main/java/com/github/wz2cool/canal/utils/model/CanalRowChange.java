package com.github.wz2cool.canal.utils.model;

import java.util.ArrayList;
import java.util.List;

/**
 * canal 行改动
 *
 * @author Frank
 */
public class CanalRowChange {
    /**
     * 流水号
     */
    private Long id;
    /**
     * 是否是ddl 语句
     */
    private boolean isDdl;
    /**
     * sql
     */
    private String sql;
    /**
     * 修改类型 类似 insert, update, alter
     */
    private String type;
    /**
     * 时间戳
     */
    private Long es;
    /**
     * 数据库
     */
    private String database;
    /**
     * 表
     */
    private String table;
    /**
     * 行数据
     */
    private List<CanalRowData> rowDataList;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public boolean isDdl() {
        return isDdl;
    }

    public void setDdl(boolean ddl) {
        isDdl = ddl;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<CanalRowData> getRowDataList() {
        return rowDataList == null ? new ArrayList<>() : new ArrayList<>(rowDataList);
    }

    public void setRowDataList(List<CanalRowData> rowDataList) {
        this.rowDataList = rowDataList == null ? new ArrayList<>() : new ArrayList<>(rowDataList);
    }
}
