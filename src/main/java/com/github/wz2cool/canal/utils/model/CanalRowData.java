package com.github.wz2cool.canal.utils.model;

import java.util.ArrayList;
import java.util.List;

/**
 * canal 行数据
 *
 * @author Frank
 */
public class CanalRowData {
    /**
     * 数据库
     */
    private String database;
    /**
     * 表
     */
    private String table;
    /**
     * 列数据
     */
    private List<CanalColumnData> columnList;

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

    public List<CanalColumnData> getColumnList() {
        return columnList == null ? new ArrayList<>() : new ArrayList<>(columnList);
    }

    public void setColumnList(List<CanalColumnData> columnList) {
        this.columnList = columnList == null ? new ArrayList<>() : new ArrayList<>(columnList);
    }
}
