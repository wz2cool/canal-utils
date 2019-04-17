package com.github.wz2cool.canal.utils.model;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;

public class AlterColumnExpression {
    private AlterOperation operation;
    private String tableName;
    private String columnName;
    private String oldColName;
    private AlterExpression.ColumnDataType colDataType;

    public AlterOperation getOperation() {
        return operation;
    }

    public void setOperation(AlterOperation operation) {
        this.operation = operation;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getOldColName() {
        return oldColName;
    }

    public void setOldColName(String oldColName) {
        this.oldColName = oldColName;
    }

    public AlterExpression.ColumnDataType getColDataType() {
        return colDataType;
    }

    public void setColDataType(AlterExpression.ColumnDataType colDataType) {
        this.colDataType = colDataType;
    }
}
