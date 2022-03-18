package com.github.wz2cool.canal.utils.model;

import net.sf.jsqlparser.statement.create.table.ColDataType;

public class AlterColumnExpression {
    private EnhancedAlterOperation operation;
    private String tableName;
    private String columnName;
    private String colOldName;
    private ColDataType colDataType;
    private boolean unsignedFlag;

    public EnhancedAlterOperation getOperation() {
        return operation;
    }

    public void setOperation(EnhancedAlterOperation operation) {
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

    public String getColOldName() {
        return colOldName;
    }

    public void setColOldName(String colOldName) {
        this.colOldName = colOldName;
    }

    public ColDataType getColDataType() {
        return colDataType;
    }

    public void setColDataType(ColDataType colDataType) {
        this.colDataType = colDataType;
    }

    public boolean isUnsignedFlag() {
        return unsignedFlag;
    }

    public void setUnsignedFlag(boolean unsignedFlag) {
        this.unsignedFlag = unsignedFlag;
    }
}
