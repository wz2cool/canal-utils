package com.github.wz2cool.canal.utils.model;

/**
 * sql context
 * @author YinPengHai
 **/
public class SqlContext {
    public String schemaName;
    public String tableName;
    public String defaultValue;
    public String action;
    public String oldColumnName;
    public String newColumnName;
    public String dataTypeString;
    public String nullAble;
    public String commentText;

    public SqlContext(String action, String schemaName, String tableName, String oldColumnName, String newColumnName,
                      String dataTypeString, String nullAble, String defaultValue, String commentText) {
        this.action = action;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.oldColumnName = oldColumnName;
        this.newColumnName = newColumnName;
        this.dataTypeString = dataTypeString;
        this.nullAble = nullAble;
        this.defaultValue = defaultValue;
        this.commentText = commentText;
    }
}
