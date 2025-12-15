package com.github.wz2cool.canal.utils.model;

public class SqlContext {
    private final String schemaName;
    private final String tableName;
    private final String defaultValue;
    private final String action;
    private final String oldColumnName;
    private final String newColumnName;
    private final String dataTypeString;
    private final String nullAble;
    private final String commentText;
    private final Boolean unsignedFlag;

    private SqlContext(Builder builder) {
        this.action = builder.action;
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.oldColumnName = builder.oldColumnName;
        this.newColumnName = builder.newColumnName;
        this.dataTypeString = builder.dataTypeString;
        this.nullAble = builder.nullAble;
        this.defaultValue = builder.defaultValue;
        this.commentText = builder.commentText;
        this.unsignedFlag = builder.unsignedFlag;
    }

    // Getter 方法
    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
    public String getDefaultValue() { return defaultValue; }
    public String getAction() { return action; }
    public String getOldColumnName() { return oldColumnName; }
    public String getNewColumnName() { return newColumnName; }
    public String getDataTypeString() { return dataTypeString; }
    public String getNullAble() { return nullAble; }
    public String getCommentText() { return commentText; }
    public Boolean getUnsignedFlag() { return unsignedFlag; }

    // Builder 内部类
    public static class Builder {
        private String schemaName;
        private String tableName;
        private String defaultValue;
        private String action;
        private String oldColumnName;
        private String newColumnName;
        private String dataTypeString;
        private String nullAble;
        private String commentText;
        private Boolean unsignedFlag;

        public Builder schemaName(String schemaName) { this.schemaName = schemaName; return this; }
        public Builder tableName(String tableName) { this.tableName = tableName; return this; }
        public Builder defaultValue(String defaultValue) { this.defaultValue = defaultValue; return this; }
        public Builder action(String action) { this.action = action; return this; }
        public Builder oldColumnName(String oldColumnName) { this.oldColumnName = oldColumnName; return this; }
        public Builder newColumnName(String newColumnName) { this.newColumnName = newColumnName; return this; }
        public Builder dataTypeString(String dataTypeString) { this.dataTypeString = dataTypeString; return this; }
        public Builder nullAble(String nullAble) { this.nullAble = nullAble; return this; }
        public Builder commentText(String commentText) { this.commentText = commentText; return this; }
        public Builder unsignedFlag(Boolean unsignedFlag) { this.unsignedFlag = unsignedFlag; return this; }

        public SqlContext build() {
            return new SqlContext(this);
        }
    }

    // 从已有对象生成 Builder
    public static Builder from(SqlContext other) {
        return new Builder()
                .action(other.action)
                .schemaName(other.schemaName)
                .tableName(other.tableName)
                .oldColumnName(other.oldColumnName)
                .newColumnName(other.newColumnName)
                .dataTypeString(other.dataTypeString)
                .nullAble(other.nullAble)
                .defaultValue(other.defaultValue)
                .commentText(other.commentText)
                .unsignedFlag(other.unsignedFlag);
    }
}
