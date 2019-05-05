package com.github.wz2cool.canal.utils.model;

public enum H2DataType {
    INT("INT"),
    BOOLEAN("BOOLEAN"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    BIGINT("BIGINT"),
    IDENTITY("IDENTITY"),
    DECIMAL("DECIMAL"),
    DOUBLE("DOUBLE"),
    REAL("REAL"),
    TIME("TIME"),
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    BINARY("BINARY"),
    VARCHAR("VARCHAR"),
    CHAR("CHAR"),
    BLOB("BLOB"),
    CLOB("CLOB");

    private String text;

    H2DataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }
}
