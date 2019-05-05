package com.github.wz2cool.canal.utils.model;

public enum HiveDataType {
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    INT("INT"),
    BIGINT("BIGINT"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    TIMESTAMP("TIMESTAMP"),
    DATE("DATE"),
    STRING("STRING"),
    VARCHAR("VARCHAR"),
    CHAR("CHAR"),
    BOOLEAN("BOOLEAN"),
    BINARY("BINARY");

    private String text;

    HiveDataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }
}
