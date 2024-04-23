package com.github.wz2cool.canal.utils.model;

public enum PostgresqlDataType {
    CHAR("CHAR"),
    CHARACTER("CHARACTER"),
    VARCHAR("VARCHAR"),
    TEXT("TEXT"),
    BIT("BIT"),
    SMALLINT("SMALLINT"),
    INT("INT"),
    INTEGER("INTEGER"),
    BIGINT("BIGINT"),
    NUMERIC("NUMERIC"),
    REAL("REAL"),
    MONEY("MONEY"),
    BOOL("BOOL"),
    BOOLEAN("BOOLEAN"),
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    TIME("TIME"),
    BYTEA("BYTEA"),
    JSON("JSON");

    private String text;

    PostgresqlDataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }
}
