package com.github.wz2cool.canal.utils.model;

// https://www.ibm.com/support/knowledgecenter/en/SSVRGU_10.0.0/basic/H_902759DB2_DATA_TYPES.html
public enum Db2DataType {
    BIGINT("BIGINT"),
    SMALLINT("SMALLINT"),
    INTEGER("INTEGER"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    REAL("REAL"),
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    CLOB("CLOB"),
    DATE("DATE"),
    TIME("TIME"),
    TIMESTAMP("TIMESTAMP"),
    BLOB("BLOB");


    private String text;

    Db2DataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }
}
