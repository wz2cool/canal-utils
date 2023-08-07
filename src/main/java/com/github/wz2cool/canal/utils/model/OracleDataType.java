package com.github.wz2cool.canal.utils.model;

public enum OracleDataType {
    CHAR("CHAR"),
    VARCHAR2("VARCHAR2"),
    NCHAR("NCHAR"),
    NVARCHAR2("NVARCHAR2"),
    CLOB("CLOB"),
    NCLOB("NCLOB"),
    LONG("LONG"),
    NUMBER("NUMBER"),
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    FLOAT("FLOAT"),
    BLOB("BLOB"),
    BFILE("BFILE"),
    RAW("RAW"),
    LONGRAW("LONG RAW"),
    ROWID("ROWID"),
    MLSLABEL("MLSLABEL");

    private String text;

    OracleDataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }
}
