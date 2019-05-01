package com.github.wz2cool.canal.utils.model;

public enum SqlserverDataType {
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    TEXT("TEXT"),
    NCHAR("NCHAR"),
    NVARCHAR("NVARCHAR"),
    NTEXT("NTEXT"),
    BIT("BIT"),
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    IMAGE("IMAGE"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    INT("INT"),
    BIGINT("BIGINT"),
    DECIMAL("DECIMAL"),
    NUMERIC("NUMERIC"),
    SMALLMONEY("SMALLMONEY"),
    MONEY("MONEY"),
    FLOAT("FLOAT"),
    REAL("REAL"),
    DATETIME("DATETIME"),
    DATETIME2("DATETIME2"),
    SMALLDATETIME("SMALLDATETIME"),
    DATE("DATE"),
    TIME("TIME"),
    DATETIMEOFFSET("DATETIMEOFFSET"),
    TIMESTAMP("TIMESTAMP");

    private String text;

    SqlserverDataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }
}
