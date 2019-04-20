package com.github.wz2cool.canal.utils.model;

import java.util.Optional;

public enum MysqlDataType {
    BIT("BIT"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    MEDIUMINT("MEDIUMINT"),
    INT("INT"),
    INTEGER("INTEGER"),
    BIGINT("BIGINT"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    DATE("DATE"),
    DATETIME("DATETIME"),
    TIMESTAMP("TIMESTAMP"),
    TIME("TIME"),
    /**
     * not support year type.
     */
    YEAR("YEAR"),
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    /**
     * not support NUMERIC type.
     */
    NUMERIC("NUMERIC"),
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    TINYBLOB("TINYBLOB"),
    TINYTEXT("TINYTEXT"),
    BLOB("BLOB"),
    TEXT("TEXT"),
    MEDIUMBLOB("MEDIUMBLOB"),
    MEDIUMTEXT("MEDIUMTEXT"),
    LONGBLOB("LONGBLOB"),
    LONGTEXT("LONGTEXT"),
    ENUM("ENUM"),
    REAL("REAL"),
    SET("SET");

    private String text;

    MysqlDataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static Optional<MysqlDataType> getDataType(String typeString) {
        try {
            MysqlDataType mysqlDataType = MysqlDataType.valueOf(typeString.toUpperCase());
            return Optional.of(mysqlDataType);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}