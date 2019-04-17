package com.github.wz2cool.canal.utils.model;

import java.util.Optional;

public enum MysqlDataType {
    BIT,
    TINYINT,
    SMALLINT,
    MEDIUMINT,
    INT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    DATETIME,
    TIMESTAMP,
    TIME,
    YEAR,
    CHAR,
    VARCHAR,
    NUMERIC,
    BINARY,
    VARBINARY,
    TINYBLOB,
    TINYTEXT,
    BLOB,
    TEXT,
    MEDIUMBLOB,
    MEDIUMTEXT,
    LONGBLOB,
    LONGTEXT,
    ENUM,
    REAL,
    SET;

    public static Optional<MysqlDataType> getDataType(String typeString) {
        try {
            MysqlDataType mysqlDataType = MysqlDataType.valueOf(typeString.toUpperCase());
            return Optional.of(mysqlDataType);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}