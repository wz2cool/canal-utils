package com.github.wz2cool.canal.utils.converter.factory;

import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.db2.Db2ValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.h2.H2ValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.hive.HiveValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.mssql.MssqlValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.oracle.OracleValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.postgresql.PostgresqlValuePlaceholderConverter;

public class ValuePlaceholderConverterFactory {
    public IValuePlaceholderConverter create(String databaseType) {
        switch (databaseType) {
            case "oracle":
                return new OracleValuePlaceholderConverter();
            case "mssql":
                return new MssqlValuePlaceholderConverter();
            case "db2":
                return new Db2ValuePlaceholderConverter();
            case "postgresql":
                return new PostgresqlValuePlaceholderConverter();
            case "hive":
                return new HiveValuePlaceholderConverter();
            case "h2":
                return new H2ValuePlaceholderConverter();
            case "mysql":
                return new MysqlValuePlaceholderConverter();
            default:
                throw new RuntimeException(String.format("not support database type: '%s'", databaseType));
        }
    }
}
