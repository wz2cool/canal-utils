package com.github.wz2cool.canal.utils.converter.factory;

import com.github.wz2cool.canal.utils.converter.AlterSqlConverterBase;
import com.github.wz2cool.canal.utils.converter.db2.Db2AlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.h2.H2AlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.hive.HiveAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.mssql.MssqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.oracle.OracleAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.postgresql.PostgresqlAlterSqlConverter;

public class AlterColumnSqlConverterFactory {
    public AlterSqlConverterBase create(String databaseType) {
        switch (databaseType) {
            case "oracle":
                return new OracleAlterSqlConverter();
            case "mssql":
                return new MssqlAlterSqlConverter();
            case "db2":
                return new Db2AlterSqlConverter();
            case "postgresql":
                return new PostgresqlAlterSqlConverter();
            case "hive":
                return new HiveAlterSqlConverter();
            case "h2":
                return new H2AlterSqlConverter();
            case "mysql":
                return new MysqlAlterSqlConverter();
            default:
                throw new RuntimeException(String.format("not support database type: '%s'", databaseType));
        }
    }
}
