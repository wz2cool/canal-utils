package com.github.wz2cool.canal.utils.converter.factory;

import com.github.wz2cool.canal.utils.converter.AlterColumnSqlConverterBase;
import com.github.wz2cool.canal.utils.converter.db2.Db2AlterColumnSqlConverter;
import com.github.wz2cool.canal.utils.converter.h2.H2AlterColumnSqlConverter;
import com.github.wz2cool.canal.utils.converter.hive.HiveAlterColumnSqlConverter;
import com.github.wz2cool.canal.utils.converter.mssql.MssqlAlterColumnSqlConverter;
import com.github.wz2cool.canal.utils.converter.oracle.OracleAlterColumnSqlConverter;
import com.github.wz2cool.canal.utils.converter.postgresql.PostgresqlAlterColumnSqlConverter;

public class AlterColumnSqlConverterFactory {
    public AlterColumnSqlConverterBase create(String databaseType) {
        switch (databaseType) {
            case "oracle":
                return new OracleAlterColumnSqlConverter();
            case "mssql":
                return new MssqlAlterColumnSqlConverter();
            case "db2":
                return new Db2AlterColumnSqlConverter();
            case "postgresql":
                return new PostgresqlAlterColumnSqlConverter();
            case "hive":
                return new HiveAlterColumnSqlConverter();
            case "h2":
                return new H2AlterColumnSqlConverter();
            default:
                throw new RuntimeException(String.format("not support database type: '%s'", databaseType));
        }
    }
}
