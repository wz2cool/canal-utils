package com.github.wz2cool.canal.utils.generator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.postgresql.PostgresqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.postgresql.PostgresqlValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.DatabaseDriverType;

/**
 * Postgresql sql 模板生成器
 *
 * @author Frank
 */
public class PostgresqlSqlTemplateGenerator extends AbstractSqlTemplateGenerator {
    private final PostgresqlAlterSqlConverter postgresqlAlterSqlConverter = new PostgresqlAlterSqlConverter();
    private final PostgresqlValuePlaceholderConverter postgresqlValuePlaceholderConverter = new PostgresqlValuePlaceholderConverter();

    @Override
    protected IValuePlaceholderConverter getValuePlaceholderConverter() {
        return this.postgresqlValuePlaceholderConverter;
    }

    @Override
    protected BaseAlterSqlConverter getAlterSqlConverter() {
        return this.postgresqlAlterSqlConverter;
    }

    @Override
    public DatabaseDriverType getDatabaseDriverType() {
        return DatabaseDriverType.POSTGRESQL;
    }
}
