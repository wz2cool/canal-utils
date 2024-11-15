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
    private final BaseAlterSqlConverter alterSqlConverter;
    private final PostgresqlValuePlaceholderConverter postgresqlValuePlaceholderConverter = new PostgresqlValuePlaceholderConverter();

    /**
     * 默认构造函数，使用默认的MysqlAlterSqlConverter
     */
    public PostgresqlSqlTemplateGenerator() {
        alterSqlConverter = new PostgresqlAlterSqlConverter();
    }

    /**
     * 构造函数，使用自定义的MysqlAlterSqlConverter
     * @param alterSqlConverter
     */
    public PostgresqlSqlTemplateGenerator(BaseAlterSqlConverter alterSqlConverter) {
        this.alterSqlConverter = alterSqlConverter;
    }

    @Override
    protected IValuePlaceholderConverter getValuePlaceholderConverter() {
        return this.postgresqlValuePlaceholderConverter;
    }

    @Override
    protected BaseAlterSqlConverter getAlterSqlConverter() {
        return this.alterSqlConverter;
    }

    @Override
    public DatabaseDriverType getDatabaseDriverType() {
        return DatabaseDriverType.POSTGRESQL;
    }
}
