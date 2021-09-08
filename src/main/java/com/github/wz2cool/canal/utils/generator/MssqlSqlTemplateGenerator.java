package com.github.wz2cool.canal.utils.generator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.mssql.MssqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.mssql.MssqlValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.DatabaseDriverType;

/**
 * mssql sql 模板生成器
 *
 * @author Frank
 */
public class MssqlSqlTemplateGenerator extends AbstractSqlTemplateGenerator {
    private final MssqlAlterSqlConverter mssqlAlterSqlConverter = new MssqlAlterSqlConverter();
    private final MssqlValuePlaceholderConverter mssqlValuePlaceholderConverter = new MssqlValuePlaceholderConverter();

    @Override
    protected IValuePlaceholderConverter getValuePlaceholderConverter() {
        return this.mssqlValuePlaceholderConverter;
    }

    @Override
    protected BaseAlterSqlConverter getAlterSqlConverter() {
        return this.mssqlAlterSqlConverter;
    }

    @Override
    public DatabaseDriverType getDatabaseDriverType() {
        return DatabaseDriverType.MSSQL;
    }
}
