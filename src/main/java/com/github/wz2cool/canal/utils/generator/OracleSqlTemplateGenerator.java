package com.github.wz2cool.canal.utils.generator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.oracle.OracleAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.oracle.OracleValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.DatabaseDriverType;

/**
 * oracle sql 模板生成器
 *
 * @author Frank
 */
public class OracleSqlTemplateGenerator extends AbstractSqlTemplateGenerator {
    private final OracleAlterSqlConverter oracleAlterColumnSqlConverter = new OracleAlterSqlConverter();
    private final OracleValuePlaceholderConverter oracleValuePlaceholderConverter = new OracleValuePlaceholderConverter();

    @Override
    protected IValuePlaceholderConverter getValuePlaceholderConverter() {
        return this.oracleValuePlaceholderConverter;
    }

    @Override
    protected BaseAlterSqlConverter getAlterSqlConverter() {
        return this.oracleAlterColumnSqlConverter;
    }

    @Override
    public DatabaseDriverType getDatabaseDriverType() {
        return DatabaseDriverType.ORACLE;
    }

    @Override
    protected String getUseColumnName(String columnName) {
        // 防止oracle  关键字
        return String.format("\"%s\"", columnName.toUpperCase());
    }
}
