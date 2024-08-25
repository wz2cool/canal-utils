package com.github.wz2cool.canal.utils.generator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.DatabaseDriverType;

/**
 * mysql sql模板生成器
 *
 * @author Frank
 */
public class MysqlSqlTemplateGenerator extends AbstractSqlTemplateGenerator {
    private final BaseAlterSqlConverter alterSqlConverter;
    private final MysqlValuePlaceholderConverter mysqlValuePlaceholderConverter = new MysqlValuePlaceholderConverter();

    /**
     * 默认构造函数，使用默认的MysqlAlterSqlConverter
     */
    public MysqlSqlTemplateGenerator() {
        alterSqlConverter = new MysqlAlterSqlConverter();
    }

    /**
     * 构造函数，使用自定义的MysqlAlterSqlConverter
     * @param alterSqlConverter
     */
    public MysqlSqlTemplateGenerator(BaseAlterSqlConverter alterSqlConverter) {
        this.alterSqlConverter = alterSqlConverter;
    }

    @Override
    protected BaseAlterSqlConverter getAlterSqlConverter() {
        return this.alterSqlConverter;
    }


    @Override
    public DatabaseDriverType getDatabaseDriverType() {
        return DatabaseDriverType.MYSQL;
    }

    @Override
    protected IValuePlaceholderConverter getValuePlaceholderConverter() {
        return this.mysqlValuePlaceholderConverter;
    }

    @Override
    protected String getUseColumnName(String columnName) {
        return String.format("`%s`", columnName);
    }
}
