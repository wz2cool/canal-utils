package com.github.wz2cool.canal.utils.generator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.TimestampConverter;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.helper.DateHelper;
import com.github.wz2cool.canal.utils.model.DatabaseDriverType;

/**
 * mysql sql模板生成器
 *
 * @author Frank
 */
public class MysqlSqlTemplateGenerator extends AbstractSqlTemplateGenerator {
    private final MysqlAlterSqlConverter mysqlAlterSqlConverter = new MysqlAlterSqlConverter();
    private final MysqlValuePlaceholderConverter mysqlValuePlaceholderConverter = new MysqlValuePlaceholderConverter();

    /**
     * 构造函数
     */
    public MysqlSqlTemplateGenerator() {
        TimestampConverter mysqlTimestampConverter = (mysqlDataType, value) -> DateHelper.getDatetime(value);
        this.timestampConverter = mysqlTimestampConverter;
    }

    @Override
    protected BaseAlterSqlConverter getAlterSqlConverter() {
        return this.mysqlAlterSqlConverter;
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
