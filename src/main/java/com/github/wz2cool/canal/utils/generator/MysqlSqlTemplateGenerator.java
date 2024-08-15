package com.github.wz2cool.canal.utils.generator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlAlterSqlConverterDecorator;
import com.github.wz2cool.canal.utils.converter.mysql.MysqlValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.helper.DateHelper;
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
        this(false); // 默认使用false
    }

    /**
     * 带布尔参数的构造函数，根据布尔值选择使用哪种转换器
     *
     * @param useEnhancedConverter 如果为true，使用EnhancedMysqlAlterSqlConverter，否则使用默认的MysqlAlterSqlConverter
     */
    public MysqlSqlTemplateGenerator(boolean useEnhancedConverter) {
        this.alterSqlConverter = useEnhancedConverter ? new MysqlAlterSqlConverterDecorator(new MysqlAlterSqlConverter()) : new MysqlAlterSqlConverter();
        this.timestampConverter = (mysqlDataType, value) -> DateHelper.getDatetime(value);
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
