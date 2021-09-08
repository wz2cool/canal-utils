package com.github.wz2cool.canal.utils.generator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.converter.db2.Db2AlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.db2.Db2ValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.DatabaseDriverType;

/**
 * DB2 sql 模板生成器
 *
 * @author Frank
 */
public class Db2SqlTemplateGenerator extends AbstractSqlTemplateGenerator {

    private final Db2AlterSqlConverter db2AlterSqlConverter = new Db2AlterSqlConverter();
    private final Db2ValuePlaceholderConverter db2ValuePlaceholderConverter = new Db2ValuePlaceholderConverter();

    @Override
    protected IValuePlaceholderConverter getValuePlaceholderConverter() {
        return this.db2ValuePlaceholderConverter;
    }

    @Override
    protected BaseAlterSqlConverter getAlterSqlConverter() {
        return this.db2AlterSqlConverter;
    }

    @Override
    public DatabaseDriverType getDatabaseDriverType() {
        return DatabaseDriverType.DB2;
    }
}
