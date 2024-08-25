package com.github.wz2cool.canal.utils.converter.mysql.decorator;

import com.github.wz2cool.canal.utils.converter.mysql.MysqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.SqlContext;

/**
 * 替换表名和schema
 * @author penghai
 */
public class ReplaceNameDecorator implements AlterSqlConverterDecorator {
    private final String targetSchemaName;
    private final String targetTableName;

    public ReplaceNameDecorator(String targetSchemaName, String targetTableName) {
        this.targetSchemaName = targetSchemaName;
        this.targetTableName = targetTableName;
    }

    @Override
    public SqlContext apply(MysqlAlterSqlConverter converter, AlterColumnExpression alterColumnExpression, SqlContext sqlContext) {
        return new SqlContext(
                sqlContext.action,
                targetSchemaName,
                targetTableName,
                sqlContext.oldColumnName,
                sqlContext.newColumnName,
                sqlContext.dataTypeString,
                sqlContext.nullAble,
                sqlContext.defaultValue,
                sqlContext.commentText
        );
    }
}