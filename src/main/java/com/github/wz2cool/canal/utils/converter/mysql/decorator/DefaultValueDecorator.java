package com.github.wz2cool.canal.utils.converter.mysql.decorator;

import com.github.wz2cool.canal.utils.converter.mysql.MysqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.SqlContext;
import org.apache.commons.lang3.StringUtils;

public class DefaultValueDecorator implements AlterSqlConverterDecorator {
    private static final String DEFAULT = "DEFAULT ";

    @Override
    public SqlContext apply(MysqlAlterSqlConverter converter, AlterColumnExpression alterColumnExpression, SqlContext sqlContext) {
        SqlContext newSqlContext = new SqlContext(
                sqlContext.action,
                sqlContext.schemaName,
                sqlContext.tableName,
                sqlContext.oldColumnName,
                sqlContext.newColumnName,
                sqlContext.dataTypeString,
                sqlContext.nullAble,
                sqlContext.defaultValue,
                sqlContext.commentText
        );
        if (StringUtils.isNotBlank(alterColumnExpression.getDefaultValue())) {
            sqlContext.defaultValue = DEFAULT + alterColumnExpression.getDefaultValue();
        }
        return newSqlContext;
    }
}
