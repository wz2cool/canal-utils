package com.github.wz2cool.canal.utils.converter.mysql.decorator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.SqlContext;
import org.apache.commons.lang3.StringUtils;
/**
 * 默认值装饰器
 * @author YinPengHai
 **/
public class DefaultValueDecorator implements AlterSqlConverterDecorator {
    private static final String DEFAULT = "DEFAULT ";

    @Override
    public SqlContext apply(BaseAlterSqlConverter converter, AlterColumnExpression alterColumnExpression, SqlContext sqlContext) {
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
            newSqlContext.defaultValue = DEFAULT + alterColumnExpression.getDefaultValue();
        }
        return newSqlContext;
    }
}
