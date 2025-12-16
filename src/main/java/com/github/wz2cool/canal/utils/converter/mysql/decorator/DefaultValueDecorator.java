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
        if (StringUtils.isNotBlank(alterColumnExpression.getDefaultValue())) {
            return SqlContext.from(sqlContext)
                    .defaultValue(DEFAULT + alterColumnExpression.getDefaultValue())
                    .build();
        }
        return sqlContext;
    }
}
