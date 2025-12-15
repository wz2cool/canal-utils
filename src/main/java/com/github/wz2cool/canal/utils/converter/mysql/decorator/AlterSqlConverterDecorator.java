package com.github.wz2cool.canal.utils.converter.mysql.decorator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.model.SqlContext;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;

/**
 * alter 语句转换装饰器
 * @author YinPengHai
 */
public interface AlterSqlConverterDecorator {
    SqlContext apply(BaseAlterSqlConverter converter, AlterColumnExpression alterColumnExpression, SqlContext sqlContext);
}
