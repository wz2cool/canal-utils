package com.github.wz2cool.canal.utils.converter.mysql.decorator;

import com.github.wz2cool.canal.utils.converter.mysql.MysqlAlterSqlConverter;
import com.github.wz2cool.canal.utils.model.SqlContext;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;

/**
 * alter sql converter decorator
 * @author penghai
 */
public interface AlterSqlConverterDecorator {
    SqlContext apply(MysqlAlterSqlConverter converter, AlterColumnExpression alterColumnExpression, SqlContext sqlContext);
}
