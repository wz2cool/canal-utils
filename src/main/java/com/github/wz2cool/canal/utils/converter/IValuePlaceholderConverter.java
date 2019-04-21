package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.MysqlDataType;

public interface IValuePlaceholderConverter {
    String convert(final MysqlDataType mysqlDataType);
}
