package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;

public interface IValuePlaceholderConverter {
    ValuePlaceholder convert(final MysqlDataType mysqlDataType, final String value);
}
