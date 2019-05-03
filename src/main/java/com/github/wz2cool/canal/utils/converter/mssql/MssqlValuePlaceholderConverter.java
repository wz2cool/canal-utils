package com.github.wz2cool.canal.utils.converter.mssql;

import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;

public class MssqlValuePlaceholderConverter implements IValuePlaceholderConverter {
    @Override
    public ValuePlaceholder convert(MysqlDataType mysqlDataType, String value) {
        ValuePlaceholder valuePlaceholder = new ValuePlaceholder();
        if (mysqlDataType == MysqlDataType.BIT) {
            valuePlaceholder.setPlaceholder("?");
            valuePlaceholder.setValue(value == null ? null : Integer.parseInt(value));
        } else {
            valuePlaceholder.setPlaceholder("?");
            valuePlaceholder.setValue(value);
        }
        return valuePlaceholder;
    }
}
