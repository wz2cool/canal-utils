package com.github.wz2cool.canal.utils.converter.h2;

import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;

import java.nio.charset.StandardCharsets;

public class H2ValuePlaceholderConverter implements IValuePlaceholderConverter {
    @Override
    public ValuePlaceholder convert(MysqlDataType mysqlDataType, String value) {
        ValuePlaceholder result = new ValuePlaceholder();
        switch (mysqlDataType) {
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                result.setPlaceholder("?");
                result.setValue(value == null ? null : value.getBytes(StandardCharsets.ISO_8859_1));
                break;
            default:
                result.setPlaceholder("?");
                result.setValue(value);
                break;
        }
        return result;
    }
}
