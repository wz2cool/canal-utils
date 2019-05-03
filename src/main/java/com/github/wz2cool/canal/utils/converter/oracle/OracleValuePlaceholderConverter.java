package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;

import java.nio.charset.StandardCharsets;

public class OracleValuePlaceholderConverter implements IValuePlaceholderConverter {
    @Override
    public ValuePlaceholder convert(final MysqlDataType mysqlDataType, final String value) {
        ValuePlaceholder result = new ValuePlaceholder();
        switch (mysqlDataType) {
            case DATE:
                result.setPlaceholder("to_date(?, 'yyyy-mm-dd')");
                result.setValue(value);
                break;
            case DATETIME:
            case TIMESTAMP:
                result.setPlaceholder("to_date(?, 'yyyy-mm-dd hh24:mi:ss')");
                result.setValue(value);
                break;
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
