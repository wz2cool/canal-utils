package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;
import org.apache.commons.lang3.StringUtils;

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
            case TIME:
                result.setPlaceholder("to_date(?, 'hh24:mi:ss')");
                result.setValue(value);
                break;
            case TINYBLOB:
                result.setPlaceholder("hextoraw(?)");
                result.setValue(StringUtils.stripStart(value, "0x"));
                break;
            default:
                result.setPlaceholder("?");
                result.setValue(value);
                break;
        }
        return result;
    }
}
