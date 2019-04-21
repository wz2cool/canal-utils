package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;

public class OracleValuePlaceholderConverter implements IValuePlaceholderConverter {
    @Override
    public String convert(final MysqlDataType mysqlDataType) {
        switch (mysqlDataType) {
            case DATE:
                return "to_date(?, 'yyyy-mm-dd')";
            case DATETIME:
            case TIMESTAMP:
                return "to_date(?, 'yyyy-mm-dd hh24:mi:ss')";
            case TIME:
                return "to_date(?, 'hh24:mi:ss')";
            default:
                return "?";
        }
    }
}
