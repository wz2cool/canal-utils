package com.github.wz2cool.canal.utils.converter.postgresql;

import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.PostgresqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;

import java.nio.charset.StandardCharsets;

public class PostgresqlValuePlaceholderConverter implements IValuePlaceholderConverter {
    @Override
    public ValuePlaceholder convert(MysqlDataType mysqlDataType, String value) {
        ValuePlaceholder result = new ValuePlaceholder();
        String commonPlaceholder = "?::%s";
        switch (mysqlDataType) {
            case BIT:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.BIT.getText()));
                result.setValue(value);
                break;
            case TINYINT:
            case SMALLINT:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.SMALLINT.getText()));
                result.setValue(value);
                break;
            case MEDIUMINT:
            case INT:
            case INTEGER:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.INT.getText()));
                result.setValue(value);
                break;
            case BIGINT:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.BIGINT.getText()));
                result.setValue(value);
                break;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.NUMERIC.getText()));
                result.setValue(value);
                break;
            case DATE:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.DATE.getText()));
                result.setValue(value);
                break;
            case DATETIME:
            case TIMESTAMP:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.TIMESTAMP.getText()));
                result.setValue(value);
                break;
            case TIME:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.TIME.getText()));
                result.setValue(value);
                break;
            case CHAR:
            case VARCHAR:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.VARCHAR.getText()));
                result.setValue(value);
                break;
            case MEDIUMBLOB:
            case TINYBLOB:
            case BLOB:
            case LONGBLOB:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.BYTEA.getText()));
                result.setValue(value == null ? null : value.getBytes(StandardCharsets.ISO_8859_1));
                break;
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                result.setPlaceholder(String.format(commonPlaceholder, MysqlDataType.TEXT.getText()));
                result.setValue(value);
            case JSON:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.JSON.getText()));
                result.setValue(value);
                break;
        }
        return result;
    }
}
