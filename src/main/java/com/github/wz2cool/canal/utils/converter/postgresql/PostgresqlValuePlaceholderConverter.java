package com.github.wz2cool.canal.utils.converter.postgresql;

import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.PostgresqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

public class PostgresqlValuePlaceholderConverter implements IValuePlaceholderConverter {
    @Override
    public ValuePlaceholder convert(MysqlDataType mysqlDataType, String value) {
        ValuePlaceholder result = new ValuePlaceholder();
        String commonPlaceholder = "?::%s";
        MysqlDataType useMysqlDataType = getMysqlDataType(mysqlDataType, value);
        switch (useMysqlDataType) {
            case BIT:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.BIT.getText()));
                result.setValue(value);
                break;
            case TINYINT:
            case SMALLINT:
                // unsigned smallint need convert to int
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.SMALLINT.getText()));
                result.setValue(value);
                break;
            case MEDIUMINT:
            case INT:
            case INTEGER:
                // unsigned int need convert to bigint
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
                break;
            case JSON:
                result.setPlaceholder(String.format(commonPlaceholder, PostgresqlDataType.JSON.getText()));
                result.setValue(value);
                break;
        }
        return result;
    }

    private MysqlDataType getMysqlDataType(MysqlDataType mysqlDataType, String value) {
        if (StringUtils.isNotBlank(value)) {
            if (mysqlDataType == MysqlDataType.SMALLINT) {
                // 性能优化：先判断字符串长度，如果长度小于5，肯定在smallint范围内，直接返回SMALLINT
                if (value.length() < 5) {
                    return MysqlDataType.SMALLINT;
                }

                // 对于长度>=5的字符串，需要进一步检查是否超出smallint范围
                long longValue = Long.parseLong(value);
                // 如果值大于 smallint 最大值 32767 或小于 smallint 最小值 -32768，则返回 INT
                if (longValue > Short.MAX_VALUE || longValue < Short.MIN_VALUE) {
                    return MysqlDataType.INT;
                }
            } else if (mysqlDataType == MysqlDataType.INT) {
                // 性能优化：先判断字符串长度，如果长度小于10，肯定在int范围内，直接返回INT
                if (value.length() < 10) {
                    return MysqlDataType.INT;
                }

                // 对于长度>=10的字符串，需要进一步检查是否超出int范围
                long longValue = Long.parseLong(value);
                // 如果值大于 int 最大值 2147483647 或小于 int 最小值 -2147483648，则返回 BIGINT
                if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
                    return MysqlDataType.BIGINT;
                }
            }
        }
        return mysqlDataType;
    }
}