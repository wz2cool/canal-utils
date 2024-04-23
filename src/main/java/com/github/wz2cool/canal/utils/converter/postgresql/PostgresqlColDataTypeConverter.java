package com.github.wz2cool.canal.utils.converter.postgresql;

import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.PostgresqlDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PostgresqlColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
        if (mysqlColDataType == null) {
            return Optional.empty();
        }

        Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(mysqlColDataType.getDataType());
        if (!mysqlDataTypeOptional.isPresent()) {
            return Optional.empty();
        }

        ColDataType colDataType = getPostgresqlColDataType(
                mysqlDataTypeOptional.get(), mysqlColDataType.getArgumentsStringList());
        return Optional.ofNullable(colDataType);
    }

    private ColDataType getPostgresqlColDataType(final MysqlDataType mysqlDataType, final List<String> argumentsStringList) {
        ColDataType result = new ColDataType();
        List<String> useArgumentsStringList = argumentsStringList == null ? new ArrayList<>() : argumentsStringList;
        List<String> argStrings = new ArrayList<>();

        switch (mysqlDataType) {
            case BIT:
                result.setDataType(PostgresqlDataType.BIT.getText());
                break;
            case TINYINT:
            case SMALLINT:
                result.setDataType(PostgresqlDataType.SMALLINT.getText());
                break;
            case MEDIUMINT:
            case INT:
            case INTEGER:
                result.setDataType(PostgresqlDataType.INT.getText());
                break;
            case BIGINT:
                result.setDataType(PostgresqlDataType.BIGINT.getText());
                break;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                result.setDataType(PostgresqlDataType.NUMERIC.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case DATE:
                result.setDataType(PostgresqlDataType.DATE.getText());
                break;
            case DATETIME:
            case TIMESTAMP:
                result.setDataType(PostgresqlDataType.TIMESTAMP.getText());
                break;
            case TIME:
                result.setDataType(PostgresqlDataType.TIME.getText());
                break;
            case CHAR:
            case VARCHAR:
                result.setDataType(PostgresqlDataType.VARCHAR.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case MEDIUMBLOB:
            case TINYBLOB:
            case BLOB:
            case LONGBLOB:
                result.setDataType(PostgresqlDataType.BYTEA.getText());
                break;
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                result.setDataType(PostgresqlDataType.TEXT.getText());
            case JSON:
                result.setDataType(PostgresqlDataType.JSON.getText());
                break;
        }
        result.setArgumentsStringList(argStrings);
        return result;
    }
}
