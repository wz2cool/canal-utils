package com.github.wz2cool.canal.utils.converter.h2;

import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.H2DataType;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class H2ColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
        if (mysqlColDataType == null) {
            return Optional.empty();
        }

        Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(mysqlColDataType.getDataType());
        if (!mysqlDataTypeOptional.isPresent()) {
            return Optional.empty();
        }

        ColDataType colDataType = getH2ColDataType(
                mysqlDataTypeOptional.get(), mysqlColDataType.getArgumentsStringList());
        return Optional.ofNullable(colDataType);
    }

    private ColDataType getH2ColDataType(final MysqlDataType mysqlDataType, final List<String> argumentsStringList) {
        ColDataType result = new ColDataType();
        List<String> useArgumentsStringList = argumentsStringList == null ? new ArrayList<>() : argumentsStringList;
        List<String> argStrings = new ArrayList<>();

        switch (mysqlDataType) {
            case BIT:
            case TINYINT:
                result.setDataType(H2DataType.TINYINT.getText());
                break;
            case SMALLINT:
                result.setDataType(H2DataType.SMALLINT.getText());
                break;
            case MEDIUMINT:
            case INT:
            case INTEGER:
                result.setDataType(H2DataType.INT.getText());
                break;
            case BIGINT:
                result.setDataType(H2DataType.BIGINT.getText());
                break;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                result.setDataType(H2DataType.DECIMAL.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case DATE:
                result.setDataType(H2DataType.DATE.getText());
                break;
            case DATETIME:
            case TIMESTAMP:
                result.setDataType(H2DataType.TIMESTAMP.getText());
                break;
            case TIME:
                result.setDataType(H2DataType.TIME.getText());
                break;
            case CHAR:
            case VARCHAR:
                result.setDataType(H2DataType.VARCHAR.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                result.setDataType(H2DataType.BLOB.getText());
                break;
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                result.setDataType(H2DataType.CLOB.getText());
                break;
        }
        result.setArgumentsStringList(argStrings);
        return result;
    }
}
