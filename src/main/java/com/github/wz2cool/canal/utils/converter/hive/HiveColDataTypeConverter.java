package com.github.wz2cool.canal.utils.converter.hive;

import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.HiveDataType;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HiveColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
        if (mysqlColDataType == null) {
            return Optional.empty();
        }

        Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(mysqlColDataType.getDataType());
        if (!mysqlDataTypeOptional.isPresent()) {
            return Optional.empty();
        }

        ColDataType colDataType = getHiveColDataType(
                mysqlDataTypeOptional.get(), mysqlColDataType.getArgumentsStringList());
        return Optional.ofNullable(colDataType);
    }

    // https://blog.csdn.net/liangzelei/article/details/80102909
    private ColDataType getHiveColDataType(final MysqlDataType mysqlDataType, final List<String> argumentsStringList) {
        ColDataType result = new ColDataType();
        List<String> useArgumentsStringList = argumentsStringList == null ? new ArrayList<>() : argumentsStringList;
        List<String> argStrings = new ArrayList<>();
        switch (mysqlDataType) {
            case BIT:
            case TINYINT:
                result.setDataType(HiveDataType.TINYINT.getText());
                break;
            case SMALLINT:
                result.setDataType(HiveDataType.SMALLINT.getText());
                break;
            case MEDIUMINT:
            case INT:
            case INTEGER:
                result.setDataType(HiveDataType.INT.getText());
                break;
            case BIGINT:
                result.setDataType(HiveDataType.BIGINT.getText());
                break;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                result.setDataType(HiveDataType.DECIMAL.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case DATE:
                result.setDataType(HiveDataType.DATE.getText());
                break;
            case DATETIME:
            case TIMESTAMP:
                result.setDataType(HiveDataType.TIMESTAMP.getText());
                break;
            case TIME:
                result.setDataType(HiveDataType.VARCHAR.getText());
                argStrings.add("50");
                break;
            case CHAR:
            case VARCHAR:
                result.setDataType(HiveDataType.VARCHAR.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                result.setDataType(HiveDataType.BINARY.getText());
                break;
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                result.setDataType(HiveDataType.STRING.getText());
                break;
        }
        result.setArgumentsStringList(argStrings);
        return result;
    }
}
