package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.OracleDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OracleColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
        if (mysqlColDataType == null) {
            return Optional.empty();
        }

        Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(mysqlColDataType.getDataType());
        if (!mysqlDataTypeOptional.isPresent()) {
            return Optional.empty();
        }

        ColDataType colDataType = getOracleDataType(
                mysqlDataTypeOptional.get(), mysqlColDataType.getArgumentsStringList());
        return Optional.ofNullable(colDataType);
    }

    // https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABGACIF
    private ColDataType getOracleDataType(final MysqlDataType mysqlDataType, final List<String> argumentsStringList) {
        ColDataType result = new ColDataType();
        List<String> useArgumentsStringList = argumentsStringList == null ? new ArrayList<>() : argumentsStringList;
        List<String> argStrings = new ArrayList<>();

        switch (mysqlDataType) {
            case BIGINT:
                result.setDataType(OracleDataType.NUMBER.getText());
                argStrings.add("20");
                argStrings.add("0");
                break;
            case BIT:
                result.setDataType(OracleDataType.NUMBER.getText());
                argStrings.add("1");
                argStrings.add("0");
                break;
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
                result.setDataType(OracleDataType.BLOB.getText());
                break;
            case CHAR:
                result.setDataType(OracleDataType.NCHAR.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case DATE:
            case DATETIME:
            case TIME:
            case TIMESTAMP:
                result.setDataType(OracleDataType.DATE.getText());
                break;
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
                result.setDataType(OracleDataType.FLOAT.getText());
                argStrings.add("24");
                break;
            case INT:
            case INTEGER:
                result.setDataType(OracleDataType.NUMBER.getText());
                argStrings.add("11");
                argStrings.add("0");
                break;
            case TINYINT:
                result.setDataType(OracleDataType.NUMBER.getText());
                argStrings.add("4");
                argStrings.add("0");
                break;
            case SMALLINT:
                result.setDataType(OracleDataType.NUMBER.getText());
                argStrings.add("6");
                argStrings.add("0");
                break;
            case MEDIUMINT:
                result.setDataType(OracleDataType.NUMBER.getText());
                argStrings.add("8");
                argStrings.add("0");
                break;
            case TEXT:
            case LONGTEXT:
            case TINYTEXT:
            case MEDIUMTEXT:
                result.setDataType(OracleDataType.NCLOB.getText());
                break;
            case VARCHAR:
                result.setDataType(OracleDataType.NVARCHAR2.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
        }

        result.setArgumentsStringList(argStrings);
        return result;
    }


}
