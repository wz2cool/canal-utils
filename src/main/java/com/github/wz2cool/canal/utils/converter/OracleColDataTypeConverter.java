package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.OracleDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OracleColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
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
        List<String> useArgumentsStringList = new ArrayList<>();
        switch (mysqlDataType) {
            case BIGINT:
                result.setDataType(OracleDataType.NUMBER.getText());
                useArgumentsStringList.add("19");
                useArgumentsStringList.add("0");
                break;
            case BIT:
                result.setDataType(OracleDataType.RAW.getText());
                break;
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
                result.setDataType(OracleDataType.BLOB.getText());
                break;
            case CHAR:
                result.setDataType(OracleDataType.CHAR.getText());
                useArgumentsStringList.addAll(argumentsStringList);
                break;
            case DATE:
            case DATETIME:
            case TIME:
            case TIMESTAMP:
                result.setDataType(OracleDataType.CHAR.getText());
                break;
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case REAL:
                result.setDataType(OracleDataType.FLOAT.getText());
                useArgumentsStringList.add("24");
                break;
            case INT:
            case INTEGER:
                result.setDataType(OracleDataType.NUMBER.getText());
                useArgumentsStringList.add("10");
                useArgumentsStringList.add("0");
                break;
            case TINYINT:
                result.setDataType(OracleDataType.NUMBER.getText());
                useArgumentsStringList.add("3");
                useArgumentsStringList.add("0");
                break;
            case SMALLINT:
                result.setDataType(OracleDataType.NUMBER.getText());
                useArgumentsStringList.add("5");
                useArgumentsStringList.add("0");
                break;
            case MEDIUMINT:
                result.setDataType(OracleDataType.NUMBER.getText());
                useArgumentsStringList.add("7");
                useArgumentsStringList.add("0");
                break;
            case YEAR:
            case NUMERIC:
                result.setDataType(OracleDataType.NUMBER.getText());
                break;
            case TEXT:
            case LONGTEXT:
            case TINYTEXT:
            case MEDIUMTEXT:
                result.setDataType(OracleDataType.NCLOB.getText());
                break;
            case ENUM:
            case SET:
            case VARCHAR:
                result.setDataType(OracleDataType.NVARCHAR2.getText());
                useArgumentsStringList.addAll(argumentsStringList);
                break;
            default:
                result.setDataType(OracleDataType.NCLOB.getText());
                break;
        }

        result.setArgumentsStringList(useArgumentsStringList);
        return result;
    }


}
