package com.github.wz2cool.canal.utils.converter.db2;

import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.Db2DataType;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Db2ColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
        if (mysqlColDataType == null) {
            return Optional.empty();
        }

        Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(mysqlColDataType.getDataType());
        if (!mysqlDataTypeOptional.isPresent()) {
            return Optional.empty();
        }

        ColDataType colDataType = getDb2ColDataType(
                mysqlDataTypeOptional.get(), mysqlColDataType.getArgumentsStringList());
        return Optional.ofNullable(colDataType);
    }

    // https://www.ibm.com/support/knowledgecenter/SSEP7J_10.1.1/com.ibm.swg.ba.cognos.vvm_reference_guide.10.1.1.doc/c_mysqlds.html#MySQLDS
    private ColDataType getDb2ColDataType(final MysqlDataType mysqlDataType, final List<String> argumentsStringList) {
        ColDataType result = new ColDataType();
        List<String> useArgumentsStringList = argumentsStringList == null ? new ArrayList<>() : argumentsStringList;
        List<String> argStrings = new ArrayList<>();

        switch (mysqlDataType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
                result.setDataType(Db2DataType.SMALLINT.getText());
                break;
            case MEDIUMINT:
            case INT:
            case INTEGER:
                result.setDataType(Db2DataType.INTEGER.getText());
                break;
            case BIGINT:
                result.setDataType(Db2DataType.BIGINT.getText());
                break;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                result.setDataType(Db2DataType.DECIMAL.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case DATE:
                result.setDataType(Db2DataType.DATE.getText());
                break;
            case DATETIME:
            case TIMESTAMP:
                result.setDataType(Db2DataType.TIMESTAMP.getText());
                break;
            case TIME:
                result.setDataType(Db2DataType.TIME.getText());
                break;
            case VARCHAR:
            case CHAR:
                result.setDataType(Db2DataType.VARCHAR.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                result.setDataType(Db2DataType.BLOB.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                result.setDataType(Db2DataType.CLOB.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
        }
        result.setArgumentsStringList(argStrings);
        return result;
    }
}
