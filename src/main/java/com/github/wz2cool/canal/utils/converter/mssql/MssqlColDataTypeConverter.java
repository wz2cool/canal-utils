package com.github.wz2cool.canal.utils.converter.mssql;

import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.MssqlDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MssqlColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
        if (mysqlColDataType == null) {
            return Optional.empty();
        }

        Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(mysqlColDataType.getDataType());
        if (!mysqlDataTypeOptional.isPresent()) {
            return Optional.empty();
        }

        ColDataType colDataType = getMssqlColDataType(
                mysqlDataTypeOptional.get(), mysqlColDataType.getArgumentsStringList());
        return Optional.ofNullable(colDataType);
    }

    // https://dev.mysql.com/doc/workbench/en/wb-migration-database-mssql-typemapping.html
    // https://docs.microsoft.com/en-us/sql/ssma/mysql/project-settings-type-mapping-mysqltosql?view=sql-server-2017
    private ColDataType getMssqlColDataType(final MysqlDataType mysqlDataType, final List<String> argumentsStringList) {
        ColDataType result = new ColDataType();
        List<String> useArgumentsStringList = argumentsStringList == null ? new ArrayList<>() : argumentsStringList;
        List<String> argStrings = new ArrayList<>();

        switch (mysqlDataType) {
            case BIT:
                result.setDataType(MssqlDataType.BIT.getText());
                break;
            case TINYINT:
                result.setDataType(MssqlDataType.TINYINT.getText());
                break;
            case SMALLINT:
                result.setDataType(MssqlDataType.SMALLINT.getText());
                break;
            case MEDIUMINT:
            case INT:
            case INTEGER:
                result.setDataType(MssqlDataType.INT.getText());
                break;
            case BIGINT:
                result.setDataType(MssqlDataType.BIGINT.getText());
                break;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                result.setDataType(MssqlDataType.DECIMAL.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case DATE:
                result.setDataType(MssqlDataType.DATE.getText());
                break;
            case DATETIME:
            case TIMESTAMP:
                result.setDataType(MssqlDataType.DATETIME.getText());
                break;
            case TIME:
                result.setDataType(MssqlDataType.TIME.getText());
                break;
            case CHAR:
            case VARCHAR:
                result.setDataType(MssqlDataType.NVARCHAR.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                result.setDataType(MssqlDataType.VARBINARY.getText());
                argStrings.add("MAX");
                break;
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                result.setDataType(MssqlDataType.NTEXT.getText());
            case JSON:
                result.setDataType(MssqlDataType.NVARCHAR.getText());
                argStrings.add("MAX");
                break;
        }

        result.setArgumentsStringList(argStrings);
        return result;
    }
}
