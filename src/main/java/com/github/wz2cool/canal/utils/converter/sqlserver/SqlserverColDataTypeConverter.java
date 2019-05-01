package com.github.wz2cool.canal.utils.converter.sqlserver;

import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.SqlserverDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SqlserverColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
        if (mysqlColDataType == null) {
            return Optional.empty();
        }

        Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(mysqlColDataType.getDataType());
        if (!mysqlDataTypeOptional.isPresent()) {
            return Optional.empty();
        }

        ColDataType colDataType = getSqlserverColDataType(
                mysqlDataTypeOptional.get(), mysqlColDataType.getArgumentsStringList());
        return Optional.ofNullable(colDataType);
    }

    // https://dev.mysql.com/doc/workbench/en/wb-migration-database-mssql-typemapping.html
    // https://docs.microsoft.com/en-us/sql/ssma/mysql/project-settings-type-mapping-mysqltosql?view=sql-server-2017
    private ColDataType getSqlserverColDataType(final MysqlDataType mysqlDataType, final List<String> argumentsStringList) {
        ColDataType result = new ColDataType();
        List<String> useArgumentsStringList = argumentsStringList == null ? new ArrayList<>() : argumentsStringList;
        List<String> argStrings = new ArrayList<>();

        switch (mysqlDataType) {
            case BIT:
                result.setDataType(SqlserverDataType.BIT.getText());
                break;
            case TINYINT:
                result.setDataType(SqlserverDataType.TINYINT.getText());
                break;
            case SMALLINT:
                result.setDataType(SqlserverDataType.SMALLINT.getText());
                break;
            case MEDIUMINT:
            case INT:
            case INTEGER:
                result.setDataType(SqlserverDataType.INT.getText());
                break;
            case BIGINT:
                result.setDataType(SqlserverDataType.BIGINT.getText());
                break;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                result.setDataType(SqlserverDataType.DECIMAL.getText());
                break;
            case DATE:
                result.setDataType(SqlserverDataType.DATE.getText());
                break;
            case DATETIME:
                result.setDataType(SqlserverDataType.DATETIME.getText());
                break;
            case TIMESTAMP:
                result.setDataType(SqlserverDataType.TIMESTAMP.getText());
                break;
            case TIME:
                result.setDataType(SqlserverDataType.TIME.getText());
                break;
            case CHAR:
                result.setDataType(SqlserverDataType.NCHAR.getText());
                break;
            case VARCHAR:
                result.setDataType(SqlserverDataType.NVARCHAR.getText());
                argStrings.addAll(useArgumentsStringList);
                break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                result.setDataType(SqlserverDataType.VARBINARY.getText());
                break;
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                result.setDataType(SqlserverDataType.NTEXT.getText());
                break;
        }

        result.setArgumentsStringList(argStrings);
        return result;
    }
}
