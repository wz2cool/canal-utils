package com.github.wz2cool.canal.utils.converter.mssql;

import com.github.wz2cool.canal.utils.converter.AlterColumnSqlConverterBase;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.EnhancedAlterOperation;
import com.github.wz2cool.canal.utils.model.exception.NotSupportDataTypeException;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MssqlAlterColumnSqlConverter extends AlterColumnSqlConverterBase {

    private final MssqlColDataTypeConverter mssqlColDataTypeConverter = new MssqlColDataTypeConverter();

    @Override
    public List<String> convert(Alter mysqlAlter) {
        return null;
    }

    private List<String> convertToAddColumnSqlList(final List<AlterColumnExpression> alterColumnExpressions) {
        List<String> result = new ArrayList<>();
        if (alterColumnExpressions == null || alterColumnExpressions.isEmpty()) {
            return result;
        }

        List<AlterColumnExpression> addColumnExpressions = alterColumnExpressions.stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.ADD_COLUMN).collect(Collectors.toList());

        for (AlterColumnExpression addColumnExpression : addColumnExpressions) {
            String tableName = addColumnExpression.getTableName();
            String columnName = addColumnExpression.getColumnName();
            ColDataType mysqlColDataType = addColumnExpression.getColDataType();
            Optional<ColDataType> mssqlColDataTypeOptional = mssqlColDataTypeConverter.convert(mysqlColDataType);
            if (!mssqlColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Add Column] Cannot convert data type: %s", mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }

            String sqlserverDataTypeString = getMssqlDataTypeString(mssqlColDataTypeOptional.get());
            String addColumnSql = String.format("ALTER TABLE %s ADD (%s %s)",
                    tableName, columnName, sqlserverDataTypeString);
            result.add(addColumnSql);
        }
        return result;
    }

    private String getMssqlDataTypeString(final ColDataType colDataType) {
        List<String> argumentsStringList = colDataType.getArgumentsStringList();
        if (argumentsStringList == null || argumentsStringList.isEmpty()) {
            return colDataType.getDataType();
        } else {
            return colDataType.toString();
        }
    }
}
