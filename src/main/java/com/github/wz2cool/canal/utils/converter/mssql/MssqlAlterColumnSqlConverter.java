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
        List<String> result = new ArrayList<>();
        if (mysqlAlter == null) {
            return result;
        }

        List<AlterColumnExpression> mysqlAlterColumnExpressions = getMysqlAlterColumnExpressions(mysqlAlter);
        List<String> convertToAddColumnSqlList = convertToAddColumnSqlList(mysqlAlterColumnExpressions);
        List<String> convertToChangeColumnTypeSqlList = convertToChangeColumnTypeSqlList(mysqlAlterColumnExpressions);
        List<String> convertToRenameColumnSqlList = convertToRenameColumnSqlList(mysqlAlterColumnExpressions);
        List<String> convertToDropColumnSqlList = convertToDropColumnSqlList(mysqlAlterColumnExpressions);
        result.addAll(convertToAddColumnSqlList);
        result.addAll(convertToChangeColumnTypeSqlList);
        result.addAll(convertToRenameColumnSqlList);
        result.addAll(convertToDropColumnSqlList);
        return result;
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
            String addColumnSql = String.format("ALTER TABLE %s ADD %s %s",
                    tableName, columnName, sqlserverDataTypeString);
            result.add(addColumnSql);
        }
        return result;
    }

    private List<String> convertToChangeColumnTypeSqlList(final List<AlterColumnExpression> alterColumnExpressions) {
        List<String> result = new ArrayList<>();
        if (alterColumnExpressions == null || alterColumnExpressions.isEmpty()) {
            return result;
        }

        List<AlterColumnExpression> changeColumnTypeExpressions = alterColumnExpressions.stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.CHANGE_COLUMN_TYPE).collect(Collectors.toList());

        for (AlterColumnExpression changeColumnTypeExpression : changeColumnTypeExpressions) {
            String tableName = changeColumnTypeExpression.getTableName();
            String columnName = changeColumnTypeExpression.getColumnName();

            ColDataType mysqlColDataType = changeColumnTypeExpression.getColDataType();
            Optional<ColDataType> mssqlColDataTypeOptional = mssqlColDataTypeConverter.convert(mysqlColDataType);
            if (!mssqlColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Change Type] Cannot convert data type: %s", mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }
            String mssqlDataTypeString = getMssqlDataTypeString(mssqlColDataTypeOptional.get());
            String changeTypeSql = String.format("ALTER TABLE %s ALTER COLUMN %s %s",
                    tableName, columnName, mssqlDataTypeString);
            result.add(changeTypeSql);
        }
        return result;
    }

    private List<String> convertToRenameColumnSqlList(final List<AlterColumnExpression> alterColumnExpressions) {
        List<String> result = new ArrayList<>();
        if (alterColumnExpressions == null || alterColumnExpressions.isEmpty()) {
            return result;
        }

        List<AlterColumnExpression> renameColumnExpressions = alterColumnExpressions.stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.RENAME_COLUMN).collect(Collectors.toList());

        for (AlterColumnExpression renameColumnExpression : renameColumnExpressions) {
            String tableName = renameColumnExpression.getTableName();
            String columnName = renameColumnExpression.getColumnName();
            String colOldName = renameColumnExpression.getColOldName();

            String renameSql = String.format("EXEC sp_RENAME '%s.%s', '%s', 'COLUMN'",
                    tableName, colOldName, columnName);
            result.add(renameSql);
        }
        return result;
    }

    private List<String> convertToDropColumnSqlList(final List<AlterColumnExpression> alterColumnExpressions) {
        List<String> result = new ArrayList<>();
        if (alterColumnExpressions == null || alterColumnExpressions.isEmpty()) {
            return result;
        }

        List<AlterColumnExpression> dropColumnExpressions = alterColumnExpressions.stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.DROP_COLUMN).collect(Collectors.toList());

        for (AlterColumnExpression dropColumnExpression : dropColumnExpressions) {
            String columnName = dropColumnExpression.getColumnName();
            String tableName = dropColumnExpression.getTableName();
            String dropSql = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName);
            result.add(dropSql);
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
