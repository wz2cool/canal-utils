package com.github.wz2cool.canal.utils.converter.db2;

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

public class Db2AlterColumnSqlConverter extends AlterColumnSqlConverterBase {

    private final Db2ColDataTypeConverter db2ColDataTypeConverter = new Db2ColDataTypeConverter();

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
        List<String> reorgTableSqlList = getReorgTableSqlList(mysqlAlterColumnExpressions);
        result.addAll(convertToAddColumnSqlList);
        result.addAll(convertToChangeColumnTypeSqlList);
        result.addAll(convertToRenameColumnSqlList);
        result.addAll(convertToDropColumnSqlList);
        result.addAll(reorgTableSqlList);
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
            Optional<ColDataType> db2ColDataTypeOptional = db2ColDataTypeConverter.convert(mysqlColDataType);
            if (!db2ColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Add Column] Cannot convert data type: %s", mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }

            String db2DataTypeString = getDb2DataTypeString(db2ColDataTypeOptional.get());
            String addColumnSql = String.format("ALTER TABLE %s ADD COLUMN %s %s",
                    tableName, columnName, db2DataTypeString);
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
            Optional<ColDataType> db2ColDataTypeOptional = db2ColDataTypeConverter.convert(mysqlColDataType);
            if (!db2ColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Change Type] Cannot convert data type: %s", mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }
            String db2DataTypeString = getDb2DataTypeString(db2ColDataTypeOptional.get());
            String changeTypeSql = String.format("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
                    tableName, columnName, db2DataTypeString);
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

            String renameSql = String.format("ALTER TABLE %s RENAME COLUMN %s To %s",
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

    // https://dba.stackexchange.com/questions/127848/db2-reorg-recommended-commands
    private List<String> getReorgTableSqlList(final List<AlterColumnExpression> alterColumnExpressions) {
        // need reorg table if drop column and change column type
        List<String> result = new ArrayList<>();
        if (alterColumnExpressions == null || alterColumnExpressions.isEmpty()) {
            return result;
        }

        List<String> needReorgTables = alterColumnExpressions.stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.DROP_COLUMN
                        || x.getOperation() == EnhancedAlterOperation.CHANGE_COLUMN_TYPE)
                .map(AlterColumnExpression::getTableName)
                .distinct()
                .collect(Collectors.toList());

        for (String tableName : needReorgTables) {
            String sql = String.format("Call Sysproc.admin_cmd ('reorg Table %s')", tableName);
            result.add(sql);
        }
        return result;
    }

    private String getDb2DataTypeString(final ColDataType colDataType) {
        List<String> argumentsStringList = colDataType.getArgumentsStringList();
        if (argumentsStringList == null || argumentsStringList.isEmpty()) {
            return colDataType.getDataType();
        } else {
            return colDataType.toString();
        }
    }
}
