package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.EnhancedAlterOperation;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class AlterColumnSqlConverterBase {
    public abstract List<String> convert(Alter mysqlAlter);

    protected List<AlterColumnExpression> getAlterColumnExpressions(Alter mysqlAlter) {

        List<AlterColumnExpression> result = new ArrayList<>();
        if (mysqlAlter == null) {
            return result;
        }

        List<AlterExpression> alterExpressions = mysqlAlter.getAlterExpressions();
        List<AlterExpression> alterExpressionsForColumn = new ArrayList<>();
        String tableName = cleanText(mysqlAlter.getTable().getName());
        for (AlterExpression alterExpression : alterExpressions) {
            String optionalSpecifier = alterExpression.getOptionalSpecifier();
            if (!"COLUMN".equalsIgnoreCase(optionalSpecifier)) {
                continue;
            }
            alterExpressionsForColumn.add(alterExpression);
        }

        if (alterExpressionsForColumn.isEmpty()) {
            return result;
        }

        for (AlterExpression alterExpression : alterExpressionsForColumn) {
            List<AlterColumnExpression> alterColumnExpressions =
                    getAlterColumnExpressions(tableName, alterExpression);
            result.addAll(alterColumnExpressions);
        }

        return result;
    }

    private List<AlterColumnExpression> getAlterColumnExpressions(
            final String tableName, final AlterExpression alterExpression) {
        if (alterExpression == null) {
            return new ArrayList<>();
        }

        List<AlterColumnExpression> result = new ArrayList<>();
        AlterOperation operation = alterExpression.getOperation();
        Optional<AlterColumnExpression> renameColumnExpression =
                getRenameColumnExpression(operation, tableName, alterExpression.getColumnName(), alterExpression.getColOldName());
        renameColumnExpression.ifPresent(result::add);
        Optional<AlterColumnExpression> dropColumnExpression =
                getDropColumnExpression(operation, tableName, alterExpression.getColumnName());
        dropColumnExpression.ifPresent(result::add);

        List<AlterColumnExpression> addColumnExpressions =
                getAddColumnExpressions(operation, tableName, alterExpression.getColDataTypeList());
        result.addAll(addColumnExpressions);
        List<AlterColumnExpression> changeTypeColumnExpressions =
                getChangeColumnTypeExpressions(operation, tableName, alterExpression.getColDataTypeList());
        result.addAll(changeTypeColumnExpressions);
        return result;
    }

    private List<AlterColumnExpression> getAddColumnExpressions(
            final AlterOperation alterOperation,
            final String tableName,
            final List<AlterExpression.ColumnDataType> columnDataTypes) {
        if (alterOperation != AlterOperation.ADD || columnDataTypes == null || columnDataTypes.isEmpty()) {
            return new ArrayList<>();
        }

        List<AlterColumnExpression> result = new ArrayList<>();
        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            AlterColumnExpression addColumnExpression = new AlterColumnExpression();
            String columnName = columnDataType.getColumnName();
            ColDataType colDataType = columnDataType.getColDataType();
            addColumnExpression.setTableName(cleanText(tableName));
            addColumnExpression.setColumnName(cleanText(columnName));
            addColumnExpression.setOperation(EnhancedAlterOperation.ADD_COLUMN);
            addColumnExpression.setColDataType(colDataType);
            result.add(addColumnExpression);
        }
        return result;
    }

    private Optional<AlterColumnExpression> getRenameColumnExpression(
            final AlterOperation alterOperation,
            final String tableName,
            final String columnName,
            final String colOldName
    ) {
        if (alterOperation != AlterOperation.CHANGE
                || columnName == null
                || colOldName == null
                || columnName.equals(colOldName)) {
            return Optional.empty();
        }

        AlterColumnExpression renameColumnExpression = new AlterColumnExpression();
        renameColumnExpression.setTableName(cleanText(tableName));
        renameColumnExpression.setColumnName(cleanText(columnName));
        renameColumnExpression.setColOldName(cleanText(colOldName));
        renameColumnExpression.setOperation(EnhancedAlterOperation.RENAME_COLUMN);
        return Optional.of(renameColumnExpression);
    }

    private List<AlterColumnExpression> getChangeColumnTypeExpressions(
            final AlterOperation alterOperation,
            final String tableName,
            final List<AlterExpression.ColumnDataType> columnDataTypes) {
        if (alterOperation != AlterOperation.CHANGE || columnDataTypes == null || columnDataTypes.isEmpty()) {
            return new ArrayList<>();
        }

        List<AlterColumnExpression> result = new ArrayList<>();
        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            AlterColumnExpression changeTypeColumnExpression = new AlterColumnExpression();
            String columnName = columnDataType.getColumnName();
            ColDataType colDataType = columnDataType.getColDataType();
            changeTypeColumnExpression.setTableName(cleanText(tableName));
            changeTypeColumnExpression.setColumnName(cleanText(columnName));
            changeTypeColumnExpression.setOperation(EnhancedAlterOperation.CHANGE_COLUMN_TYPE);
            changeTypeColumnExpression.setColDataType(colDataType);
            result.add(changeTypeColumnExpression);
        }
        return result;
    }

    private Optional<AlterColumnExpression> getDropColumnExpression(
            final AlterOperation alterOperation,
            final String tableName,
            final String columnName
    ) {
        if (alterOperation != AlterOperation.DROP || columnName == null) {
            return Optional.empty();
        }

        AlterColumnExpression dropColumnExpression = new AlterColumnExpression();
        dropColumnExpression.setTableName(cleanText(tableName));
        dropColumnExpression.setColumnName(cleanText(columnName));
        dropColumnExpression.setOperation(EnhancedAlterOperation.DROP_COLUMN);
        return Optional.of(dropColumnExpression);
    }

    private String cleanText(String element) {
        return element.replace("`", "");
    }
}
