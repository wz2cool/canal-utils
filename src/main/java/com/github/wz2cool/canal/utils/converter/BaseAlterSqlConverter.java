package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.EnhancedAlterOperation;
import com.github.wz2cool.canal.utils.model.exception.NotSupportDataTypeException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Frank
 */
public abstract class BaseAlterSqlConverter {

    public List<String> convert(String mysqlAlterSqlInput) throws JSQLParserException {
        String mysqlAlterSql = cleanMysqlAlterSql(mysqlAlterSqlInput);

        List<String> result = new ArrayList<>();
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(mysqlAlterSql);
        if (!(statement instanceof Alter)) {
            return result;
        }

        Alter mysqlAlter = (Alter) statement;

        List<AlterColumnExpression> alterColumnExpressions = getAlterColumnExpressions(mysqlAlter);
        List<String> addColumnSqlList = alterColumnExpressions
                .stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.ADD_COLUMN)
                .map(this::convertToAddColumnSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        List<String> changeColumnTypeSqlList = alterColumnExpressions
                .stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.CHANGE_COLUMN_TYPE)
                .map(this::convertToChangeColumnTypeSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        List<String> renameColumnSqlList = alterColumnExpressions
                .stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.RENAME_COLUMN)
                .map(this::convertToRenameColumnSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        List<String> dropColumnSqlList = alterColumnExpressions
                .stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.DROP_COLUMN)
                .map(this::convertToDropColumnSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        List<String> otherColumnActionSqlList = convertToOtherColumnActionSqlList(alterColumnExpressions);

        result.addAll(addColumnSqlList);
        result.addAll(changeColumnTypeSqlList);
        result.addAll(renameColumnSqlList);
        result.addAll(dropColumnSqlList);
        result.addAll(otherColumnActionSqlList);
        return result;
    }

    protected abstract IColDataTypeConverter getColDataTypeConverter();

    protected abstract Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression);

    protected abstract Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression);

    protected abstract Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression);

    protected abstract Optional<String> convertToDropColumnSql(AlterColumnExpression alterColumnExpression);

    protected abstract List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions);

    private List<AlterColumnExpression> getAlterColumnExpressions(Alter mysqlAlter) {
        List<AlterColumnExpression> result = new ArrayList<>();
        if (mysqlAlter == null) {
            return result;
        }

        List<AlterExpression> alterExpressions = mysqlAlter.getAlterExpressions();
        List<AlterExpression> alterExpressionsForColumn = new ArrayList<>();
        String tableName = cleanText(mysqlAlter.getTable().getName());
        for (AlterExpression alterExpression : alterExpressions) {
            String optionalSpecifier = alterExpression.getOptionalSpecifier();
            if ("COLUMN".equalsIgnoreCase(optionalSpecifier)
                    || (alterExpression.getColDataTypeList() != null && !alterExpression.getColDataTypeList().isEmpty())
                    || (alterExpression.getOperation() == AlterOperation.DROP && alterExpression.getColumnName() != null)) {
                alterExpressionsForColumn.add(alterExpression);
            }
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


    protected String getDataTypeString(final ColDataType colDataType) {
        List<String> argumentsStringList = colDataType.getArgumentsStringList();
        if (argumentsStringList == null || argumentsStringList.isEmpty()) {
            return colDataType.getDataType();
        } else {
            return colDataType.toString();
        }
    }

    private List<AlterColumnExpression> getAlterColumnExpressions(
            final String tableName, final AlterExpression alterExpression) {
        List<AlterColumnExpression> result = new ArrayList<>();
        if (alterExpression == null) {
            return result;
        }

        AlterOperation operation = alterExpression.getOperation();
        String columnName = alterExpression.getColumnName();
        String colOldName = alterExpression.getColumnOldName();
        List<AlterExpression.ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();

        Optional<AlterColumnExpression> dropColumnExpression =
                getDropColumnExpression(operation, tableName, columnName);
        dropColumnExpression.ifPresent(result::add);

        List<AlterColumnExpression> renameColumnExpressions =
                getRenameColumnExpressions(operation, tableName, colOldName, colDataTypeList);
        result.addAll(renameColumnExpressions);

        List<AlterColumnExpression> addColumnExpressions =
                getAddColumnExpressions(operation, tableName, colDataTypeList);
        result.addAll(addColumnExpressions);

        List<AlterColumnExpression> changeColumnTypeExpressions =
                getChangeColumnTypeExpressions(operation, tableName, colOldName, colDataTypeList);
        result.addAll(changeColumnTypeExpressions);
        return result;
    }

    private List<AlterColumnExpression> getAddColumnExpressions(
            final AlterOperation alterOperation,
            final String tableName,
            final List<AlterExpression.ColumnDataType> columnDataTypes) {
        List<AlterColumnExpression> result = new ArrayList<>();
        if (alterOperation != AlterOperation.ADD || columnDataTypes == null || columnDataTypes.isEmpty()) {
            return result;
        }

        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            AlterColumnExpression addColumnExpression = new AlterColumnExpression();
            String columnName = columnDataType.getColumnName();

            ColDataType mysqlColDataType = columnDataType.getColDataType();
            Optional<ColDataType> covColDataTypeOptional = getColDataTypeConverter().convert(mysqlColDataType);
            if (!covColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Add Column] Cannot convert data type: %s",
                        mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }

            final boolean unsignedFlag = columnDataType.getColumnSpecs().stream().anyMatch("unsigned"::equalsIgnoreCase);
            addColumnExpression.setTableName(cleanText(tableName));
            addColumnExpression.setColumnName(cleanText(columnName));
            addColumnExpression.setOperation(EnhancedAlterOperation.ADD_COLUMN);
            addColumnExpression.setColDataType(covColDataTypeOptional.get());
            addColumnExpression.setUnsignedFlag(unsignedFlag);
            // 说明
            List<String> columnSpecs = columnDataType.getColumnSpecs();
            setColumnSpecs(addColumnExpression, columnSpecs);
            result.add(addColumnExpression);
        }
        return result;
    }

    private List<AlterColumnExpression> getRenameColumnExpressions(
            final AlterOperation alterOperation,
            final String tableName,
            final String colOldName,
            final List<AlterExpression.ColumnDataType> columnDataTypes
    ) {
        List<AlterColumnExpression> result = new ArrayList<>();
        if (alterOperation != AlterOperation.CHANGE
                || colOldName == null
                || columnDataTypes == null
                || columnDataTypes.isEmpty()) {
            return result;
        }

        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            String columnName = columnDataType.getColumnName();
            if (columnName.equals(colOldName)) {
                // ignore if column name not change.
                continue;
            }

            ColDataType mysqlColDataType = columnDataType.getColDataType();
            Optional<ColDataType> covColDataTypeOptional = getColDataTypeConverter().convert(mysqlColDataType);
            if (!covColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Rename Column] Cannot convert data type: %s",
                        mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }
            final boolean unsignedFlag = columnDataType.getColumnSpecs().stream().anyMatch("unsigned"::equalsIgnoreCase);
            AlterColumnExpression renameColumnExpression = new AlterColumnExpression();
            renameColumnExpression.setTableName(cleanText(tableName));
            renameColumnExpression.setColumnName(cleanText(columnName));
            renameColumnExpression.setColOldName(cleanText(colOldName));
            renameColumnExpression.setOperation(EnhancedAlterOperation.RENAME_COLUMN);
            renameColumnExpression.setColDataType(covColDataTypeOptional.get());
            renameColumnExpression.setUnsignedFlag(unsignedFlag);
            // 说明
            List<String> columnSpecs = columnDataType.getColumnSpecs();
            setColumnSpecs(renameColumnExpression, columnSpecs);
            result.add(renameColumnExpression);
        }
        return result;
    }

    private List<AlterColumnExpression> getChangeColumnTypeExpressions(
            final AlterOperation alterOperation,
            final String tableName,
            final String colOldName,
            final List<AlterExpression.ColumnDataType> columnDataTypes) {
        List<AlterColumnExpression> result = new ArrayList<>();
        // https://stackoverflow.com/questions/14767174/modify-column-vs-change-column
        // MODIFY COLUMN This command does everything CHANGE COLUMN can, but without renaming the column.
        if (!(alterOperation == AlterOperation.CHANGE || alterOperation == AlterOperation.MODIFY)
                || columnDataTypes == null
                || columnDataTypes.isEmpty()) {
            return result;
        }

        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            AlterColumnExpression changeTypeColumnExpression = new AlterColumnExpression();
            String columnName = columnDataType.getColumnName();
            if (alterOperation == AlterOperation.CHANGE && !columnName.equals(colOldName)) {
                // need check rename
                continue;
            }

            ColDataType mysqlColDataType = columnDataType.getColDataType();
            Optional<ColDataType> covColDataTypeOptional = getColDataTypeConverter().convert(mysqlColDataType);
            if (!covColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Change Column Type] Cannot convert data type: %s",
                        mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }
            final boolean unsignedFlag = columnDataType.getColumnSpecs().stream().anyMatch("unsigned"::equalsIgnoreCase);
            changeTypeColumnExpression.setTableName(cleanText(tableName));
            changeTypeColumnExpression.setColumnName(cleanText(columnName));
            changeTypeColumnExpression.setOperation(EnhancedAlterOperation.CHANGE_COLUMN_TYPE);
            changeTypeColumnExpression.setColDataType(covColDataTypeOptional.get());
            changeTypeColumnExpression.setUnsignedFlag(unsignedFlag);
            List<String> columnSpecs = columnDataType.getColumnSpecs();
            setColumnSpecs(changeTypeColumnExpression, columnSpecs);
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

    private String cleanMysqlAlterSql(String mysqlAlterSql) {
        return mysqlAlterSql.replace("COLLATE", "")
                .replace("collate", "");
    }

    private AlterColumnExpression setColumnSpecs(AlterColumnExpression columnExpression, List<String> columnSpecs) {
        columnExpression.setCommentText("");
        columnExpression.setNullAble("NULL");
        columnExpression.setDefaultValue("");
        for (int i = 0; i < columnSpecs.size(); i++) {
            if ("COMMENT".equalsIgnoreCase(columnSpecs.get(i))) {
                columnExpression.setCommentText(columnSpecs.get(i + 1));
            }
            if ("NULL".equalsIgnoreCase(columnSpecs.get(i)) && i != 0) {
                if (columnSpecs.get(i - 1).equalsIgnoreCase("NOT")) {
                    columnExpression.setNullAble("NOT NULL");
                }
            }
            if ("DEFAULT".equalsIgnoreCase(columnSpecs.get(i))) {
                columnExpression.setDefaultValue(columnSpecs.get(i + 1));
            }
        }
        return columnExpression;
    }
}
