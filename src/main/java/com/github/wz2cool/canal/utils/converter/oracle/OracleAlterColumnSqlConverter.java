package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.converter.IAlterColumnSqlConverter;
import com.github.wz2cool.canal.utils.model.exception.NotSupportDataTypeException;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class OracleAlterColumnSqlConverter implements IAlterColumnSqlConverter {
    private final OracleColDataTypeConverter oracleColDataTypeConverter = new OracleColDataTypeConverter();

    @Override
    public List<String> convert(Alter mysqlAlter) {
        if (mysqlAlter == null) {
            return new ArrayList<>();
        }

        List<AlterExpression> alterColumnExpressions = mysqlAlter.getAlterExpressions()
                .stream().filter(x -> !x.getColDataTypeList().isEmpty()).collect(Collectors.toList());
        if (alterColumnExpressions.isEmpty()) {
            return new ArrayList<>();
        }

        String table = cleanText(mysqlAlter.getTable().getName());
        List<String> result = new ArrayList<>();
        for (AlterExpression alterExpression : alterColumnExpressions) {
            String optionalSpecifier = alterExpression.getOptionalSpecifier();
            // only get alter column
            if (!"COLUMN".equalsIgnoreCase(optionalSpecifier)) {
                continue;
            }

            List<String> alterColumnSqlList = generateAlterColumnSqlList(table, alterExpression);
            if (!alterColumnSqlList.isEmpty()) {
                result.addAll(alterColumnSqlList);
            }
        }
        return result;
    }

    private List<String> generateAlterColumnSqlList(final String table, final AlterExpression alterExpression) {
        AlterOperation operation = alterExpression.getOperation();
        String colOldName = cleanText(alterExpression.getColOldName());
        List<AlterExpression.ColumnDataType> columnDataTypes = alterExpression.getColDataTypeList();
        List<String> result = new ArrayList<>();
        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            Optional<String> alterColumnSqlOptional = generateAlterColumnSql(
                    table, operation, colOldName, columnDataType);
            alterColumnSqlOptional.ifPresent(result::add);
        }
        return result;
    }

    private Optional<String> generateAlterColumnSql(
            final String table,
            final AlterOperation operation,
            final String colOldName,
            final AlterExpression.ColumnDataType columnDataType) {

        String columnName = cleanText(columnDataType.getColumnName());
        ColDataType mysqlColDataType = columnDataType.getColDataType();
        Optional<ColDataType> oracleColDataTypeOptional = oracleColDataTypeConverter.convert(mysqlColDataType);

        if (operation == AlterOperation.DROP) {
            String result = String.format("ALTER TABLE %s DROP %s", table, columnName);
            return Optional.ofNullable(result);
        } else if (operation == AlterOperation.ADD) {
            if (!oracleColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("Cannot convert data type: %s", mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }
            String result = String.format("ALTER TABLE %s ADD (%s %s)",
                    table, columnName, oracleColDataTypeOptional.get());
            return Optional.ofNullable(result);
        } else if (operation == AlterOperation.CHANGE) {
            // rename
            if (!columnName.equals(colOldName)) {
                String result = String.format("ALTER TABLE %s RENAME COLUMN %s to %s",
                        table, colOldName, columnName);
                return Optional.ofNullable(result);
            } else {
                if (!oracleColDataTypeOptional.isPresent()) {
                    String errorMsg = String.format("Cannot convert data type: %s", mysqlColDataType.getDataType());
                    throw new NotSupportDataTypeException(errorMsg);
                }
                String result = String.format("ALTER TABLE %s MODIFY (%s %s)",
                        table, columnName, oracleColDataTypeOptional.get());
                return Optional.ofNullable(result);
            }
        }

        return Optional.empty();
    }

    private String cleanText(String element) {
        return element.replace("`", "");
    }
}
