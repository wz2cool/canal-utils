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

        String table = mysqlAlter.getTable().getName().replace("`", "");
        List<String> result = new ArrayList<>();
        for (AlterExpression alterExpression : alterColumnExpressions) {
            List<String> alterColumnSqls = generateAlterColumnSqls(table, alterExpression);
            if (!alterColumnSqls.isEmpty()) {
                result.addAll(alterColumnSqls);
            }
        }
        return result;
    }

    private List<String> generateAlterColumnSqls(final String table, final AlterExpression alterExpression) {
        AlterOperation operation = alterExpression.getOperation();
        List<AlterExpression.ColumnDataType> columnDataTypes = alterExpression.getColDataTypeList();
        List<String> result = new ArrayList<>();
        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            Optional<String> alterColumnSqlOptional = generateAlterColumnSql(table, operation, columnDataType);
            alterColumnSqlOptional.ifPresent(result::add);
        }
        return result;
    }

    private Optional<String> generateAlterColumnSql(
            final String table,
            final AlterOperation operation,
            final AlterExpression.ColumnDataType columnDataType) {

        String columnName = columnDataType.getColumnName().replace("`", "");
        ColDataType mysqlColDataType = columnDataType.getColDataType();

        if (operation == AlterOperation.DROP) {
            String result = String.format("ALTER TABLE %s DROP %s", table, columnName);
            return Optional.ofNullable(result);
        } else if (operation == AlterOperation.ADD) {
            Optional<ColDataType> oracleColDataTypeOptional = oracleColDataTypeConverter.convert(mysqlColDataType);
            if (!oracleColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("Cannot convert data type: %s", mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }
            String result = String.format("ALTER TABLE %s ADD (%s %s)",
                    table, columnName, oracleColDataTypeOptional.get());
            return Optional.ofNullable(result);
        }

        return Optional.empty();
    }
}
