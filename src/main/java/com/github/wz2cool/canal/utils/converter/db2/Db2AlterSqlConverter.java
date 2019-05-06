package com.github.wz2cool.canal.utils.converter.db2;

import com.github.wz2cool.canal.utils.converter.AlterSqlConverterBase;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.EnhancedAlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Db2AlterSqlConverter extends AlterSqlConverterBase {
    private final Db2ColDataTypeConverter db2ColDataTypeConverter = new Db2ColDataTypeConverter();

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return this.db2ColDataTypeConverter;
    }

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE %s ADD COLUMN %s %s",
                tableName, columnName, dataTypeString);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
                tableName, columnName, dataTypeString);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String oldColumnName = alterColumnExpression.getColOldName();
        String sql = String.format("ALTER TABLE %s RENAME COLUMN %s To %s",
                tableName, oldColumnName, columnName);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToDropColumnSql(AlterColumnExpression alterColumnExpression) {
        String columnName = alterColumnExpression.getColumnName();
        String tableName = alterColumnExpression.getTableName();
        String sql = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName);
        return Optional.of(sql);
    }

    @Override
    protected List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions) {
        return getReorgTableSqlList(alterColumnExpressions);
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
}
