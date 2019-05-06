package com.github.wz2cool.canal.utils.converter.h2;

import com.github.wz2cool.canal.utils.converter.AlterSqlConverterBase;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class H2AlterSqlConverter extends AlterSqlConverterBase {
    private final H2ColDataTypeConverter h2ColDataTypeConverter = new H2ColDataTypeConverter();

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return this.h2ColDataTypeConverter;
    }

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE %s ADD %s %s",
                tableName, columnName, dataTypeString);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
                tableName, columnName, dataTypeString);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String colOldName = alterColumnExpression.getColOldName();
        String sql = String.format("ALTER TABLE %s RENAME COLUMN %s TO %s",
                tableName, colOldName, columnName);
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
        return new ArrayList<>();
    }
}
