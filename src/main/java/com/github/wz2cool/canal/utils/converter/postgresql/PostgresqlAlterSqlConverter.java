package com.github.wz2cool.canal.utils.converter.postgresql;

import com.github.wz2cool.canal.utils.converter.AlterSqlConverterBase;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PostgresqlAlterSqlConverter extends AlterSqlConverterBase {
    private final PostgresqlColDataTypeConverter postgresqlColDataTypeConverter = new PostgresqlColDataTypeConverter();

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return this.postgresqlColDataTypeConverter;
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
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String sql = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName);
        return Optional.of(sql);
    }

    @Override
    protected List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions) {
        return new ArrayList<>();
    }
}
