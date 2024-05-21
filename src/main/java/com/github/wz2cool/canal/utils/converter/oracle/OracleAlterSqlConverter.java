package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OracleAlterSqlConverter extends BaseAlterSqlConverter {
    private final OracleColDataTypeConverter oracleColDataTypeConverter = new OracleColDataTypeConverter();
    private static final String COMMENT_FORMAT = "COMMENT ON COLUMN %s.%s IS %s";

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return this.oracleColDataTypeConverter;
    }

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String commentText = StringUtils.isBlank(alterColumnExpression.getCommentText()) ? "" : String.format(COMMENT_FORMAT, tableName, columnName, alterColumnExpression.getCommentText());

        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE %s ADD (%s %s);%s",
                tableName, columnName, dataTypeString, commentText);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String commentText = StringUtils.isBlank(alterColumnExpression.getCommentText()) ? "" : String.format(COMMENT_FORMAT, tableName, columnName, alterColumnExpression.getCommentText());
        String dataTypeString = getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE %s MODIFY (%s %s);%s",
                tableName, columnName, dataTypeString, commentText);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String colOldName = alterColumnExpression.getColOldName();
        String commentText = StringUtils.isBlank(alterColumnExpression.getCommentText()) ? "" : String.format(COMMENT_FORMAT, tableName, columnName, alterColumnExpression.getCommentText());
        String sql = String.format("ALTER TABLE %s RENAME COLUMN %s to %s;%s",
                tableName, colOldName, columnName, commentText);
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
