package com.github.wz2cool.canal.utils.converter.mssql;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author Frank
 */
public class MssqlAlterSqlConverter extends BaseAlterSqlConverter {
    private final MssqlColDataTypeConverter mssqlColDataTypeConverter = new MssqlColDataTypeConverter();
    private static final String DEFAULT = " DEFAULT ";

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return this.mssqlColDataTypeConverter;
    }

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String nullAble = StringUtils.isBlank(alterColumnExpression.getNullAble()) ? "" : alterColumnExpression.getNullAble();
        String commentText = StringUtils.isBlank(alterColumnExpression.getCommentText()) ? "" : alterColumnExpression.getCommentText();
        String defaultValue = StringUtils.isBlank(alterColumnExpression.getDefaultValue()) ? "" : DEFAULT + alterColumnExpression.getDefaultValue();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = getDataTypeString(colDataType);
        String comment = String.format("EXEC sp_addextendedproperty\n" +
                        "'MS_Description', N%s,\n" +
                        "'SCHEMA', N'dbo',\n" +
                        "'TABLE', N'%s',\n" +
                        "'COLUMN', N'%s'\n"
                , commentText, tableName, columnName);
        String sql = String.format("ALTER TABLE %s ADD %s %s %s %s;%s",
                tableName, columnName, dataTypeString, nullAble, defaultValue, comment);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String nullAble = StringUtils.isBlank(alterColumnExpression.getNullAble()) ? "" : alterColumnExpression.getNullAble();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE %s ALTER COLUMN %s %s %s",
                tableName, columnName, dataTypeString, nullAble);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String colOldName = alterColumnExpression.getColOldName();
        String sql = String.format("EXEC sp_RENAME '%s.%s', '%s', 'COLUMN'",
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

    @Override
    protected Optional<String> convertToDropIndexSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String indexName = alterColumnExpression.getColumnName();
        String sql = String.format("DROP INDEX %s ON %s;", indexName, tableName);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToAddIndexSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String constraintName = alterColumnExpression.getColumnName();
        String constraintType = alterColumnExpression.getColOldName();
        String columns = alterColumnExpression.getCommentText();
        
        String sql = String.format("ALTER TABLE %s ADD CONSTRAINT %s %s (%s);", 
                tableName, constraintName, constraintType, columns);
        return Optional.of(sql);
    }
}
