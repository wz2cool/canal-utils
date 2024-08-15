package com.github.wz2cool.canal.utils.converter.mysql;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;

/**
 * A decorator for MysqlAlterSqlConverter that enhances its functionality by adding support
 * for DEFAULT values and handling fully qualified table names with schema prefixes.
 * @author penghai
 */
public class MysqlAlterSqlConverterDecorator extends BaseAlterSqlConverter {
    private static final String DEFAULT = "DEFAULT ";
    private final MysqlAlterSqlConverter wrapped;

    /**
     * Constructor
     * @param wrapped wrapped
     */
    public MysqlAlterSqlConverterDecorator(MysqlAlterSqlConverter wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        return Optional.of(generateColumnSql(
                "ADD",
                alterColumnExpression.getSchemaName(),
                alterColumnExpression.getTableName(),
                "",
                alterColumnExpression.getColumnName(),
                getFormattedDataType(alterColumnExpression),
                alterColumnExpression.getNullAble(),
                getFormattedDefaultValue(alterColumnExpression),
                alterColumnExpression.getCommentText())
        );
    }

    @Override
    protected Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression) {
        return Optional.of(generateColumnSql(
                "CHANGE",
                alterColumnExpression.getSchemaName(),
                alterColumnExpression.getTableName(),
                alterColumnExpression.getColumnName(),
                alterColumnExpression.getColumnName(),
                getFormattedDataType(alterColumnExpression),
                alterColumnExpression.getNullAble(),
                getFormattedDefaultValue(alterColumnExpression),
                alterColumnExpression.getCommentText())
        );
    }

    @Override
    protected Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression) {
        return Optional.of(generateColumnSql(
                "CHANGE",
                alterColumnExpression.getSchemaName(),
                alterColumnExpression.getTableName(),
                alterColumnExpression.getColOldName(),
                alterColumnExpression.getColumnName(),
                getFormattedDataType(alterColumnExpression),
                alterColumnExpression.getNullAble(),
                getFormattedDefaultValue(alterColumnExpression),
                alterColumnExpression.getCommentText()
        ));
    }

    @Override
    protected Optional<String> convertToDropColumnSql(AlterColumnExpression alterColumnExpression) {
        String qualifiedTableName = getQualifiedTableName(alterColumnExpression.getSchemaName(), alterColumnExpression.getTableName());
        String formattedColumnName = getFormattedColumnNameOrEmpty(alterColumnExpression.getColumnName());
        String sql = String.format("ALTER TABLE %s DROP COLUMN %s;", qualifiedTableName, formattedColumnName).trim();
        return Optional.of(sql);
    }

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return wrapped.getColDataTypeConverter();
    }

    @Override
    protected List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions) {
        return wrapped.convertToOtherColumnActionSqlList(alterColumnExpressions);
    }

    private String getFormattedDefaultValue(AlterColumnExpression alterColumnExpression) {
        return StringUtils.isBlank(alterColumnExpression.getDefaultValue())
                ? ""
                : (DEFAULT + alterColumnExpression.getDefaultValue());
    }

    protected String generateColumnSql(String action, String schemaName, String tableName, String oldColumnName, String newColumnName,
                                       String dataTypeString,
                                       String nullAble, String defaultValue, String commentText) {
        return wrapped.generateColumnSql(action, schemaName, tableName, oldColumnName, newColumnName,
                dataTypeString, nullAble, defaultValue, commentText);
    }


    protected String getFormattedDataType(AlterColumnExpression alterColumnExpression) {
        return wrapped.getFormattedDataType(alterColumnExpression);
    }


    protected String getQualifiedTableName(String schemaName, String tableName) {
        return wrapped.getQualifiedTableName(schemaName, tableName);
    }


    protected String getFormattedColumnNameOrEmpty(String columnName) {
        return wrapped.getFormattedColumnNameOrEmpty(columnName);
    }
}
