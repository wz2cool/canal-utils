package com.github.wz2cool.canal.utils.converter.mysql;

import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * Enhanced MySQL alter SQL converter with DEFAULT functionality and qualified table names.
 * Extends the default MysqlAlterSqlConverter and adds DEFAULT functionality and qualified table names.
 * @author penghai
 */
public class MysqlEnhancedAlterSqlConverter extends MysqlAlterSqlConverter {
    private static final String DEFAULT = " DEFAULT ";

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        return Optional.of(generateColumnSql(
                "ADD",
                alterColumnExpression.getSchemaName(),
                alterColumnExpression.getTableName(),
                "",
                alterColumnExpression.getColumnName(),
                getFormattedDataType(alterColumnExpression),
                formatNullable(alterColumnExpression),
                formatDefaultValue(alterColumnExpression),
                formatComment(alterColumnExpression)
        ));
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
                formatNullable(alterColumnExpression),
                formatDefaultValue(alterColumnExpression),
                formatComment(alterColumnExpression)
        ));
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
                formatNullable(alterColumnExpression),
                formatDefaultValue(alterColumnExpression),
                formatComment(alterColumnExpression)
        ));
    }

    @Override
    protected Optional<String> convertToDropColumnSql(AlterColumnExpression alterColumnExpression) {
        String sql = String.format("ALTER TABLE %s DROP COLUMN `%s`",
                getQualifiedTableName(alterColumnExpression.getSchemaName(), alterColumnExpression.getTableName()),
                alterColumnExpression.getColumnName()).trim() + END_SIGN;
        return Optional.of(sql);
    }

    private String formatDefaultValue(AlterColumnExpression alterColumnExpression) {
        return StringUtils.isBlank(alterColumnExpression.getDefaultValue())
                ? ""
                : (DEFAULT + alterColumnExpression.getDefaultValue());
    }



}
