package com.github.wz2cool.canal.utils.converter.mysql;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MysqlAlterSqlConverter extends BaseAlterSqlConverter {
    private final MysqlColDataTypeConverter mysqlColDataTypeConverter = new MysqlColDataTypeConverter();
    private static final String COMMENT = "COMMENT ";
    private static final String UNSIGNED = " UNSIGNED";
    private static final String DEFAULT = " DEFAULT ";

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return this.mysqlColDataTypeConverter;
    }

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String commentText = StringUtils.isBlank(alterColumnExpression.getCommentText()) ? "" : COMMENT + alterColumnExpression.getCommentText();
        String nullAble = StringUtils.isBlank(alterColumnExpression.getNullAble()) ? "" : alterColumnExpression.getNullAble();
        String defaultValue = StringUtils.isBlank(alterColumnExpression.getDefaultValue()) ? "" : DEFAULT + alterColumnExpression.getDefaultValue();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = alterColumnExpression.isUnsignedFlag() ?
                getDataTypeString(colDataType) + UNSIGNED : getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE `%s` ADD COLUMN `%s` %s %s %s %s ;",
                tableName, columnName, dataTypeString, nullAble, defaultValue, commentText);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String commentText = StringUtils.isBlank(alterColumnExpression.getCommentText()) ? "" : COMMENT + alterColumnExpression.getCommentText();
        String nullAble = StringUtils.isBlank(alterColumnExpression.getNullAble()) ? "" : alterColumnExpression.getNullAble();
        String defaultValue = StringUtils.isBlank(alterColumnExpression.getDefaultValue()) ? "" : DEFAULT + alterColumnExpression.getDefaultValue();
        String columnName = alterColumnExpression.getColumnName();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = alterColumnExpression.isUnsignedFlag() ?
                getDataTypeString(colDataType) + " UNSIGNED" : getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s %s %s %s ;",
                tableName, columnName, columnName, dataTypeString, nullAble, defaultValue, commentText);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression) {
        String tableName = alterColumnExpression.getTableName();
        String columnName = alterColumnExpression.getColumnName();
        String commentText = StringUtils.isBlank(alterColumnExpression.getCommentText()) ? "" : COMMENT + alterColumnExpression.getCommentText();
        String nullAble = StringUtils.isBlank(alterColumnExpression.getNullAble()) ? "" : alterColumnExpression.getNullAble();
        String defaultValue = StringUtils.isBlank(alterColumnExpression.getDefaultValue()) ? "" : DEFAULT + alterColumnExpression.getDefaultValue();
        String colOldName = alterColumnExpression.getColOldName();
        ColDataType colDataType = alterColumnExpression.getColDataType();
        String dataTypeString = alterColumnExpression.isUnsignedFlag() ?
                getDataTypeString(colDataType) + UNSIGNED : getDataTypeString(colDataType);
        String sql = String.format("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s %s %s %s ;",
                tableName, colOldName, columnName, dataTypeString, nullAble, defaultValue, commentText);
        return Optional.of(sql);
    }

    @Override
    protected Optional<String> convertToDropColumnSql(AlterColumnExpression alterColumnExpression) {
        String columnName = alterColumnExpression.getColumnName();
        String tableName = alterColumnExpression.getTableName();
        String sql = String.format("ALTER TABLE `%s` DROP COLUMN `%s` ;", tableName, columnName);
        return Optional.of(sql);
    }

    @Override
    protected List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions) {
        return new ArrayList<>();
    }
}
