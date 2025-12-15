package com.github.wz2cool.canal.utils.converter.mysql;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.converter.mysql.decorator.AlterSqlConverterDecorator;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.SqlContext;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * MySQL alter 语句转换装饰器
 *
 * @author YinPenghai
 */
public class MysqlAlterSqlConverter extends BaseAlterSqlConverter {
    private final MysqlColDataTypeConverter mysqlColDataTypeConverter = new MysqlColDataTypeConverter();
    private static final String UNSIGNED = " UNSIGNED";
    private static final String COMMENT = "COMMENT ";
    private final List<AlterSqlConverterDecorator> decorators;

    public MysqlAlterSqlConverter() {
        this.decorators = new ArrayList<>();
    }

    public MysqlAlterSqlConverter(List<AlterSqlConverterDecorator> decorators) {
        this.decorators = decorators;
    }

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return this.mysqlColDataTypeConverter;
    }

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        return generateSql("ADD", alterColumnExpression);
    }

    @Override
    protected Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression) {
        alterColumnExpression.setColOldName(alterColumnExpression.getColumnName());
        return generateSql("CHANGE", alterColumnExpression);
    }

    @Override
    protected Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression) {
        return generateSql("CHANGE", alterColumnExpression);
    }

    @Override
    protected Optional<String> convertToDropColumnSql(AlterColumnExpression alterColumnExpression) {
        return generateDropColumnSql(alterColumnExpression);
    }

    private Optional<String> generateSql(String action, AlterColumnExpression alterColumnExpression) {
        SqlContext sqlContext = new SqlContext.Builder()
                .action(action)
                .schemaName("")
                .tableName(alterColumnExpression.getTableName())
                .oldColumnName(alterColumnExpression.getColOldName())
                .newColumnName(alterColumnExpression.getColumnName())
                .dataTypeString(getFormattedDataType(alterColumnExpression))
                .nullAble(alterColumnExpression.getNullAble())
                .defaultValue("")
                .commentText(alterColumnExpression.getCommentText())
                .build();

        for (AlterSqlConverterDecorator decorator : decorators) {
            sqlContext = decorator.apply(this, alterColumnExpression, sqlContext);
        }

        return Optional.of(generateColumnSql(sqlContext));
    }

    private Optional<String> generateDropColumnSql(AlterColumnExpression alterColumnExpression) {
        SqlContext sqlContext = new SqlContext.Builder()
                .action("DROP")
                .schemaName("")
                .tableName(alterColumnExpression.getTableName())
                .oldColumnName(alterColumnExpression.getColumnName())
                .newColumnName(null)
                .dataTypeString(null)
                .nullAble(null)
                .defaultValue(null)
                .commentText(null)
                .build();

        for (AlterSqlConverterDecorator decorator : decorators) {
            sqlContext = decorator.apply(this, alterColumnExpression, sqlContext);
        }

        String qualifiedTableName = getQualifiedTableName(sqlContext.getSchemaName(), sqlContext.getTableName());
        String formattedColumnName = getFormattedColumnNameOrEmpty(sqlContext.getOldColumnName());
        String sql = String.format("ALTER TABLE %s DROP COLUMN %s;", qualifiedTableName, formattedColumnName).trim();
        return Optional.of(sql);
    }

    @Override
    protected List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions) {
        return new ArrayList<>();
    }

    protected String generateColumnSql(SqlContext sqlContext) {
        final String action = sqlContext.getAction();
        final String schemaName = sqlContext.getSchemaName();
        final String tableName = sqlContext.getTableName();
        final String oldColumnName = sqlContext.getOldColumnName();
        final String newColumnName = sqlContext.getNewColumnName();
        final String dataTypeString = sqlContext.getDataTypeString();
        final String nullAble = sqlContext.getNullAble();
        final String defaultValue = sqlContext.getDefaultValue();
        final String commentText = sqlContext.getCommentText();
        String qualifiedTableName = getQualifiedTableName(schemaName, tableName);
        String formattedOldColumnName = getFormattedColumnNameOrEmpty(oldColumnName);
        String formattedNewColumnName = getFormattedColumnNameOrEmpty(newColumnName);
        String formattedComment = getFormattedComment(commentText);
        return String.format("ALTER TABLE %s %s COLUMN %s %s %s %s %s %s;",
                qualifiedTableName,
                action,
                formattedOldColumnName,
                formattedNewColumnName,
                dataTypeString,
                nullAble,
                defaultValue,
                formattedComment);
    }

    protected String getFormattedDataType(AlterColumnExpression alterColumnExpression) {
        ColDataType colDataType = alterColumnExpression.getColDataType();
        return (alterColumnExpression.isUnsignedFlag())
                ? (getDataTypeString(colDataType) + UNSIGNED)
                : getDataTypeString(colDataType);
    }

    protected String getFormattedComment(String commentText) {
        return StringUtils.isBlank(commentText)
                ? ""
                : (COMMENT + commentText);
    }

    protected String getQualifiedTableName(String schemaName, String tableName) {
        return (StringUtils.isNotEmpty(schemaName))
                ? (schemaName + ".`" + tableName + "`")
                : ("`" + tableName + "`");
    }

    protected String getFormattedColumnNameOrEmpty(String columnName) {
        return (StringUtils.isNotEmpty(columnName)) ? ("`" + columnName + "`") : "";
    }
}
