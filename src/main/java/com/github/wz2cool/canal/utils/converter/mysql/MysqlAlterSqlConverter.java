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
 * MySQL alter SQL converter.
 * Handles ALTER operations with optional decorators.
 *
 * @autor penghai
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

    @Override
    protected List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions) {
        return new ArrayList<>();
    }

    @Override
    protected Optional<String> convertToDropIndexSql(AlterColumnExpression alterColumnExpression) {
        return generateDropIndexSql(alterColumnExpression);
    }

    @Override
    protected Optional<String> convertToAddIndexSql(AlterColumnExpression alterColumnExpression) {
        return generateAddIndexSql(alterColumnExpression);
    }

    private Optional<String> generateSql(String action, AlterColumnExpression alterColumnExpression) {
        SqlContext sqlContext = new SqlContext(
                action,
                "",
                alterColumnExpression.getTableName(),
                alterColumnExpression.getColOldName(),
                alterColumnExpression.getColumnName(),
                getFormattedDataType(alterColumnExpression),
                alterColumnExpression.getNullAble(),
                "",
                alterColumnExpression.getCommentText()
        );

        for (AlterSqlConverterDecorator decorator : decorators) {
            sqlContext = decorator.apply(this, alterColumnExpression, sqlContext);
        }

        return Optional.of(generateColumnSql(
                sqlContext.action,
                sqlContext.schemaName,
                sqlContext.tableName,
                sqlContext.oldColumnName,
                sqlContext.newColumnName,
                sqlContext.dataTypeString,
                sqlContext.nullAble,
                sqlContext.defaultValue,
                sqlContext.commentText
        ));
    }

    private Optional<String> generateDropColumnSql(AlterColumnExpression alterColumnExpression) {
        SqlContext sqlContext = new SqlContext(
                "DROP",
                "",
                alterColumnExpression.getTableName(),
                alterColumnExpression.getColumnName(),
                null,
                null,
                null,
                null,
                null
        );

        for (AlterSqlConverterDecorator decorator : decorators) {
            sqlContext = decorator.apply(this, alterColumnExpression, sqlContext);
        }

        String qualifiedTableName = getQualifiedTableName(sqlContext.schemaName, sqlContext.tableName);
        String formattedColumnName = getFormattedColumnNameOrEmpty(sqlContext.oldColumnName);
        String sql = String.format("ALTER TABLE %s DROP COLUMN %s;", qualifiedTableName, formattedColumnName).trim();
        return Optional.of(sql);
    }

    private Optional<String> generateDropIndexSql(AlterColumnExpression alterColumnExpression) {
        SqlContext sqlContext = new SqlContext(
                "DROP INDEX",
                "",
                alterColumnExpression.getTableName(),
                alterColumnExpression.getColumnName(),
                null,
                null,
                null,
                "",
                ""
        );

        for (AlterSqlConverterDecorator decorator : decorators) {
            sqlContext = decorator.apply(this, alterColumnExpression, sqlContext);
        }

        String qualifiedTableName = getQualifiedTableName(sqlContext.schemaName, sqlContext.tableName);
        String formattedIndexName = getFormattedColumnNameOrEmpty(sqlContext.oldColumnName);
        String sql = String.format("ALTER TABLE %s DROP INDEX %s;", qualifiedTableName, formattedIndexName).trim();
        return Optional.of(sql);
    }

    private Optional<String> generateAddIndexSql(AlterColumnExpression alterColumnExpression) {
        SqlContext sqlContext = new SqlContext(
                "ADD CONSTRAINT",
                "",
                alterColumnExpression.getTableName(),
                alterColumnExpression.getColumnName(),
                alterColumnExpression.getColOldName(),
                null,
                null,
                "",
                alterColumnExpression.getCommentText()
        );

        for (AlterSqlConverterDecorator decorator : decorators) {
            sqlContext = decorator.apply(this, alterColumnExpression, sqlContext);
        }

        String qualifiedTableName = getQualifiedTableName(sqlContext.schemaName, sqlContext.tableName);
        String formattedConstraintName = getFormattedColumnNameOrEmpty(sqlContext.oldColumnName);
        String constraintType = sqlContext.newColumnName;
        String columns = sqlContext.commentText;
        
        String sql = String.format("ALTER TABLE %s ADD CONSTRAINT %s %s (%s);", 
                qualifiedTableName, formattedConstraintName, constraintType, columns).trim();
        return Optional.of(sql);
    }

    protected String generateColumnSql(String action, String schemaName, String tableName, String oldColumnName, String newColumnName,
                                       String dataTypeString,
                                       String nullAble, String defaultValue, String commentText) {
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
