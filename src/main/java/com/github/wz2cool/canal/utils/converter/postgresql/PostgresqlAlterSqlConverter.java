package com.github.wz2cool.canal.utils.converter.postgresql;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import com.github.wz2cool.canal.utils.converter.mysql.decorator.AlterSqlConverterDecorator;
import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.SqlContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL alter sql converter.
 *
 * @author YinPengHai
 **/
public class PostgresqlAlterSqlConverter extends BaseAlterSqlConverter {
    private final PostgresqlColDataTypeConverter postgresqlColDataTypeConverter = new PostgresqlColDataTypeConverter();

    private final List<AlterSqlConverterDecorator> decorators;

    public PostgresqlAlterSqlConverter() {
        this.decorators = new ArrayList<>();
    }

    public PostgresqlAlterSqlConverter(List<AlterSqlConverterDecorator> decorators) {
        this.decorators = decorators;
    }

    @Override
    protected IColDataTypeConverter getColDataTypeConverter() {
        return this.postgresqlColDataTypeConverter;
    }

    @Override
    protected Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression) {
        return generateSql("ADD", alterColumnExpression);
    }

    @Override
    protected Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression) {
        return generateSql("ALTER", alterColumnExpression);
    }

    @Override
    protected Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression) {
        return generateSql("RENAME", alterColumnExpression);
    }

    @Override
    protected Optional<String> convertToDropColumnSql(AlterColumnExpression alterColumnExpression) {
        return generateSql("DROP", alterColumnExpression);
    }

    @Override
    protected List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions) {
        return new ArrayList<>();
    }

    private Optional<String> generateSql(String action, AlterColumnExpression alterColumnExpression) {
        SqlContext sqlContext = new SqlContext.Builder()
                .action( action)
                .schemaName("")
                .tableName(alterColumnExpression.getTableName())
                .oldColumnName(alterColumnExpression.getColOldName())
                .newColumnName(alterColumnExpression.getColumnName())
                .dataTypeString(alterColumnExpression.getColDataType() != null ? getDataTypeString(alterColumnExpression.getColDataType()) : "")
                .nullAble(alterColumnExpression.getNullAble())
                .defaultValue(alterColumnExpression.getDefaultValue())
                .commentText(alterColumnExpression.getCommentText())
                .build();
        for (AlterSqlConverterDecorator decorator : decorators) {
            sqlContext = decorator.apply(this, alterColumnExpression, sqlContext);
        }

        return Optional.of(generateColumnSql(sqlContext));
    }

    protected String generateColumnSql(SqlContext sqlContext) {
        final String action = sqlContext.getAction();
        final String schemaName = sqlContext.getSchemaName();
        final String tableName = sqlContext.getTableName();
        final String oldColumnName = sqlContext.getOldColumnName();
        final String newColumnName = sqlContext.getNewColumnName();
        final String dataTypeString = sqlContext.getDataTypeString();
        final String qualifiedTableName = getQualifiedTableName(schemaName, tableName);
        if ("ADD".equals(action)) {
            return String.format("ALTER TABLE %s %s %s %s",
                    qualifiedTableName,
                    action,
                    newColumnName,
                    dataTypeString);
        } else if ("ALTER".equals(action)) {
            return alterColumnType(action, newColumnName, dataTypeString, qualifiedTableName);
        } else if ("RENAME".equals(action)) {
            // mysql可以一行sql完成字段名和类型的修改，postgresql需要分开，我们用__SPLIT__拼接
            String renameSql = String.format("ALTER TABLE %s %s COLUMN %s TO %s",
                    qualifiedTableName,
                    action,
                    oldColumnName,
                    newColumnName);
            String alterColumnTypeSql = alterColumnType(action, newColumnName, dataTypeString, qualifiedTableName);
            return String.format("%s__SPLIT__%s", renameSql, alterColumnTypeSql);
        } else if ("DROP".equals(action)) {
            return String.format("ALTER TABLE %s %s COLUMN %s",
                    qualifiedTableName,
                    action,
                    newColumnName);
        }
        return "";
    }

    private String alterColumnType(String action, String newColumnName, String dataTypeString, String qualifiedTableName) {
        return String.format("ALTER TABLE %s %s COLUMN %s TYPE %s USING %s::%s",
                qualifiedTableName,
                action,
                newColumnName,
                dataTypeString,
                newColumnName,
                dataTypeString);
    }

    protected String getQualifiedTableName(String schemaName, String tableName) {
        return (StringUtils.isNotEmpty(schemaName))
                ? (schemaName + ".\"" + tableName + "\"")
                : ("\"" + tableName + "\"");
    }
}
