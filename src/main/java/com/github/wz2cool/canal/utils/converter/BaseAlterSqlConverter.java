package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import com.github.wz2cool.canal.utils.model.EnhancedAlterOperation;
import com.github.wz2cool.canal.utils.model.exception.NotSupportDataTypeException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Frank
 */
public abstract class BaseAlterSqlConverter {
    // 用于匹配 DROP KEY 语法的正则表达式
    private static final Pattern DROP_KEY_PATTERN = Pattern.compile(
            "(?i)\\s*drop\\s+key\\s+([^\\s,;]+)", Pattern.CASE_INSENSITIVE);
    
    // 用于匹配 ADD CONSTRAINT 语法的正则表达式
    private static final Pattern ADD_CONSTRAINT_PATTERN = Pattern.compile(
            "(?i)\\s*add\\s+constraint\\s+([^\\s]+)\\s+(unique|primary\\s+key|foreign\\s+key)\\s*\\(([^)]+)\\)", 
            Pattern.CASE_INSENSITIVE);
    
    public List<String> convert(String mysqlAlterSqlInput) throws JSQLParserException {
        String mysqlAlterSql = cleanMysqlAlterSql(mysqlAlterSqlInput);

        List<String> result = new ArrayList<>();
        
        // 提取表名
        String tableName = extractTableName(mysqlAlterSql);
        
        // 检查是否包含索引操作语法
        List<AlterColumnExpression> indexExpressions = extractIndexOperations(mysqlAlterSql, tableName);
        
        // 如果有索引操作，先处理它们
        if (!indexExpressions.isEmpty()) {
            List<String> dropIndexSqlList = indexExpressions
                    .stream()
                    .filter(x -> x.getOperation() == EnhancedAlterOperation.DROP_INDEX)
                    .map(this::convertToDropIndexSql)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            
            List<String> addIndexSqlList = indexExpressions
                    .stream()
                    .filter(x -> x.getOperation() == EnhancedAlterOperation.ADD_INDEX)
                    .map(this::convertToAddIndexSql)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            
            result.addAll(dropIndexSqlList);
            result.addAll(addIndexSqlList);
            
            // 从SQL中移除索引操作
            mysqlAlterSql = removeIndexOperationsFromSql(mysqlAlterSql);
            
            // 如果只有索引操作，直接返回结果
            if (mysqlAlterSql.trim().matches("(?i)alter\\s+table\\s+\\S+\\s*;?\\s*")) {
                return result;
            }
        }
        
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(mysqlAlterSql);
        if (!(statement instanceof Alter)) {
            return result;
        }

        Alter mysqlAlter = (Alter) statement;
        List<AlterColumnExpression> alterColumnExpressions = getAlterColumnExpressions(mysqlAlter);
        for (AlterColumnExpression alterColumnExpression : alterColumnExpressions) {
            alterColumnExpression.setSchemaName(mysqlAlter.getTable().getSchemaName());
        }
        List<String> addColumnSqlList = alterColumnExpressions
                .stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.ADD_COLUMN)
                .map(this::convertToAddColumnSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        List<String> changeColumnTypeSqlList = alterColumnExpressions
                .stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.CHANGE_COLUMN_TYPE)
                .map(this::convertToChangeColumnTypeSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        List<String> renameColumnSqlList = alterColumnExpressions
                .stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.RENAME_COLUMN)
                .map(this::convertToRenameColumnSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        List<String> dropColumnSqlList = alterColumnExpressions
                .stream()
                .filter(x -> x.getOperation() == EnhancedAlterOperation.DROP_COLUMN)
                .map(this::convertToDropColumnSql)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        List<String> otherColumnActionSqlList = convertToOtherColumnActionSqlList(alterColumnExpressions);

        result.addAll(addColumnSqlList);
        result.addAll(changeColumnTypeSqlList);
        result.addAll(renameColumnSqlList);
        result.addAll(dropColumnSqlList);
        result.addAll(otherColumnActionSqlList);
        return result;
    }

    protected abstract IColDataTypeConverter getColDataTypeConverter();

    protected abstract Optional<String> convertToAddColumnSql(AlterColumnExpression alterColumnExpression);

    protected abstract Optional<String> convertToChangeColumnTypeSql(AlterColumnExpression alterColumnExpression);

    protected abstract Optional<String> convertToRenameColumnSql(AlterColumnExpression alterColumnExpression);

    protected abstract Optional<String> convertToDropColumnSql(AlterColumnExpression alterColumnExpression);

    protected abstract List<String> convertToOtherColumnActionSqlList(List<AlterColumnExpression> alterColumnExpressions);
    
    /**
     * 转换 DROP INDEX 操作的 SQL
     * @param alterColumnExpression 包含索引信息的表达式
     * @return 转换后的 SQL
     */
    protected abstract Optional<String> convertToDropIndexSql(AlterColumnExpression alterColumnExpression);
    
    /**
     * 转换 ADD INDEX 操作的 SQL
     * @param alterColumnExpression 包含索引信息的表达式
     * @return 转换后的 SQL
     */
    protected abstract Optional<String> convertToAddIndexSql(AlterColumnExpression alterColumnExpression);

    /**
     * 提取索引操作并创建对应的AlterColumnExpression
     */
    private List<AlterColumnExpression> extractIndexOperations(String mysqlAlterSql, String tableName) {
        List<AlterColumnExpression> result = new ArrayList<>();
        
        if (tableName == null) {
            return result;
        }
        
        // 处理 DROP KEY
        Matcher dropKeyMatcher = DROP_KEY_PATTERN.matcher(mysqlAlterSql);
        while (dropKeyMatcher.find()) {
            String indexName = dropKeyMatcher.group(1);
            AlterColumnExpression expression = new AlterColumnExpression();
            expression.setTableName(cleanText(tableName));
            expression.setColumnName(cleanText(indexName)); // 使用columnName存储索引名
            expression.setOperation(EnhancedAlterOperation.DROP_INDEX);
            result.add(expression);
        }
        
        // 处理 ADD CONSTRAINT
        Matcher addConstraintMatcher = ADD_CONSTRAINT_PATTERN.matcher(mysqlAlterSql);
        while (addConstraintMatcher.find()) {
            String constraintName = addConstraintMatcher.group(1);
            String constraintType = addConstraintMatcher.group(2);
            String columns = addConstraintMatcher.group(3);
            
            AlterColumnExpression expression = new AlterColumnExpression();
            expression.setTableName(cleanText(tableName));
            expression.setColumnName(cleanText(constraintName)); // 使用columnName存储约束名
            expression.setColOldName(constraintType.trim()); // 使用colOldName存储约束类型
            expression.setCommentText(columns.trim()); // 使用commentText存储列信息
            expression.setOperation(EnhancedAlterOperation.ADD_INDEX);
            result.add(expression);
        }
        
        return result;
    }
    
    /**
     * 从 ALTER 语句中提取表名
     */
    private String extractTableName(String mysqlAlterSql) {
        Pattern tablePattern = Pattern.compile("(?i)alter\\s+table\\s+([^\\s]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = tablePattern.matcher(mysqlAlterSql);
        if (matcher.find()) {
            return cleanText(matcher.group(1));
        }
        return null;
    }
    
    /**
     * 从 SQL 中移除索引操作部分
     */
    private String removeIndexOperationsFromSql(String mysqlAlterSql) {
        String result = mysqlAlterSql;
        
        // 移除 DROP KEY
        result = DROP_KEY_PATTERN.matcher(result).replaceAll("");
        
        // 移除 ADD CONSTRAINT
        result = ADD_CONSTRAINT_PATTERN.matcher(result).replaceAll("");
        
        // 更精确的逗号清理逻辑
        // 1. 清理连续的逗号
        result = result.replaceAll(",\\s*,", ",");
        
        // 2. 清理表名后直接跟逗号的情况（如：alter table xxx, drop column yyy）
        result = result.replaceAll("(?i)(alter\\s+table\\s+\\S+)\\s*,\\s*", "$1 ");
        
        // 3. 清理逗号后直接跟分号的情况
        result = result.replaceAll(",\\s*;", ";");
        
        // 4. 清理开头的逗号
        result = result.replaceAll("^\\s*,\\s*", "");
        
        // 5. 清理结尾的逗号
        result = result.replaceAll(",\\s*$", "");
        
        // 6. 规范化空格
        result = result.replaceAll("\\s+", " ").trim();
        
        return result;
    }

    private List<AlterColumnExpression> getAlterColumnExpressions(Alter mysqlAlter) {
        List<AlterColumnExpression> result = new ArrayList<>();
        if (mysqlAlter == null) {
            return result;
        }

        List<AlterExpression> alterExpressions = mysqlAlter.getAlterExpressions();
        List<AlterExpression> alterExpressionsForColumn = new ArrayList<>();
        String tableName = cleanText(mysqlAlter.getTable().getName());
        for (AlterExpression alterExpression : alterExpressions) {
            String optionalSpecifier = alterExpression.getOptionalSpecifier();
            if ("COLUMN".equalsIgnoreCase(optionalSpecifier)
                    || (alterExpression.getColDataTypeList() != null && !alterExpression.getColDataTypeList().isEmpty())
                    || (alterExpression.getOperation() == AlterOperation.DROP && alterExpression.getColumnName() != null)) {
                alterExpressionsForColumn.add(alterExpression);
            }
        }

        if (alterExpressionsForColumn.isEmpty()) {
            return result;
        }

        for (AlterExpression alterExpression : alterExpressionsForColumn) {
            List<AlterColumnExpression> alterColumnExpressions =
                    getAlterColumnExpressions(tableName, alterExpression);
            result.addAll(alterColumnExpressions);
        }

        return result;
    }


    protected String getDataTypeString(final ColDataType colDataType) {
        List<String> argumentsStringList = colDataType.getArgumentsStringList();
        if (argumentsStringList == null || argumentsStringList.isEmpty()) {
            return colDataType.getDataType();
        } else {
            return colDataType.toString();
        }
    }

    private List<AlterColumnExpression> getAlterColumnExpressions(
            final String tableName, final AlterExpression alterExpression) {
        List<AlterColumnExpression> result = new ArrayList<>();
        if (alterExpression == null) {
            return result;
        }

        AlterOperation operation = alterExpression.getOperation();
        String columnName = alterExpression.getColumnName();
        String colOldName = alterExpression.getColumnOldName();
        List<AlterExpression.ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();

        Optional<AlterColumnExpression> dropColumnExpression =
                getDropColumnExpression(operation, tableName, columnName);
        dropColumnExpression.ifPresent(result::add);

        List<AlterColumnExpression> renameColumnExpressions =
                getRenameColumnExpressions(operation, tableName, colOldName, colDataTypeList);
        result.addAll(renameColumnExpressions);

        List<AlterColumnExpression> addColumnExpressions =
                getAddColumnExpressions(operation, tableName, colDataTypeList);
        result.addAll(addColumnExpressions);

        List<AlterColumnExpression> changeColumnTypeExpressions =
                getChangeColumnTypeExpressions(operation, tableName, colOldName, colDataTypeList);
        result.addAll(changeColumnTypeExpressions);
        return result;
    }

    private List<AlterColumnExpression> getAddColumnExpressions(
            final AlterOperation alterOperation,
            final String tableName,
            final List<AlterExpression.ColumnDataType> columnDataTypes) {
        List<AlterColumnExpression> result = new ArrayList<>();
        if (alterOperation != AlterOperation.ADD || columnDataTypes == null || columnDataTypes.isEmpty()) {
            return result;
        }

        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            AlterColumnExpression addColumnExpression = new AlterColumnExpression();
            String columnName = columnDataType.getColumnName();

            ColDataType mysqlColDataType = columnDataType.getColDataType();
            Optional<ColDataType> covColDataTypeOptional = getColDataTypeConverter().convert(mysqlColDataType);
            if (!covColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Add Column] Cannot convert data type: %s",
                        mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }

            final boolean unsignedFlag = columnDataType.getColumnSpecs().stream().anyMatch("unsigned"::equalsIgnoreCase);
            addColumnExpression.setTableName(cleanText(tableName));
            addColumnExpression.setColumnName(cleanText(columnName));
            addColumnExpression.setOperation(EnhancedAlterOperation.ADD_COLUMN);
            addColumnExpression.setColDataType(covColDataTypeOptional.get());
            addColumnExpression.setUnsignedFlag(unsignedFlag);
            // 说明
            List<String> columnSpecs = columnDataType.getColumnSpecs();
            addColumnExpression = setColumnSpecs(addColumnExpression, columnSpecs);
            result.add(addColumnExpression);
        }
        return result;
    }

    private List<AlterColumnExpression> getRenameColumnExpressions(
            final AlterOperation alterOperation,
            final String tableName,
            final String colOldName,
            final List<AlterExpression.ColumnDataType> columnDataTypes
    ) {
        List<AlterColumnExpression> result = new ArrayList<>();
        if (alterOperation != AlterOperation.CHANGE
                || colOldName == null
                || columnDataTypes == null
                || columnDataTypes.isEmpty()) {
            return result;
        }

        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            String columnName = columnDataType.getColumnName();
            if (columnName.equals(colOldName)) {
                // ignore if column name not change.
                continue;
            }

            ColDataType mysqlColDataType = columnDataType.getColDataType();
            Optional<ColDataType> covColDataTypeOptional = getColDataTypeConverter().convert(mysqlColDataType);
            if (!covColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Rename Column] Cannot convert data type: %s",
                        mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }
            final boolean unsignedFlag = columnDataType.getColumnSpecs().stream().anyMatch("unsigned"::equalsIgnoreCase);
            AlterColumnExpression renameColumnExpression = new AlterColumnExpression();
            renameColumnExpression.setTableName(cleanText(tableName));
            renameColumnExpression.setColumnName(cleanText(columnName));
            renameColumnExpression.setColOldName(cleanText(colOldName));
            renameColumnExpression.setOperation(EnhancedAlterOperation.RENAME_COLUMN);
            renameColumnExpression.setColDataType(covColDataTypeOptional.get());
            renameColumnExpression.setUnsignedFlag(unsignedFlag);
            // 说明
            List<String> columnSpecs = columnDataType.getColumnSpecs();
            renameColumnExpression = setColumnSpecs(renameColumnExpression, columnSpecs);
            result.add(renameColumnExpression);
        }
        return result;
    }

    private List<AlterColumnExpression> getChangeColumnTypeExpressions(
            final AlterOperation alterOperation,
            final String tableName,
            final String colOldName,
            final List<AlterExpression.ColumnDataType> columnDataTypes) {
        List<AlterColumnExpression> result = new ArrayList<>();
        // https://stackoverflow.com/questions/14767174/modify-column-vs-change-column
        // MODIFY COLUMN This command does everything CHANGE COLUMN can, but without renaming the column.
        if (!(alterOperation == AlterOperation.CHANGE || alterOperation == AlterOperation.MODIFY)
                || columnDataTypes == null
                || columnDataTypes.isEmpty()) {
            return result;
        }

        for (AlterExpression.ColumnDataType columnDataType : columnDataTypes) {
            AlterColumnExpression changeTypeColumnExpression = new AlterColumnExpression();
            String columnName = columnDataType.getColumnName();
            if (alterOperation == AlterOperation.CHANGE && !columnName.equals(colOldName)) {
                // need check rename
                continue;
            }

            ColDataType mysqlColDataType = columnDataType.getColDataType();
            Optional<ColDataType> covColDataTypeOptional = getColDataTypeConverter().convert(mysqlColDataType);
            if (!covColDataTypeOptional.isPresent()) {
                String errorMsg = String.format("[Change Column Type] Cannot convert data type: %s",
                        mysqlColDataType.getDataType());
                throw new NotSupportDataTypeException(errorMsg);
            }
            final boolean unsignedFlag = columnDataType.getColumnSpecs().stream().anyMatch("unsigned"::equalsIgnoreCase);
            changeTypeColumnExpression.setTableName(cleanText(tableName));
            changeTypeColumnExpression.setColumnName(cleanText(columnName));
            changeTypeColumnExpression.setOperation(EnhancedAlterOperation.CHANGE_COLUMN_TYPE);
            changeTypeColumnExpression.setColDataType(covColDataTypeOptional.get());
            changeTypeColumnExpression.setUnsignedFlag(unsignedFlag);
            List<String> columnSpecs = columnDataType.getColumnSpecs();
            changeTypeColumnExpression = setColumnSpecs(changeTypeColumnExpression, columnSpecs);
            result.add(changeTypeColumnExpression);
        }
        return result;
    }

    private AlterColumnExpression setColumnSpecs(AlterColumnExpression columnExpression, List<String> columnSpecs) {

        AlterColumnExpression useColumnExpression = new AlterColumnExpression();
        useColumnExpression.setOperation(columnExpression.getOperation());
        useColumnExpression.setTableName(columnExpression.getTableName());
        useColumnExpression.setColumnName(columnExpression.getColumnName());
        useColumnExpression.setColOldName(columnExpression.getColOldName());
        useColumnExpression.setColDataType(columnExpression.getColDataType());
        useColumnExpression.setUnsignedFlag(columnExpression.isUnsignedFlag());
        useColumnExpression.setCommentText("");
        useColumnExpression.setNullAble("NULL");
        useColumnExpression.setDefaultValue("");
        for (int i = 0; i < columnSpecs.size(); i++) {
            if ("COMMENT".equalsIgnoreCase(columnSpecs.get(i))) {
                useColumnExpression.setCommentText(columnSpecs.get(i + 1));
            }
            if ("NULL".equalsIgnoreCase(columnSpecs.get(i)) && i != 0 && columnSpecs.get(i - 1).equalsIgnoreCase("NOT")) {
                useColumnExpression.setNullAble("NOT NULL");
            }
            if ("DEFAULT".equalsIgnoreCase(columnSpecs.get(i))) {
                useColumnExpression.setDefaultValue(columnSpecs.get(i + 1));
            }
        }
        return useColumnExpression;
    }


    private Optional<AlterColumnExpression> getDropColumnExpression(
            final AlterOperation alterOperation,
            final String tableName,
            final String columnName
    ) {
        if (alterOperation != AlterOperation.DROP || columnName == null) {
            return Optional.empty();
        }

        AlterColumnExpression dropColumnExpression = new AlterColumnExpression();
        dropColumnExpression.setTableName(cleanText(tableName));
        dropColumnExpression.setColumnName(cleanText(columnName));
        dropColumnExpression.setOperation(EnhancedAlterOperation.DROP_COLUMN);
        return Optional.of(dropColumnExpression);
    }

    private String cleanText(String element) {
        return element.replace("`", "");
    }

    private String cleanMysqlAlterSql(String mysqlAlterSql) {
        return mysqlAlterSql.replace("COLLATE", "")
                .replace("collate", "");
    }
}
