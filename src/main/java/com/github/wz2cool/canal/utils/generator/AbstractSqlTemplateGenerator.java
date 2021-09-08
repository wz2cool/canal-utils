package com.github.wz2cool.canal.utils.generator;

import com.github.wz2cool.canal.utils.converter.BaseAlterSqlConverter;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.helper.DateHelper;
import com.github.wz2cool.canal.utils.model.*;
import com.github.wz2cool.canal.utils.model.exception.NotSupportDataTypeException;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 基础sql模板生成器
 *
 * @author Frank
 */
public abstract class AbstractSqlTemplateGenerator {

    protected abstract IValuePlaceholderConverter getValuePlaceholderConverter();

    protected abstract BaseAlterSqlConverter getAlterSqlConverter();

    public abstract DatabaseDriverType getDatabaseDriverType();

    /**
     * 获取DML sql 模板
     *
     * @param canalRowChange canal 行改动
     * @return sql 模板
     */
    public List<SqlTemplate> listDMLSqlTemplates(final CanalRowChange canalRowChange) {
        if (Boolean.TRUE.equals(canalRowChange.isDdl()) || CollectionUtils.isEmpty(canalRowChange.getRowDataList())) {
            return new ArrayList<>();
        }
        List<SqlTemplate> result = new ArrayList<>();
        if ("insert".equalsIgnoreCase(canalRowChange.getType())) {
            for (CanalRowData canalRowData : canalRowChange.getRowDataList()) {
                Optional<SqlTemplate> insertTemplateOptional = getInsertSqlTemplate(canalRowData);
                insertTemplateOptional.ifPresent(result::add);
            }
        } else if ("delete".equalsIgnoreCase(canalRowChange.getType())) {
            for (CanalRowData canalRowData : canalRowChange.getRowDataList()) {
                Optional<SqlTemplate> insertTemplateOptional = getDeleteSqlTemplate(canalRowData);
                insertTemplateOptional.ifPresent(result::add);
            }
        } else if ("update".equalsIgnoreCase(canalRowChange.getType())) {
            for (CanalRowData canalRowData : canalRowChange.getRowDataList()) {
                Optional<SqlTemplate> insertTemplateOptional = getUpdateSqlTemplate(canalRowData);
                insertTemplateOptional.ifPresent(result::add);
            }
        }
        return result;
    }

    /**
     * 获取DDL sql 模板
     *
     * @param canalRowChange canal 行改动
     * @return sql 模板
     * @throws JSQLParserException jsql 转化异常
     */
    public List<SqlTemplate> listDDLSqlTemplates(final CanalRowChange canalRowChange) throws JSQLParserException {
        List<SqlTemplate> result = new ArrayList<>();
        if (!canalRowChange.isDdl()
                || StringUtils.isEmpty(canalRowChange.getSql())) {
            return result;
        }

        Optional<String> alterSqlOptional = getAlterStatement(canalRowChange);
        if (alterSqlOptional.isPresent()) {
            String alterSql = alterSqlOptional.get();
            List<String> alterSqlList = getAlterSqlConverter().convert(alterSql);
            for (String s : alterSqlList) {
                SqlTemplate sqlTemplate = new SqlTemplate();
                sqlTemplate.setExpression(s);
                result.add(sqlTemplate);
            }
        }
        return result;
    }

    /**
     * 获取插入sql 模板
     *
     * @param canalRowData canal 行数据
     * @return sql 模板
     */
    public Optional<SqlTemplate> getInsertSqlTemplate(final CanalRowData canalRowData) {
        if (CollectionUtils.isEmpty(canalRowData.getColumnList())) {
            return Optional.empty();
        }
        ColumnsParserInfo columnsParserInfo = getColumnsParserInfo(canalRowData.getColumnList());
        String columnString = String.join(", ", columnsParserInfo.getColumnNameList());
        String placeholderString = String.join(", ", columnsParserInfo.getPlaceholderList());
        String sql = String.format("insert into %s(%s) values (%s)", canalRowData.getTable(), columnString, placeholderString);
        SqlTemplate result = new SqlTemplate();
        result.setExpression(sql);
        result.setParams(columnsParserInfo.getValueList().toArray());
        return Optional.of(result);
    }

    /**
     * 获取删除模板
     *
     * @param canalRowData canal 行数据
     * @return sql 模板
     */
    public Optional<SqlTemplate> getDeleteSqlTemplate(final CanalRowData canalRowData) {
        if (CollectionUtils.isEmpty(canalRowData.getColumnList())) {
            return Optional.empty();
        }

        List<CanalColumnData> whereColumns = getWhereColumns(canalRowData.getColumnList());
        ColumnsParserInfo whereColumnsParserInfo = getColumnsParserInfo(whereColumns);
        String appendWhereColumnEqualsString = getAppendColumnEquals(whereColumnsParserInfo, "and");
        String sql = String.format("delete from %s where %s", canalRowData.getTable(), appendWhereColumnEqualsString);
        SqlTemplate result = new SqlTemplate();
        result.setExpression(sql);
        result.setParams(whereColumnsParserInfo.getValueList().toArray());
        return Optional.of(result);
    }

    /**
     * 获取更新sql 模板
     *
     * @param canalRowData canal 行数据
     * @return sql 模板
     */
    public Optional<SqlTemplate> getUpdateSqlTemplate(final CanalRowData canalRowData) {
        // 为了防止洗数据，我们全量更新
        List<CanalColumnData> updateColumns = canalRowData.getColumnList();
        if (CollectionUtils.isEmpty(updateColumns)) {
            return Optional.empty();
        }

        ColumnsParserInfo updateColumnsParserInfo = getColumnsParserInfo(updateColumns);
        String appendUpdateColumnEqualsString = getAppendColumnEquals(updateColumnsParserInfo, ", ");
        List<CanalColumnData> whereColumns = getWhereColumns(canalRowData.getColumnList());

        String sql = String.format("update %s set %s", canalRowData.getTable(), appendUpdateColumnEqualsString);
        List<Object> params = new ArrayList<>(updateColumnsParserInfo.getValueList());
        // append where sql if has
        if (!whereColumns.isEmpty()) {
            ColumnsParserInfo whereColumnsParserInfo = getColumnsParserInfo(whereColumns);
            String appendWhereColumnEqualsString = getAppendColumnEquals(whereColumnsParserInfo, "and");
            sql = String.format("%s where %s", sql, appendWhereColumnEqualsString);
            params.addAll(whereColumnsParserInfo.getValueList());
        }

        SqlTemplate result = new SqlTemplate();
        result.setExpression(sql);
        result.setParams(params.toArray());
        return Optional.of(result);
    }

    private Optional<String> getAlterStatement(final CanalRowChange canalRowChange) {
        // 目前我们只要alter 语句 比如create 语句我们会扔掉
        if ("alter".equalsIgnoreCase(canalRowChange.getType())) {
            return Optional.ofNullable(canalRowChange.getSql());
        } else {
            return Optional.empty();
        }
    }

    private String getAppendColumnEquals(final ColumnsParserInfo columnsParserInfo, final String separator) {
        List<String> columnNames = columnsParserInfo.getColumnNameList();
        List<String> placeholders = columnsParserInfo.getPlaceholderList();
        int size = columnNames.size();
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < size; i++) {
            String columnName = columnNames.get(i);
            String placeholder = placeholders.get(i);
            sql.append(" ").append(columnName).append(" = ").append(placeholder).append(" ");
            if (i != size - 1) {
                sql.append(separator);
            }
        }
        return sql.toString();
    }

    private ColumnsParserInfo getColumnsParserInfo(final List<CanalColumnData> canalColumnDataList) {
        List<String> columnNames = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        for (CanalColumnData canalColumnData : canalColumnDataList) {
            String useColumnName = getUseColumnName(canalColumnData.getName());
            columnNames.add(useColumnName);
            ValuePlaceholder valuePlaceholder = getValuePlaceholder(canalColumnData.getMysqlType(), canalColumnData.getValue());
            placeholders.add(valuePlaceholder.getPlaceholder());

            Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(canalColumnData.getMysqlType());
            if (mysqlDataTypeOptional.isPresent()
                    && Objects.nonNull(valuePlaceholder.getValue())) {
                MysqlDataType mysqlDataType = mysqlDataTypeOptional.get();
                if (mysqlDataType == MysqlDataType.TIMESTAMP || mysqlDataType == MysqlDataType.DATETIME) {
                    String cleanDateTime = DateHelper.getCleanDateTime(valuePlaceholder.getValue().toString());
                    values.add(cleanDateTime);
                } else if (mysqlDataType == MysqlDataType.DATE) {
                    String cleanDate = DateHelper.getCleanDate(valuePlaceholder.getValue().toString());
                    values.add(cleanDate);
                } else {
                    values.add(valuePlaceholder.getValue());
                }
            } else {
                values.add(valuePlaceholder.getValue());
            }
        }
        return new ColumnsParserInfo(columnNames, placeholders, values);
    }

    private List<CanalColumnData> getWhereColumns(final List<CanalColumnData> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return new ArrayList<>();
        }
        // ignore null value 和 改动的列，
        List<CanalColumnData> useColumnList = columns.stream()
                .filter(x -> Objects.nonNull(x.getValue()) && Boolean.FALSE.equals(x.isUpdated())).collect(Collectors.toList());
        List<CanalColumnData> pkColumnList = getPkColumnList(useColumnList);
        if (!CollectionUtils.isEmpty(pkColumnList)) {
            return pkColumnList;
        }
        return getIsUpdatedColumns(useColumnList, false);
    }

    private List<CanalColumnData> getIsUpdatedColumns(final List<CanalColumnData> canalColumnDataList, final boolean updated) {
        List<CanalColumnData> result = new ArrayList<>();
        for (CanalColumnData column : canalColumnDataList) {
            if (column.isUpdated() == updated) {
                result.add(column);
            }
        }
        return result;
    }

    private List<CanalColumnData> getPkColumnList(final List<CanalColumnData> canalColumnDataList) {
        if (CollectionUtils.isEmpty(canalColumnDataList)) {
            return new ArrayList<>();
        }

        return canalColumnDataList.stream().filter(CanalColumnData::isKey).collect(Collectors.toList());
    }

    private ValuePlaceholder getValuePlaceholder(String mysqlTypeString, String value) {
        Optional<MysqlDataType> mysqlDataTypeOptional = MysqlDataType.getDataType(mysqlTypeString);
        if (mysqlDataTypeOptional.isPresent()) {
            return getValuePlaceholderConverter().convert(mysqlDataTypeOptional.get(), value);
        } else {
            String errorMsg = String.format("[getValuePlaceholder] Cannot convert data type: %s", mysqlTypeString);
            throw new NotSupportDataTypeException(errorMsg);
        }
    }

    // 为了兼容 "_" 开头的列
    protected String getUseColumnName(String columnName) {
        return columnName;
    }
}
