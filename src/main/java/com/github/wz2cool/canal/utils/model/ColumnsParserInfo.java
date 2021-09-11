package com.github.wz2cool.canal.utils.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 列转换信息
 *
 * @author Frank
 */
public class ColumnsParserInfo {

    private List<String> columnNameList;
    private List<String> placeholderList;
    private List<Object> valueList;

    public List<String> getColumnNameList() {
        return columnNameList == null ? null : new ArrayList<>(columnNameList);
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList == null ? null : new ArrayList<>(columnNameList);
    }

    public List<String> getPlaceholderList() {
        return placeholderList == null ? null : new ArrayList<>(placeholderList);
    }

    public void setPlaceholderList(List<String> placeholderList) {
        this.placeholderList = placeholderList == null ? null : new ArrayList<>(placeholderList);
    }

    public List<Object> getValueList() {
        return valueList == null ? null : new ArrayList<>(valueList);
    }

    public void setValueList(List<Object> valueList) {
        this.valueList = valueList == null ? null : new ArrayList<>(valueList);
    }

    /**
     * 列转换信息
     *
     * @param columnNameList  列明列表
     * @param placeholderList 占位符列表
     * @param valueList       值列表
     */
    public ColumnsParserInfo(final List<String> columnNameList, final List<String> placeholderList, final List<Object> valueList) {
        this.columnNameList = new ArrayList<>(columnNameList);
        this.placeholderList = new ArrayList<>(placeholderList);
        this.valueList = new ArrayList<>(valueList);
    }
}
