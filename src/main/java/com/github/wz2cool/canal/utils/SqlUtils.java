package com.github.wz2cool.canal.utils;

import com.github.wz2cool.canal.utils.model.*;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * sql 工具类
 *
 * @author Frank
 */
public class SqlUtils {

    private SqlUtils() {
        // empty constructor
    }

    public static CanalRowChange getRowChange(final FlatMessage flatMessage) {
        CanalRowChange rowChange = new CanalRowChange();
        rowChange.setId(flatMessage.getId());
        rowChange.setDdl(flatMessage.getIsDdl());
        rowChange.setEs(flatMessage.getEs());
        rowChange.setType(flatMessage.getType());
        rowChange.setDatabase(flatMessage.getDatabase());
        rowChange.setTable(flatMessage.getTable());
        rowChange.setSql(flatMessage.getSql());
        if (CollectionUtils.isEmpty(flatMessage.getData())) {
            return rowChange;
        }

        Map<String, String> mysqlTypeMap = flatMessage.getMysqlType();
        List<String> pkNames = CollectionUtils.isEmpty(flatMessage.getPkNames()) ? new ArrayList<>() : flatMessage.getPkNames();
        List<CanalRowData> canalRowDataList = new ArrayList<>();
        for (int i = 0; i < flatMessage.getData().size(); i++) {
            Map<String, String> rowData = flatMessage.getData().get(i);
            Map<String, String> oldRowData = getOldRowData(flatMessage.getOld(), i);
            CanalRowData canalRowData = new CanalRowData();
            canalRowData.setDatabase(flatMessage.getDatabase());
            canalRowData.setTable(flatMessage.getTable());
            List<CanalColumnData> canalColumnDataList = new ArrayList<>();
            for (Map.Entry<String, String> colValueEntry : rowData.entrySet()) {
                CanalColumnData canalColumnData = new CanalColumnData();
                String column = colValueEntry.getKey();
                String value = colValueEntry.getValue();
                String oldValue = oldRowData.get(column);
                String mysqlType = mysqlTypeMap.get(column);
                boolean isKey = pkNames.contains(column);
                boolean updated = oldRowData.containsKey(column);
                canalColumnData.setName(column);
                canalColumnData.setKey(isKey);
                canalColumnData.setValue(value);
                canalColumnData.setOldValue(oldValue);
                canalColumnData.setMysqlType(mysqlType);
                canalColumnData.setUpdated(updated);
                canalColumnDataList.add(canalColumnData);
            }
            canalRowData.setColumnList(canalColumnDataList);
            canalRowDataList.add(canalRowData);
        }
        rowChange.setRowDataList(canalRowDataList);
        return rowChange;
    }

    private static Map<String, String> getOldRowData(final List<Map<String, String>> oldList, final int index) {
        if (CollectionUtils.isEmpty(oldList) || oldList.size() <= index) {
            return new HashMap<>();
        }
        return oldList.get(index);
    }
}
