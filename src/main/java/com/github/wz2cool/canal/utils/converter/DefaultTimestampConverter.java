package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.helper.DateHelper;
import com.github.wz2cool.canal.utils.model.MysqlDataType;

/**
 * 默认时间转换器
 * @author yinpenghai
 **/
public class DefaultTimestampConverter implements TimestampConverter {
    @Override
    public String convert(MysqlDataType mysqlDataType, String value) {
        return DateHelper.getCleanDateTime(value);
    }
}
