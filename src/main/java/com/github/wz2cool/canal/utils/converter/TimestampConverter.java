package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.MysqlDataType;

/**
 * 时间戳转换器
 * @author yinpenghai
 **/
public interface TimestampConverter {
    /**
     * 转换时间戳
     * @param mysqlDataType 数据类型
     * @param value         值
     * @return 转换后的值
     */
    String convert(MysqlDataType mysqlDataType, String value);
}
