package com.github.wz2cool.canal.utils.helper;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.sql.Timestamp;
import java.text.ParseException;

/**
 * 通用工具类
 *
 * @author Frank
 */
@SuppressWarnings("squid:S1166")
public final class DateHelper {

    private DateHelper() {
    }

    public static final String DATE_TIME_SORT = "yyyyMMddHHmmss";
    public static final String DATE_TIME_TMP = "yyyyMMddHHmmssSSS";
    public static final String DATE_TIME_SEC = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_TIME_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DATE_TIME_YMD = "yyyy/MM/dd HH:mm:ss";
    public static final String DATE_TIME_YMS = "yyyy/MM/dd HH:mm:ss.SSS";
    public static final String DATE_DD = "yyyy-MM-dd";
    public static final String DATE_YMD = "yyyy/MM/dd";


    /**
     * 转化成时间戳
     *
     * @param dateTimeString 全时间字符串
     * @return 时间戳
     * @throws ParseException 转化异常
     */
    public static Timestamp getTimestamp(String dateTimeString) throws ParseException {
        long time = DateUtils.parseDate(dateTimeString,
                DATE_TIME_SEC,
                DATE_TIME_SSS,
                DATE_TIME_YMD,
                DATE_TIME_YMS,
                DATE_TIME_TMP,
                DATE_TIME_SORT,
                DATE_DD,
                DATE_YMD).getTime();
        return new Timestamp(time);
    }

    /**
     * 获取干净的时间 (删除毫秒)
     *
     * @param dateTimeString 全时间字符串
     * @return 干净的时间 (删除毫秒)
     */
    public static String getCleanDateTime(String dateTimeString) {
        try {
            Timestamp timestamp = getTimestamp(dateTimeString);
            return DateFormatUtils.format(timestamp, DATE_TIME_SEC);
        } catch (Exception ex) {
            return dateTimeString;
        }
    }

    /**
     * 获取干净的时间 (删除毫秒)
     *
     * @param dateTimeString 全时间字符串
     * @return 干净的时间 (删除毫秒)
     */
    public static String getCleanDate(String dateTimeString) {
        try {
            Timestamp timestamp = getTimestamp(dateTimeString);
            return DateFormatUtils.format(timestamp, DATE_DD);
        } catch (Exception ex) {
            return dateTimeString;
        }
    }
}


