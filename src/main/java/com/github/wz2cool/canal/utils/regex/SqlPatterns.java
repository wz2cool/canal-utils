package com.github.wz2cool.canal.utils.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL 正则表达式工具类
 * 提供常用的 SQL 语句匹配和处理功能
 * 
 * @author YinPengHai
 */
public class SqlPatterns {
    
    /**
     * DROP KEY 语法匹配
     * 匹配: drop key index_name
     */
    private static final Pattern DROP_KEY_PATTERN = Pattern.compile(
            "\\s*drop\\s+key\\s+([^\\s,;]+)", 
            Pattern.CASE_INSENSITIVE
    );
    
    /**
     * DROP INDEX 语法匹配
     * 匹配: drop index index_name
     */
    private static final Pattern DROP_INDEX_PATTERN = Pattern.compile(
            "\\s*drop\\s+index\\s+([^\\s,;]+)", 
            Pattern.CASE_INSENSITIVE
    );
    
    /**
     * ADD CONSTRAINT 语法匹配
     * 匹配: add constraint constraint_name unique|primary key|foreign key (columns)
     */
    private static final Pattern ADD_CONSTRAINT_PATTERN = Pattern.compile(
            "\\s*add\\s+constraint\\s+([^\\s]+)\\s+(unique|primary\\s+key|foreign\\s+key)\\s*\\(([^)]+)\\)", 
            Pattern.CASE_INSENSITIVE
    );
    
    /**
     * ALTER TABLE 表名提取
     * 匹配: alter table table_name
     */
    private static final Pattern ALTER_TABLE_PATTERN = Pattern.compile(
            "alter\\s+table\\s+([^\\s]+)", 
            Pattern.CASE_INSENSITIVE
    );
    
    /**
     * 检查是否只剩下表名的 ALTER 语句
     * 匹配: alter table table_name 或 alter table table_name;
     */
    private static final Pattern ONLY_TABLE_NAME_PATTERN = Pattern.compile(
            "alter\\s+table\\s+\\S+\\s*;?\\s*", 
            Pattern.CASE_INSENSITIVE
    );
    
    // 私有构造函数，防止实例化
    private SqlPatterns() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }
    
    /**
     * 获取 DROP KEY 的匹配器
     * @param sql SQL 语句
     * @return Matcher 对象
     */
    public static Matcher getDropKeyMatcher(String sql) {
        return DROP_KEY_PATTERN.matcher(sql);
    }
    
    /**
     * 获取 DROP INDEX 的匹配器
     * @param sql SQL 语句
     * @return Matcher 对象
     */
    public static Matcher getDropIndexMatcher(String sql) {
        return DROP_INDEX_PATTERN.matcher(sql);
    }
    
    /**
     * 获取 ADD CONSTRAINT 的匹配器
     * @param sql SQL 语句
     * @return Matcher 对象
     */
    public static Matcher getAddConstraintMatcher(String sql) {
        return ADD_CONSTRAINT_PATTERN.matcher(sql);
    }
    
    /**
     * 从 SQL 中提取表名
     * @param sql ALTER TABLE 语句
     * @return 表名，如果未找到返回 null
     */
    public static String extractTableName(String sql) {
        Matcher matcher = ALTER_TABLE_PATTERN.matcher(sql);
        return matcher.find() ? matcher.group(1) : null;
    }
    
    /**
     * 检查 SQL 是否只包含表名（即所有操作都已被移除）
     * @param sql 清理后的 SQL 语句
     * @return true 如果只剩下表名
     */
    public static boolean isOnlyTableName(String sql) {
        return ONLY_TABLE_NAME_PATTERN.matcher(sql.trim()).matches();
    }
    
    /**
     * 移除 DROP KEY 操作
     * @param sql 原始 SQL
     * @return 移除 DROP KEY 后的 SQL
     */
    public static String removeDropKey(String sql) {
        return DROP_KEY_PATTERN.matcher(sql).replaceAll("");
    }
    
    /**
     * 移除 DROP INDEX 操作
     * @param sql 原始 SQL
     * @return 移除 DROP INDEX 后的 SQL
     */
    public static String removeDropIndex(String sql) {
        return DROP_INDEX_PATTERN.matcher(sql).replaceAll("");
    }
    
    /**
     * 移除 ADD CONSTRAINT 操作
     * @param sql 原始 SQL
     * @return 移除 ADD CONSTRAINT 后的 SQL
     */
    public static String removeAddConstraint(String sql) {
        return ADD_CONSTRAINT_PATTERN.matcher(sql).replaceAll("");
    }
} 