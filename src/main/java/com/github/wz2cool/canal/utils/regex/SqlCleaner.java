package com.github.wz2cool.canal.utils.regex;

/**
 * SQL 清理工具类
 * 提供 SQL 语句的清理和格式化功能
 * 
 * @author YinPengHai
 */
public class SqlCleaner {
    
    // 私有构造函数，防止实例化
    private SqlCleaner() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }
    
    /**
     * 清理 SQL 中的逗号问题
     * 处理移除索引操作后可能出现的各种逗号问题
     * 
     * @param sql 需要清理的 SQL 语句
     * @return 清理后的 SQL 语句
     */
    public static String cleanCommas(String sql) {
        String result = sql;
        
        // 1. 清理连续的逗号 (,, -> ,)
        result = cleanConsecutiveCommas(result);
        
        // 2. 清理表名后直接跟逗号的情况 (alter table xxx, -> alter table xxx )
        result = cleanCommaAfterTableName(result);
        
        // 3. 清理逗号后直接跟分号的情况 (, ; -> ;)
        result = cleanCommaBeforeSemicolon(result);
        
        // 4. 清理开头的逗号 (, xxx -> xxx)
        result = cleanLeadingComma(result);
        
        // 5. 清理结尾的逗号 (xxx, -> xxx)
        result = cleanTrailingComma(result);
        
        // 6. 规范化空格
        result = normalizeWhitespace(result);
        
        return result;
    }
    
    /**
     * 清理连续的逗号
     * 例如: "a,, b" -> "a, b"
     */
    private static String cleanConsecutiveCommas(String sql) {
        return sql.replaceAll(",\\s*,", ",");
    }
    
    /**
     * 清理表名后直接跟逗号的情况
     * 例如: "alter table test, drop column col" -> "alter table test drop column col"
     */
    private static String cleanCommaAfterTableName(String sql) {
        return sql.replaceAll("(?i)(alter\\s+table\\s+\\S+)\\s*,\\s*", "$1 ");
    }
    
    /**
     * 清理逗号后直接跟分号的情况
     * 例如: "alter table test add column col1 int, ;" -> "alter table test add column col1 int;"
     */
    private static String cleanCommaBeforeSemicolon(String sql) {
        return sql.replaceAll(",\\s*;", ";");
    }
    
    /**
     * 清理开头的逗号
     * 例如: ", drop column col" -> "drop column col"
     */
    private static String cleanLeadingComma(String sql) {
        return sql.replaceAll("^\\s*,\\s*", "");
    }
    
    /**
     * 清理结尾的逗号
     * 例如: "alter table test add column col1 int," -> "alter table test add column col1 int"
     */
    private static String cleanTrailingComma(String sql) {
        return sql.replaceAll(",\\s*$", "");
    }
    
    /**
     * 规范化空格
     * 将多个连续空格替换为单个空格，并去除首尾空格
     */
    private static String normalizeWhitespace(String sql) {
        return sql.replaceAll("\\s+", " ").trim();
    }
    
    /**
     * 清理 MySQL ALTER 语句中的特殊关键字
     * 移除一些可能导致解析问题的关键字
     * 
     * @param sql 原始 SQL 语句
     * @return 清理后的 SQL 语句
     */
    public static String cleanMysqlAlterSql(String sql) {
        return sql.replace("COLLATE", "")
                  .replace("collate", "");
    }
    
    /**
     * 清理文本中的反引号
     * MySQL 中的表名和列名可能包含反引号，需要清理
     * 
     * @param text 包含反引号的文本
     * @return 清理后的文本
     */
    public static String cleanBackticks(String text) {
        return text.replace("`", "");
    }
} 