package com.github.wz2cool.canal.utils.converter.mysql;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MysqlAlterSqlConverterTest {

    private final MysqlAlterSqlConverter mysqlAlterSqlConverter = new MysqlAlterSqlConverter();

    @Test
    public void testAddColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `test`.`student` ADD COLUMN `Column 3` INT(11) UNSIGNED NULL AFTER `name`;";
        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        String expectSql = "ALTER TABLE `student` ADD COLUMN  `Column 3` INT (11) UNSIGNED NULL  ;";
        assertEquals(expectSql, result.get(0));
    }

    @Test
    public void testAddMultiColumns() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tADD COLUMN `Column 4` INT NULL AFTER `Column 3`,\n" +
                "\tADD COLUMN `Column 5` INT NULL AFTER `Column 4`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` ADD COLUMN  `Column 4` INT NULL  ;", result.get(0));
        assertEquals("ALTER TABLE `student` ADD COLUMN  `Column 5` INT NULL  ;", result.get(1));
    }

    @Test
    public void testRenameColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tCHANGE COLUMN `Column 3` `Column 4` INT(11) NULL DEFAULT NULL AFTER `name`;";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 3` `Column 4` INT (11) NULL  ;", result.get(0));
    }

    @Test
    public void testRenameMultiColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tCHANGE COLUMN `Column 5` `Column 1` INT(11) UNSIGNED NULL DEFAULT NULL AFTER `name`,\n" +
                "\tCHANGE COLUMN `Column 4` `Column 2` INT(11) UNSIGNED NULL DEFAULT NULL AFTER `Column 1`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);

        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 5` `Column 1` INT (11) UNSIGNED NULL  ;", result.get(0));
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 4` `Column 2` INT (11) UNSIGNED NULL  ;", result.get(1));
    }

    @Test
    public void testDropColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tDROP COLUMN `Column 4`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` DROP COLUMN `Column 4`;", result.get(0));
    }

    @Test
    public void testDropMultiColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tDROP COLUMN `Column 5`,\n" +
                "\tDROP COLUMN `Column 4`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` DROP COLUMN `Column 5`;", result.get(0));
        assertEquals("ALTER TABLE `student` DROP COLUMN `Column 4`;", result.get(1));
    }

    @Test
    public void testChangeColumnType() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tCHANGE COLUMN `Column 4` `Column 4` INT (11) UNSIGNED NULL DEFAULT NULL AFTER `Column 5`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 4` `Column 4` INT (11) UNSIGNED NULL  ;", result.get(0));
    }

    @Test
    public void testChangeMultiColumnType() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tCHANGE COLUMN `Column 5` `Column 5` VARCHAR(50) NULL DEFAULT NULL AFTER `name`,\n" +
                "\tCHANGE COLUMN `Column 4` `Column 4` VARCHAR(50) NULL DEFAULT NULL AFTER `Column 5`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 5` `Column 5` VARCHAR (50) NULL  ;", result.get(0));
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 4` `Column 4` VARCHAR (50) NULL  ;", result.get(1));
    }

    @Test
    public void testDropKey() throws JSQLParserException {
//        String testSql = "alter table com_related_party\n" +
//                "    drop key uk_business";
        String testSql = "alter table com_related_party\n" +
                "    drop index uk_business";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
    }

    @Test
    public void testDropKeyWithSemicolon() throws JSQLParserException {
        String testSql = "alter table com_related_party drop key uk_business;";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
    }

    @Test
    public void testDropMultipleKeys() throws JSQLParserException {
        String testSql = "alter table com_related_party\n" +
                "    drop key uk_business,\n" +
                "    drop key idx_name";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(2, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `idx_name`;", result.get(1));
    }

    @Test
    public void testMixedDropKeyAndColumn() throws JSQLParserException {
        String testSql = "alter table com_related_party\n" +
                "    drop key uk_business,\n" +
                "    drop column old_column";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(2, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
        assertEquals("ALTER TABLE `com_related_party` DROP COLUMN `old_column`;", result.get(1));
    }

    @Test
    public void testAddConstraintUnique() throws JSQLParserException {
        String testSql = "alter table com_related_party add constraint uk_business unique (com_uni_code, report_date, related_party_code, relation_type, related_party_full_name)";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` ADD CONSTRAINT `uk_business` unique (com_uni_code, report_date, related_party_code, relation_type, related_party_full_name);", result.get(0));
    }

    @Test
    public void testAddConstraintPrimaryKey() throws JSQLParserException {
        String testSql = "alter table test_table add constraint pk_test primary key (id)";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `test_table` ADD CONSTRAINT `pk_test` primary key (id);", result.get(0));
    }

    @Test
    public void testMixedAddConstraintAndDropKey() throws JSQLParserException {
        String testSql = "alter table com_related_party\n" +
                "    drop key old_uk_business,\n" +
                "    add constraint uk_business unique (com_uni_code, report_date)";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(2, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `old_uk_business`;", result.get(0));
        assertEquals("ALTER TABLE `com_related_party` ADD CONSTRAINT `uk_business` unique (com_uni_code, report_date);", result.get(1));
    }

    /**
     * 验证原始问题的SQL能够正常解析和转换
     */
    @Test
    public void testOriginalProblemSql() throws JSQLParserException {
        // 这是原始报错的SQL
        String problemSql = "alter table com_related_party\n    drop key uk_business";
        
        // 应该不再抛出ParseException
        List<String> result = mysqlAlterSqlConverter.convert(problemSql);
        
        // 验证结果
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
    }

    /**
     * 验证原始ADD CONSTRAINT问题的SQL能够正常解析和转换
     */
    @Test
    public void testOriginalAddConstraintSql() throws JSQLParserException {
        // 这是用户提到的ADD CONSTRAINT SQL
        String addConstraintSql = "alter table com_related_party add constraint uk_business unique (com_uni_code, report_date, related_party_code, relation_type, related_party_full_name)";
        
        // 应该能够正常解析和转换
        List<String> result = mysqlAlterSqlConverter.convert(addConstraintSql);
        
        // 验证结果
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` ADD CONSTRAINT `uk_business` unique (com_uni_code, report_date, related_party_code, relation_type, related_party_full_name);", result.get(0));
    }

    @Test
    public void testDropIndex() throws JSQLParserException {
        String testSql = "alter table  com_related_party\n" +
                "    drop index uk_business";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
    }

    @Test
    public void testDropIndexWithSemicolon() throws JSQLParserException {
        String testSql = "alter table com_related_party drop index uk_business;";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
    }

    @Test
    public void testDropMultipleIndexes() throws JSQLParserException {
        String testSql = "alter table com_related_party\n" +
                "    drop index uk_business,\n" +
                "    drop index idx_name";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(2, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `idx_name`;", result.get(1));
    }

    @Test
    public void testMixedDropIndexAndColumn() throws JSQLParserException {
        String testSql = "alter table com_related_party\n" +
                "    drop index uk_business,\n" +
                "    drop column old_column";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(2, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
        assertEquals("ALTER TABLE `com_related_party` DROP COLUMN `old_column`;", result.get(1));
    }

    @Test
    public void testMixedDropKeyAndDropIndex() throws JSQLParserException {
        String testSql = "alter table com_related_party\n" +
                "    drop key uk_business,\n" +
                "    drop index idx_name";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(2, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `uk_business`;", result.get(0));
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `idx_name`;", result.get(1));
    }

    @Test
    public void testStandaloneDropIndexSimple() throws JSQLParserException {
        String testSql = "drop index com_related_party_publish_date_index on com_related_party";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `com_related_party_publish_date_index`;", result.get(0));
    }

    @Test
    public void testStandaloneDropIndexWithComment() throws JSQLParserException {
        String testSql = "/* ApplicationName=DataGrip 2024.3.5 */ drop index com_related_party_publish_date_index on com_related_party";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `com_related_party_publish_date_index`;", result.get(0));
    }

    @Test
    public void testStandaloneDropIndexWithBackticks() throws JSQLParserException {
        String testSql = "drop index `idx_test_index` on `com_related_party`";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `com_related_party` DROP INDEX `idx_test_index`;", result.get(0));
    }

    @Test
    public void testStandaloneDropIndexCaseInsensitive() throws JSQLParserException {
        String testSql = "DROP INDEX IDX_UPPER_CASE ON COM_RELATED_PARTY";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals(1, result.size());
        assertEquals("ALTER TABLE `COM_RELATED_PARTY` DROP INDEX `IDX_UPPER_CASE`;", result.get(0));
    }
}
