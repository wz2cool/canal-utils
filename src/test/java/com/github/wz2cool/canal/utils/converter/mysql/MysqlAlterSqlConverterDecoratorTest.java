package com.github.wz2cool.canal.utils.converter.mysql;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author penghai
 */
public class MysqlAlterSqlConverterDecoratorTest {

    private final MysqlAlterSqlConverterDecorator mysqlAlterSqlConverter =  new MysqlAlterSqlConverterDecorator(new MysqlAlterSqlConverter());

    @org.junit.Test
    public void testRenameColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `qa_penghai`.`bond_amount_2` CHANGE COLUMN `publish_date` `publish_date_2` date NULL DEFAULT NULL COMMENT '公告日期'";
        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        String expectSql = "ALTER TABLE `qa_penghai`.`bond_amount_2` CHANGE COLUMN `publish_date` `publish_date_2` date NULL DEFAULT NULL" +
                " COMMENT '公告日期';";
        assertEquals(expectSql, result.get(0));
    }

    @org.junit.Test
    public void testChangeColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student` CHANGE COLUMN `Column 3` `Column 3` INT(11) NULL DEFAULT NULL COMMENT '公告日期'";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 3` `Column 3` INT (11) NULL DEFAULT NULL COMMENT '公告日期';", result.get(0));
    }

    @org.junit.Test
    public void testDropColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tDROP COLUMN `Column 4`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` DROP COLUMN `Column 4`;", result.get(0));
    }

    @Test
    public void testRenameMultiColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tCHANGE COLUMN `Column 5` `Column 1` INT(11) UNSIGNED NULL DEFAULT NULL AFTER `name`,\n" +
                "\tCHANGE COLUMN `Column 4` `Column 2` INT(11) UNSIGNED NULL DEFAULT NULL AFTER `Column 1`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);

        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 5` `Column 1` INT (11) UNSIGNED NULL DEFAULT NULL ;", result.get(0));
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 4` `Column 2` INT (11) UNSIGNED NULL DEFAULT NULL ;", result.get(1));
    }

    @Test
    public void testChangeMultiColumnType() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tCHANGE COLUMN `Column 5` `Column 5` VARCHAR(50) NULL DEFAULT NULL AFTER `name`,\n" +
                "\tCHANGE COLUMN `Column 4` `Column 4` VARCHAR(50) NULL DEFAULT NULL AFTER `Column 5`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 5` `Column 5` VARCHAR (50) NULL DEFAULT NULL ;", result.get(0));
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 4` `Column 4` VARCHAR (50) NULL DEFAULT NULL ;", result.get(1));
    }

    @Test
    public void testChangeColumnType() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tCHANGE COLUMN `Column 4` `Column 4` INT (11) UNSIGNED NULL DEFAULT NULL AFTER `Column 5`;\n";

        List<String> result = mysqlAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 4` `Column 4` INT (11) UNSIGNED NULL DEFAULT NULL ;", result.get(0));
    }
}
