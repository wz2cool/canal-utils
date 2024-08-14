package com.github.wz2cool.canal.utils.converter.mysql;

import net.sf.jsqlparser.JSQLParserException;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author penghai
 */
public class MysqlEnhancedAlterSqlConverterTest {

    private final MysqlAlterSqlConverter mysqlEnhancedAlterSqlConverter = new MysqlEnhancedAlterSqlConverter();

    @org.junit.Test
    public void testRenameColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `qa_penghai`.`bond_amount_2` CHANGE COLUMN `publish_date` `publish_date_2` date NULL DEFAULT NULL COMMENT '公告日期'";
        List<String> result = mysqlEnhancedAlterSqlConverter.convert(testSql);
        String expectSql = "ALTER TABLE `qa_penghai`.`bond_amount_2` CHANGE COLUMN `publish_date` `publish_date_2` date NULL DEFAULT NULL" +
                " COMMENT '公告日期';";
        assertEquals(expectSql, result.get(0));
    }

    @org.junit.Test
    public void testChangeColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student` CHANGE COLUMN `Column 3` `Column 3` INT(11) NULL DEFAULT NULL COMMENT '公告日期'";

        List<String> result = mysqlEnhancedAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` CHANGE COLUMN `Column 3` `Column 3` INT (11) NULL DEFAULT NULL COMMENT '公告日期';", result.get(0));
    }

    @org.junit.Test
    public void testDropColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `student`\n" +
                "\tDROP COLUMN `Column 4`;\n";

        List<String> result = mysqlEnhancedAlterSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE `student` DROP COLUMN `Column 4`;", result.get(0));
    }

}
