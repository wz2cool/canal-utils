package com.github.wz2cool.canal.utils.converter.oracle;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class OracleAlterColumnSqlConverterTest {
    private final OracleAlterColumnSqlConverter oracleAlterColumnSqlConverter = new OracleAlterColumnSqlConverter();

    @Test
    public void testAddColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `bug` ADD COLUMN `newColumn` INT NULL";
        Statement statement = CCJSqlParserUtil.parse(testSql);

        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        String expectSql = "ALTER TABLE bug ADD (newColumn NUMBER (10, 0))";
        assertEquals(expectSql, result.get(0));
    }

    @Test
    public void testAddMultiColumns() throws JSQLParserException {
        String testSql = "ALTER TABLE `test`\n" +
                "\tADD COLUMN `col1` INT NOT NULL AFTER `date_test`,\n" +
                "\tADD COLUMN `col2` INT NOT NULL AFTER `col1`;";

        Statement statement = CCJSqlParserUtil.parse(testSql);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        assertEquals("ALTER TABLE test ADD (col1 NUMBER (10, 0))", result.get(0));
        assertEquals("ALTER TABLE test ADD (col2 NUMBER (10, 0))", result.get(1));
    }

    @Test
    public void testRenameColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `bug` CHANGE COLUMN `newColumn` `newColumn1` INT(11) NULL DEFAULT NULL";
        Statement statement = CCJSqlParserUtil.parse(testSql);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        assertEquals("ALTER TABLE bug RENAME COLUMN newColumn to newColumn1", result.get(0));
    }

    @Test
    public void testRenameMultiColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `test`\n" +
                "\tCHANGE COLUMN `col1` `col11` INT(11) NOT NULL AFTER `date_test`,\n" +
                "\tCHANGE COLUMN `col2` `col22` INT(11) NOT NULL AFTER `col11`";
        Statement statement = CCJSqlParserUtil.parse(testSql);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);

        assertEquals("ALTER TABLE test RENAME COLUMN col1 to col11", result.get(0));
        assertEquals("ALTER TABLE test RENAME COLUMN col2 to col22", result.get(1));
    }

    @Test
    public void testDropColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `bug` DROP COLUMN `newColumn1`";
        Statement statement = CCJSqlParserUtil.parse(testSql);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        assertEquals("ALTER TABLE bug DROP COLUMN newColumn1", result.get(0));
    }
}
