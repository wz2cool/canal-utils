package com.github.wz2cool.canal.utils.converter.oracle;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class OracleAlterColumnSqlConverterTest {
    private final OracleAlterColumnSqlConverter oracleAlterColumnSqlConverter = new OracleAlterColumnSqlConverter();

    @Test
    public void testAddColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `bug` ADD COLUMN `newColumn` INT NULL";
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);
        String expectSql = "ALTER TABLE bug ADD (newColumn NUMBER (11, 0))";
        assertEquals(expectSql, result.get(0));
    }

    @Test
    public void testAddMultiColumns() throws JSQLParserException {
        String testSql = "ALTER TABLE `test`\n" +
                "\tADD COLUMN `col1` INT NOT NULL AFTER `date_test`,\n" +
                "\tADD COLUMN `col2` INT NOT NULL AFTER `col1`;";

        
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE test ADD (col1 NUMBER (11, 0))", result.get(0));
        assertEquals("ALTER TABLE test ADD (col2 NUMBER (11, 0))", result.get(1));
    }

    @Test
    public void testRenameColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `bug` CHANGE COLUMN `newColumn` `newColumn1` INT(11) NULL DEFAULT NULL";
        
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE bug RENAME COLUMN newColumn to newColumn1", result.get(0));
    }

    @Test
    public void testRenameMultiColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `test`\n" +
                "\tCHANGE COLUMN `col1` `col11` INT(11) NOT NULL AFTER `date_test`,\n" +
                "\tCHANGE COLUMN `col2` `col22` INT(11) NOT NULL AFTER `col11`";
        
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);

        assertEquals("ALTER TABLE test RENAME COLUMN col1 to col11", result.get(0));
        assertEquals("ALTER TABLE test RENAME COLUMN col2 to col22", result.get(1));
    }

    @Test
    public void testDropColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `bug` DROP COLUMN `newColumn1`";
        
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE bug DROP COLUMN newColumn1", result.get(0));
    }

    @Test
    public void testDropMultiColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `users`\n" +
                "\tDROP COLUMN `col1`,\n" +
                "\tDROP COLUMN `col2`;";
        
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE users DROP COLUMN col1", result.get(0));
        assertEquals("ALTER TABLE users DROP COLUMN col2", result.get(1));
    }

    @Test
    public void testChangeMultiColumnType() throws JSQLParserException {
        String testSql = "ALTER TABLE `users`\n" +
                "\tCHANGE COLUMN `col1` `col1` BIGINT NULL DEFAULT NULL AFTER `password`,\n" +
                "\tCHANGE COLUMN `col2` `col2` BIGINT NULL DEFAULT NULL AFTER `col1`;";
        
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE users MODIFY (col1 NUMBER (20, 0))", result.get(0));
        assertEquals("ALTER TABLE users MODIFY (col2 NUMBER (20, 0))", result.get(1));
    }

    @Test
    public void testDropMultiColumns() throws JSQLParserException {
        String testSql = "ALTER TABLE `test`\n" +
                "\tDROP COLUMN `col11`,\n" +
                "\tDROP COLUMN `col22`;";
        
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);
        assertEquals("ALTER TABLE test DROP COLUMN col11", result.get(0));
        assertEquals("ALTER TABLE test DROP COLUMN col22", result.get(1));
    }

    @Test
    public void testChangeColumnType() throws JSQLParserException {
        String testSql = "ALTER TABLE `test`\n" +
                "\tCHANGE COLUMN `Column1` `Column1` INT NOT NULL AFTER `date_test`";
        
        List<String> result = oracleAlterColumnSqlConverter.convert(testSql);

        assertEquals("ALTER TABLE test MODIFY (Column1 NUMBER (11, 0))", result.get(0));
    }
}
