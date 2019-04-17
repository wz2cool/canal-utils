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

        assertEquals(1, result.size());
    }

    @Test
    public void testRenameColumn() throws JSQLParserException {
        String testSql = "ALTER TABLE `bug` CHANGE COLUMN `newColumn` `newColumn1` INT(11) NULL DEFAULT NULL AFTER `assignTo`";

        Statement statement = CCJSqlParserUtil.parse(testSql);

        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
    }
}
