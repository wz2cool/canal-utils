package com.github.wz2cool.canal.utils.converter.mysql;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MysqlCreateIndexConverterTest {

    private final MysqlAlterSqlConverter mysqlAlterSqlConverter = new MysqlAlterSqlConverter();

    @Test
    public void testCreateIndexWithComment() throws JSQLParserException {
        String sql = "/* ApplicationName=DataGrip 2024.3.5 */ create index com_related_party_com_uni_code_index on com_related_party (com_uni_code)";
        
        List<String> result = mysqlAlterSqlConverter.convert(sql);
        
        assertEquals(1, result.size());
        assertEquals("CREATE INDEX `com_related_party_com_uni_code_index` ON `com_related_party` (com_uni_code);", result.get(0));
    }

    @Test
    public void testCreateIndexSimple() throws JSQLParserException {
        String sql = "create index idx_name on table_name (column1)";
        
        List<String> result = mysqlAlterSqlConverter.convert(sql);
        
        assertEquals(1, result.size());
        assertEquals("CREATE INDEX `idx_name` ON `table_name` (column1);", result.get(0));
    }

    @Test
    public void testCreateIndexMultipleColumns() throws JSQLParserException {
        String sql = "CREATE INDEX idx_multi ON test_table (col1, col2, col3)";
        
        List<String> result = mysqlAlterSqlConverter.convert(sql);
        
        assertEquals(1, result.size());
        assertEquals("CREATE INDEX `idx_multi` ON `test_table` (col1, col2, col3);", result.get(0));
    }

    @Test
    public void testCreateIndexWithBackticks() throws JSQLParserException {
        String sql = "create index `idx_test` on `test_table` (`col1`)";
        
        List<String> result = mysqlAlterSqlConverter.convert(sql);
        
        assertEquals(1, result.size());
        assertEquals("CREATE INDEX `idx_test` ON `test_table` (`col1`);", result.get(0));
    }

    @Test
    public void testCreateIndexCaseInsensitive() throws JSQLParserException {
        String sql = "CREATE INDEX IDX_UPPER ON TABLE_NAME (COLUMN_NAME)";
        
        List<String> result = mysqlAlterSqlConverter.convert(sql);
        
        assertEquals(1, result.size());
        assertEquals("CREATE INDEX `IDX_UPPER` ON `TABLE_NAME` (COLUMN_NAME);", result.get(0));
    }

    @Test
    public void testCreateIndexWithSpaces() throws JSQLParserException {
        String sql = "   CREATE   INDEX   idx_space   ON   table_space   (  column_space  )  ";
        
        List<String> result = mysqlAlterSqlConverter.convert(sql);
        
        assertEquals(1, result.size());
        assertEquals("CREATE INDEX `idx_space` ON `table_space` (column_space);", result.get(0));
    }

    @Test
    public void testUserRequestedSyntax() throws JSQLParserException {
        // User requested specific syntax
        String sql = "/* ApplicationName=DataGrip 2024.3.5 */ create index com_related_party_com_uni_code_index on com_related_party (com_uni_code)";
        
        List<String> result = mysqlAlterSqlConverter.convert(sql);
        
        assertEquals(1, result.size());
        assertEquals("CREATE INDEX `com_related_party_com_uni_code_index` ON `com_related_party` (com_uni_code);", result.get(0));
    }

    @Test
    public void testNonCreateIndexStatement() throws JSQLParserException {
        String sql = "ALTER TABLE test_table ADD COLUMN new_col VARCHAR(255)";
        
        List<String> result = mysqlAlterSqlConverter.convert(sql);
        
        // This is not a CREATE INDEX operation, should be other type of operation
        assertEquals(1, result.size());
        // Ensure it's not a CREATE INDEX statement
        assertEquals("ALTER TABLE `test_table` ADD COLUMN  `new_col` VARCHAR (255) NULL  ;", result.get(0));
    }
} 