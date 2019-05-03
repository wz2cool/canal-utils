package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.converter.AlterColumnSqlConverterBase;
import com.github.wz2cool.canal.utils.converter.DatabaseTestBase;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OracleDatabaseTest extends DatabaseTestBase {
    private static String TABLE_NAME = "MY_TEST";

    private final OracleAlterColumnSqlConverter oracleAlterColumnSqlConverter = new OracleAlterColumnSqlConverter();
    private final OracleValuePlaceholderConverter oracleValuePlaceholderConverter = new OracleValuePlaceholderConverter();

    @Override
    protected String getTestTableName() {
        return TABLE_NAME;
    }

    @Override
    protected AlterColumnSqlConverterBase getAlterColumnSqlConverter() {
        return this.oracleAlterColumnSqlConverter;
    }

    @Override
    protected IValuePlaceholderConverter getValuePlaceholderConverter() {
        return this.oracleValuePlaceholderConverter;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        String username = "test";
        String password = "innodealing";
        String URL = "jdbc:oracle:thin:@192.168.2.116:1521:XE";
        return DriverManager.getConnection(URL, username, password);
    }

    @Before
    public void initTable() throws SQLException, ClassNotFoundException {
        String driver = "oracle.jdbc.driver.OracleDriver";
        Class.forName(driver);
        tryDropTestTable();
        createTestTable();
    }

    @Test
    public void addBITColumn() throws JSQLParserException, SQLException {
        super.addBITColumn();
    }

    @Test
    public void addTINYINTColumn() throws JSQLParserException, SQLException {
        super.addTINYINTColumn();
    }

    @Test
    public void addBIGINTColumn() throws SQLException, JSQLParserException {
        super.addBIGINTColumn();
    }

    @Test
    public void addBLOBColumn() throws SQLException, JSQLParserException {
        super.addBLOBColumn();
    }

    @Test
    public void addCHARColumn() throws SQLException, JSQLParserException {
        super.addCHARColumn();
    }

    @Test
    public void addDATEColumn() throws SQLException, JSQLParserException {
        super.addDATEColumn();
    }

    @Test
    public void addDATETIMEColumn() throws SQLException, JSQLParserException {
        super.addDATETIMEColumn();
    }

    @Test
    public void addDECIMALColumn() throws SQLException, JSQLParserException {
        super.addDECIMALColumn();
    }

    @Test
    public void addDOUBLEColumn() throws SQLException, JSQLParserException {
        super.addDOUBLEColumn();
    }

    @Test
    public void addFLOATColumn() throws SQLException, JSQLParserException {
        super.addFLOATColumn();
    }

    @Test
    public void addINTColumn() throws SQLException, JSQLParserException {
        super.addINTColumn();
    }

    @Test
    public void addINTEGERColumn() throws SQLException, JSQLParserException {
        super.addINTEGERColumn();
    }

    @Test
    public void addLONGBLOBColumn() throws SQLException, JSQLParserException {
        super.addLONGBLOBColumn();
    }

    @Test
    public void addLONGTEXTColumn() throws SQLException, JSQLParserException {
        super.addLONGTEXTColumn();
    }

    @Test
    public void addMEDIUMBLOBColumn() throws SQLException, JSQLParserException {
        super.addMEDIUMBLOBColumn();
    }

    @Test
    public void addMEDIUMINTColumn() throws SQLException, JSQLParserException {
        super.addMEDIUMINTColumn();
    }

    @Test
    public void addMEDIUMTEXTColumn() throws SQLException, JSQLParserException {
        super.addMEDIUMTEXTColumn();
    }

    @Test
    public void addSMALLINTColumn() throws SQLException, JSQLParserException {
        super.addSMALLINTColumn();
    }

    @Test
    public void addTEXTColumn() throws SQLException, JSQLParserException {
        super.addTEXTColumn();
    }

    @Test
    public void addTIMEColumn() throws SQLException, JSQLParserException {
        super.addTIMEColumn();
    }

    @Test
    public void addTIMESTAMPColumn() throws SQLException, JSQLParserException {
        super.addTIMESTAMPColumn();
    }

    @Test
    public void addTINYBLOBColumn() throws SQLException, JSQLParserException {
        super.addTINYBLOBColumn();
    }

    @Test
    public void addTINYTEXTColumn() throws SQLException, JSQLParserException {
        super.addTINYTEXTColumn();
    }

    @Test
    public void addVARCHARColumn() throws SQLException, JSQLParserException {
        super.addVARCHARColumn();
    }

    @Test
    public void addMultiColumns() throws SQLException, JSQLParserException {
        super.addMultiColumns();
    }

    @Test
    public void changeColumnType() throws SQLException, JSQLParserException {
        super.changeColumnType();
    }

    @Test
    public void renameColumn() throws SQLException, JSQLParserException {
        super.renameColumn();
    }

    @Test
    public void dropColumn() throws SQLException, JSQLParserException {
        super.dropColumn();
    }

    private void tryDropTestTable() {
        String dropTableSql = String.format("DROP TABLE %s", TABLE_NAME);
        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            // execute the SQL statement
            statement.execute(dropTableSql);
            System.out.println("Drop table success");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void createTestTable() throws SQLException {
        String createTableSQL = String.format("CREATE TABLE %s (USER_ID NUMBER(5) NOT NULL, PRIMARY KEY (USER_ID) )",
                TABLE_NAME);

        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            statement.execute(createTableSQL);
            System.out.println("create table success");
        }
    }
}
