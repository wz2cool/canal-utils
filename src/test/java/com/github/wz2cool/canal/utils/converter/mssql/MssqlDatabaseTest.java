package com.github.wz2cool.canal.utils.converter.mssql;

import com.github.wz2cool.canal.utils.converter.AlterColumnSqlConverterBase;
import com.github.wz2cool.canal.utils.converter.DatabaseTestBase;
import com.github.wz2cool.canal.utils.converter.IValuePlaceholderConverter;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MssqlDatabaseTest extends DatabaseTestBase {
    private final static String TABLE_NAME = "MY_TEST";
    private final MssqlAlterColumnSqlConverter mssqlAlterColumnSqlConverter = new MssqlAlterColumnSqlConverter();
    private final MssqlValuePlaceholderConverter mssqlValuePlaceholderConverter = new MssqlValuePlaceholderConverter();

    @Override
    protected String getTestTableName() {
        return TABLE_NAME;
    }

    @Override
    protected AlterColumnSqlConverterBase getAlterColumnSqlConverter() {
        return this.mssqlAlterColumnSqlConverter;
    }

    @Override
    protected IValuePlaceholderConverter getValuePlaceholderConverter() {
        return this.mssqlValuePlaceholderConverter;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        String username = "sa";
        String password = "Innodealing@123";
        String URL = "jdbc:sqlserver://192.168.2.112:1433;DatabaseName=test";
        return DriverManager.getConnection(URL, username, password);
    }

    @Before
    public void initTable() throws SQLException, ClassNotFoundException {
        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
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
    public void addDATEColumn() throws SQLException, JSQLParserException, ParseException {
        super.addDATEColumn();
    }

    @Test
    public void addDATETIMEColumn() throws SQLException, JSQLParserException, ParseException {
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
    public void addTIMEColumn() throws SQLException, JSQLParserException, ParseException {
        super.addTIMEColumn();
    }

    @Test
    public void addTIMESTAMPColumn() throws SQLException, JSQLParserException, ParseException {
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
        String createTableSQL = String.format("CREATE TABLE %s (USER_ID INT NOT NULL, PRIMARY KEY (USER_ID) )",
                TABLE_NAME);

        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            statement.execute(createTableSQL);
            System.out.println("create table success");
        }
    }
}
