package com.github.wz2cool.canal.utils.converter.oracle;

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
public class OracleDatabaseTest {
    private static String TABLE_NAME = "MY_TEST";

    private final OracleAlterColumnSqlConverter oracleAlterColumnSqlConverter = new OracleAlterColumnSqlConverter();
    private final OracleValuePlaceholderConverter oracleValuePlaceholderConverter = new OracleValuePlaceholderConverter();


    @Before
    public void initTable() throws SQLException, ClassNotFoundException {
        String driver = "oracle.jdbc.driver.OracleDriver";
        Class.forName(driver);
        tryDropTestTable();
        createTestTable();
    }

    @Test
    public void addBITColumn() throws JSQLParserException, SQLException {
        System.out.println("addBITColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BIT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.BIT, "1");
    }

    @Test
    public void addTINYINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addTINYINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TINYINT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TINYINT, "1");
    }

    @Test
    public void addMEDIUMINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addMEDIUMINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` MEDIUMINT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.MEDIUMINT, "1");
    }

    @Test
    public void addSMALLINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addSMALLINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` SMALLINT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.SMALLINT, "1");
    }

    @Test
    public void addINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` INT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.INT, "1");
    }

    @Test
    public void addINTEGERColumn() throws JSQLParserException, SQLException {
        System.out.println("addINTEGERColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` INTEGER NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.INTEGER, "1");
    }

    @Test
    public void addBIGINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addBIGINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BIGINT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.BIGINT, "1");
    }

    @Test
    public void addFLOATColumn() throws JSQLParserException, SQLException {
        System.out.println("addFLOATColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` FLOAT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.FLOAT, "3.33333");
    }

    @Test
    public void addDOUBLEColumn() throws JSQLParserException, SQLException {
        System.out.println("addDOUBLEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DOUBLE NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DOUBLE, "3.33333");
    }

    @Test
    public void addDECIMALColumn() throws JSQLParserException, SQLException {
        System.out.println("addDECIMALColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DECIMAL NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DECIMAL, "3.33333");
    }

    @Test
    public void addNUMERICColumn() {
        // not support
    }

    @Test
    public void addDATEColumn() throws JSQLParserException, SQLException {
        System.out.println("addDATEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DATE NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DATE, "2019-04-20");
    }

    @Test
    public void addDATETIMEColumn() throws JSQLParserException, SQLException {
        System.out.println("addDATETIMEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DATETIME NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DATETIME, "2019-04-20 22:16:12");
    }

    @Test
    public void addTIMESTAMPColumn() throws JSQLParserException, SQLException {
        System.out.println("addTIMESTAMPColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TIMESTAMP NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TIMESTAMP, "2019-04-20 22:16:12");
    }

    @Test
    public void addTIMEColumn() throws JSQLParserException, SQLException {
        System.out.println("addTIMESTAMPColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TIME NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TIME, "10:00:00");
    }

    @Test
    public void addYEARColumn() {
        // not support year.
    }

    @Test
    public void addCHARColumn() throws JSQLParserException, SQLException {
        System.out.println("addCHARColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` CHAR (50) NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.CHAR, "test");
    }

    @Test
    public void addVARCHARColumn() throws JSQLParserException, SQLException {
        System.out.println("addVARCHARColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` VARCHAR (255) NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.VARCHAR, "test");
    }

    @Test
    public void addTINYBLOBColumn() throws JSQLParserException, SQLException {
        System.out.println("addTINYBLOBColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TINYBLOB NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TINYBLOB, "0x32");
    }

    @Test
    public void addTINYTEXTColumn() throws JSQLParserException, SQLException {
        System.out.println("addTINYTEXTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TINYTEXT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TINYTEXT, "test");
    }

    @Test
    public void addTEXTColumn() throws JSQLParserException, SQLException {
        System.out.println("addTEXTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TEXT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TEXT, "test");
    }

    @Test
    public void addMEDIUMTEXTColumn() throws JSQLParserException, SQLException {
        System.out.println("addTEXTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` MEDIUMTEXT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.MEDIUMTEXT, "test");
    }

    @Test
    public void addLONGTEXTColumn() throws JSQLParserException, SQLException {
        System.out.println("addLONGTEXTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` LONGTEXT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.LONGTEXT, "test");
    }

    @Test
    public void addSETColumn() {
        // don't support set type
    }

    @Test
    public void addBLOBColumn() throws JSQLParserException, SQLException {
        System.out.println("addBLOBColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BLOB NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.BLOB, "0x32");
    }

    @Test
    public void addMEDIUMBLOBColumn() throws JSQLParserException, SQLException {
        System.out.println("addBLOBColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` MEDIUMBLOB NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.MEDIUMBLOB, "0x32");
    }

    @Test
    public void addLONGBLOBColumn() throws JSQLParserException, SQLException {
        System.out.println("addLONGBLOBColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` LONGBLOB NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.LONGBLOB, "0x32");
    }

    @Test
    public void addMultiColumns() throws JSQLParserException, SQLException {
        System.out.println("addMultiColumns");
        String msqlAddColumn = String.format("ALTER TABLE %s\n" +
                "\tADD COLUMN `col1` DECIMAL(10,0) NOT NULL AFTER `assignTo`,\n" +
                "\tADD COLUMN `col2` TEXT NOT NULL AFTER `col1`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }
    }

    @Test
    public void changeColumnType() throws SQLException, JSQLParserException {
        System.out.println("addMultiColumns");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` INTEGER NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String mysqlChangeColumnType = String.format("ALTER TABLE `%s`\n" +
                "\tCHANGE COLUMN `col1` `col1` DATETIME NULL DEFAULT NULL AFTER `assignTo`;", TABLE_NAME);
        statement = CCJSqlParserUtil.parse(mysqlChangeColumnType);
        List<String> changeColumnTypeSqls = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : changeColumnTypeSqls) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DATETIME, "2019-04-20 22:16:12");
    }

    @Test
    public void renameColumn() throws SQLException, JSQLParserException {
        System.out.println("addMultiColumns");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col2` INTEGER NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String mysqlChangeColumnType = String.format("ALTER TABLE `%s`\n" +
                "\tCHANGE COLUMN `col2` `col1` INT(11) NULL DEFAULT NULL AFTER `assignTo`;", TABLE_NAME);
        statement = CCJSqlParserUtil.parse(mysqlChangeColumnType);
        List<String> changeColumnTypeSqls = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : changeColumnTypeSqls) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.INT, "1");
    }

    private synchronized void executeAlterSql(String sql) throws SQLException {
        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            // execute the SQL statement
            System.out.println(String.format("sql: %s", sql));
            statement.execute(sql);
            System.out.println("execute success");
        }
    }

    private synchronized void insertData(MysqlDataType mysqlDataType, String value) throws SQLException {
        try (Connection dbConnection = getConnection()) {
            ValuePlaceholder valuePlaceholder = oracleValuePlaceholderConverter.convert(mysqlDataType, value);
            String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)",
                    TABLE_NAME, 1, valuePlaceholder.getPlaceholder());

            try (PreparedStatement statement = dbConnection.prepareStatement(insertSql)) {
                System.out.println(String.format("[%s] insertSql: %s", mysqlDataType.getText(), insertSql));
                statement.setObject(1, valuePlaceholder.getValue());
                statement.execute();
                System.out.println("execute success");
            }
        }
    }

    private void tryDropTestTable() throws SQLException {
        String dropTableSql = String.format("DROP TABLE %s", TABLE_NAME);
        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            // execute the SQL statement
            statement.execute(dropTableSql);
            System.out.println("Drop table success");
        } catch (SQLSyntaxErrorException ex) {
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

    private synchronized Connection getConnection() throws SQLException {
        String username = "test";
        String password = "innodealing";
        String URL = "jdbc:oracle:thin:@192.168.2.116:1521:XE";
        return DriverManager.getConnection(URL, username, password);
    }
}
