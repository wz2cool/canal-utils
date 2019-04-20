package com.github.wz2cool.canal.utils.converter.oracle;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.List;

public class OracleDatabaseTest {
    private static String TABLE_NAME = "MY_TEST";

    private final OracleAlterColumnSqlConverter oracleAlterColumnSqlConverter = new OracleAlterColumnSqlConverter();

    @Before
    public void initTable() throws SQLException, ClassNotFoundException {
        tryDropTestTable();
        createTestTable();
    }

    @Test
    public void addBITColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addBITColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BIT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 1);
        executeAlterSql(insertSql);
    }

    @Test
    public void addTINYINTColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addTINYINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TINYINT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 1);
        executeAlterSql(insertSql);
    }

    @Test
    public void addMEDIUMINTColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addMEDIUMINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` MEDIUMINT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 1);
        executeAlterSql(insertSql);
    }

    @Test
    public void addSMALLINTColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addSMALLINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` SMALLINT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 1);
        executeAlterSql(insertSql);
    }

    @Test
    public void addINTColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` INT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 1);
        executeAlterSql(insertSql);
    }

    @Test
    public void addINTEGERColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addINTEGERColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` INTEGER NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 1);
        executeAlterSql(insertSql);
    }

    @Test
    public void addBIGINTColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addBIGINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BIGINT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 1000000);
        executeAlterSql(insertSql);
    }

    @Test
    public void addFLOATColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addFLOATColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` FLOAT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 3.33333);
        executeAlterSql(insertSql);
    }

    @Test
    public void addDOUBLEColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addDOUBLEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DOUBLE NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 3.33333);
        executeAlterSql(insertSql);
    }

    @Test
    public void addDECIMALColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addDECIMALColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DECIMAL NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)", TABLE_NAME, 1, 3.33333);
        executeAlterSql(insertSql);
    }

    @Test
    public void addDATEColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addDATEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DATE NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }
        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, to_date('%s', 'yyyy-mm-dd'))",
                TABLE_NAME, 1, "2019-04-20");
        executeAlterSql(insertSql);

    }

    @Test
    public void addDATETIMEColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addDATETIMEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DATETIME NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }
        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, to_date('%s', 'yyyy-mm-dd hh24:mi:ss'))",
                TABLE_NAME, 1, "2019-04-20 22:16:12");
        executeAlterSql(insertSql);
    }

    @Test
    public void addTIMESTAMPColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addTIMESTAMPColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TIMESTAMP NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }
        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, to_date('%s', 'yyyy-mm-dd hh24:mi:ss'))",
                TABLE_NAME, 1, "2019-04-20 22:16:12");
        executeAlterSql(insertSql);
    }

    @Test
    public void addTIMEColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        System.out.println("addTIMESTAMPColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TIME NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }
        String insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, to_date('%s', 'hh24:mi:ss'))",
                TABLE_NAME, 1, "10:00:00");
        executeAlterSql(insertSql);
    }

    @Test
    public void addYEARColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        // not support year.
    }

    private void executeAlterSql(String sql) throws SQLException, ClassNotFoundException {
        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            // execute the SQL statement
            System.out.println(String.format("sql: %s", sql));
            statement.execute(sql);
            System.out.println("execute success");
        }
    }

    private void tryDropTestTable() throws SQLException, ClassNotFoundException {
        String dropTableSql = String.format("DROP TABLE %s", TABLE_NAME);
        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            // execute the SQL statement
            statement.execute(dropTableSql);
            System.out.println("Drop table success");
        } catch (SQLSyntaxErrorException ex) {
            ex.printStackTrace();
        }
    }

    private void createTestTable() throws SQLException, ClassNotFoundException {
        String createTableSQL = String.format("CREATE TABLE %s (USER_ID NUMBER(5) NOT NULL, PRIMARY KEY (USER_ID) )",
                TABLE_NAME);

        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            statement.execute(createTableSQL);
            System.out.println("create table success");
        }
    }

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        String driver = "oracle.jdbc.driver.OracleDriver";
        Class.forName(driver);
        String username = "test";
        String password = "innodealing";
        String URL = "jdbc:oracle:thin:@192.168.2.111:1521:XE";
        return DriverManager.getConnection(URL, username, password);
    }
}
