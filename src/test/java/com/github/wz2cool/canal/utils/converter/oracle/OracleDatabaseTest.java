package com.github.wz2cool.canal.utils.converter.oracle;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.List;

public class OracleDatabaseTest {
    private static String USERNAMR = "test";
    private static String PASSWORD = "innodealing";
    private static String DRVIER = "oracle.jdbc.driver.OracleDriver";
    private static String URL = "jdbc:oracle:thin:@192.168.2.111:1521:XE";
    private static String TABLE_NAME = "MY_TEST";

    private final OracleAlterColumnSqlConverter oracleAlterColumnSqlConverter = new OracleAlterColumnSqlConverter();

    @Before
    public void initTable() throws SQLException, ClassNotFoundException {
        tryDropTestTable();
        createTestTable();
    }

    @Test
    public void addBITColumn() throws JSQLParserException, SQLException, ClassNotFoundException {
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BIT NULL AFTER `assignTo`;", TABLE_NAME);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(msqlAddColumn);
        List<String> result = oracleAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }
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
        String createTableSQL = "CREATE TABLE " + TABLE_NAME + "("
                + "USER_ID NUMBER(5) NOT NULL, PRIMARY KEY (USER_ID) "
                + ")";
        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            statement.execute(createTableSQL);
            System.out.println("create table success");
            System.out.println("create table success");
        }
    }

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(DRVIER);
        return DriverManager.getConnection(URL, USERNAMR, PASSWORD);
    }
}
