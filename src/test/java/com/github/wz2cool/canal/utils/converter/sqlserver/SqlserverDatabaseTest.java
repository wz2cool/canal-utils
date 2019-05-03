package com.github.wz2cool.canal.utils.converter.sqlserver;

import org.junit.Test;

import java.sql.*;

public class SqlserverDatabaseTest {
    private static String TABLE_NAME = "MY_TEST";

    public SqlserverDatabaseTest() throws ClassNotFoundException, SQLException {
        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        Class.forName(driver);
        tryDropTestTable();
        createTestTable();
    }

    @Test
    public void test() {

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

    private synchronized Connection getConnection() throws SQLException {
        String username = "sa";
        String password = "Innodealing@123";
        String URL = "jdbc:sqlserver://192.168.2.117:1433;DatabaseName=test";
        return DriverManager.getConnection(URL, username, password);
    }
}
