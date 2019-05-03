package com.github.wz2cool.canal.utils.converter.mssql;

import com.github.wz2cool.canal.utils.converter.oracle.OracleAlterColumnSqlConverter;
import com.github.wz2cool.canal.utils.converter.oracle.OracleValuePlaceholderConverter;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import org.junit.Test;

import java.sql.*;
import java.util.List;

public class MssqlDatabaseTest {
    private static String TABLE_NAME = "MY_TEST";

    private final MssqlAlterColumnSqlConverter mssqlAlterColumnSqlConverter = new MssqlAlterColumnSqlConverter();
    private final MssqlValuePlaceholderConverter mssqlValuePlaceholderConverter = new MssqlValuePlaceholderConverter();


    public MssqlDatabaseTest() throws ClassNotFoundException, SQLException {
        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
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
        List<String> result = mssqlAlterColumnSqlConverter.convert((Alter) statement);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.BIT, "1");
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
            ValuePlaceholder valuePlaceholder = mssqlValuePlaceholderConverter.convert(mysqlDataType, value);
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
        String URL = "jdbc:sqlserver://192.168.2.112:1433;DatabaseName=test";
        return DriverManager.getConnection(URL, username, password);
    }
}
