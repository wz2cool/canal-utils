package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.converter.factory.AlterColumnSqlConverterFactory;
import com.github.wz2cool.canal.utils.converter.factory.ValuePlaceholderConverterFactory;
import com.github.wz2cool.canal.utils.helper.ConfigHelper;
import com.github.wz2cool.canal.utils.helper.DatabaseInfo;
import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.ValuePlaceholder;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DatabaseTest {
    private final AlterColumnSqlConverterFactory alterColumnSqlConverterFactory = new AlterColumnSqlConverterFactory();
    private final ValuePlaceholderConverterFactory valuePlaceholderConverterFactory = new ValuePlaceholderConverterFactory();

    private final DatabaseInfo databaseInfo;

    public DatabaseTest() {
        this.databaseInfo = ConfigHelper.getInstance().getDatabaseInfo();
    }

    private String getTestTableName() {
        return "MY_TEST";
    }

    private AlterSqlConverterBase getAlterColumnSqlConverter() {
        return alterColumnSqlConverterFactory.create(this.databaseInfo.getDatabaseType());
    }

    private IValuePlaceholderConverter getValuePlaceholderConverter() {
        return valuePlaceholderConverterFactory.create(this.databaseInfo.getDatabaseType());
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                this.databaseInfo.getJdbcUrl(),
                this.databaseInfo.getUsername(),
                this.databaseInfo.getPassword());
    }

    @Before
    public void initTable() throws SQLException, ClassNotFoundException {
        Class.forName(this.databaseInfo.getClassName());
        tryDropTestTable();
        createTestTable();
    }

    @Test
    public void addBITColumn() throws JSQLParserException, SQLException {
        System.out.println("addBITColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BIT NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.BIT, "1");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("1", insertOptional.orElse(null));
    }

    @Test
    public void addTINYINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addTINYINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TINYINT NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TINYINT, "1");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("1", insertOptional.orElse(null));
    }

    @Test
    public void addMEDIUMINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addMEDIUMINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` MEDIUMINT NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.MEDIUMINT, "1");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("1", insertOptional.orElse(null));
    }

    @Test
    public void addSMALLINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addSMALLINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` SMALLINT NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.SMALLINT, "1");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("1", insertOptional.orElse(null));
    }

    @Test
    public void addINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` INT NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.INT, "1");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("1", insertOptional.orElse(null));
    }

    @Test
    public void addINTEGERColumn() throws JSQLParserException, SQLException {
        System.out.println("addINTEGERColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` INTEGER NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.INTEGER, "1");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("1", insertOptional.orElse(null));
    }

    @Test
    public void addBIGINTColumn() throws JSQLParserException, SQLException {
        System.out.println("addBIGINTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BIGINT NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.BIGINT, "1");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("1", insertOptional.orElse(null));
    }

    @Test
    public void addFLOATColumn() throws JSQLParserException, SQLException {
        System.out.println("addFLOATColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` FLOAT (18 ,6) NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.FLOAT, "3.33333");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("3.33333", StringUtils.strip(insertOptional.orElse(null), "0"));
    }

    @Test
    public void addDOUBLEColumn() throws JSQLParserException, SQLException {
        System.out.println("addDOUBLEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DOUBLE (18 ,6) NULL;", getTestTableName());
        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DOUBLE, "3.33333");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("3.33333", StringUtils.strip(insertOptional.orElse(null), "0"));
    }

    @Test
    public void addDECIMALColumn() throws JSQLParserException, SQLException {
        System.out.println("addDECIMALColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DECIMAL (18, 6) NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DECIMAL, "3.33333");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("3.33333", StringUtils.strip(insertOptional.orElse(null), "0"));
    }

    @Test
    public void addDATEColumn() throws JSQLParserException, SQLException, ParseException {
        System.out.println("addDATEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DATE NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DATE, "2019-04-20");
        Optional<String> insertOptional = getData();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date expectedDate = simpleDateFormat.parse("2019-04-20");
        Date actualDate = simpleDateFormat.parse(insertOptional.orElse(null));
        Assert.assertEquals(expectedDate, actualDate);
    }

    @Test
    public void addDATETIMEColumn() throws JSQLParserException, SQLException, ParseException {
        System.out.println("addDATETIMEColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` DATETIME NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.DATETIME, "2019-04-20 22:16:12");
        Optional<String> insertOptional = getData();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date expectedDate = simpleDateFormat.parse("2019-04-20 22:16:12");
        Date actualDate = simpleDateFormat.parse(insertOptional.orElse(null));
        Assert.assertEquals(expectedDate, actualDate);
    }

    @Test
    public void addTIMESTAMPColumn() throws JSQLParserException, SQLException, ParseException {
        System.out.println("addTIMESTAMPColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TIMESTAMP NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TIMESTAMP, "2019-04-20 22:16:12");
        Optional<String> insertOptional = getData();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date expectedDate = simpleDateFormat.parse("2019-04-20 22:16:12");
        Date actualDate = simpleDateFormat.parse(insertOptional.orElse(null));
        Assert.assertEquals(expectedDate, actualDate);
    }

    @Test
    public void addTIMEColumn() throws JSQLParserException, SQLException, ParseException {
        System.out.println("addTIMESTAMPColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TIME NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TIME, "10:00:00");
        Optional<String> insertOptional = getData();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");
        Date expectedDate = simpleDateFormat.parse("10:00:00");
        Date actualDate = simpleDateFormat.parse(insertOptional.orElse(null));
        Assert.assertEquals(expectedDate, actualDate);
    }

    @Test
    public void addCHARColumn() throws JSQLParserException, SQLException {
        System.out.println("addCHARColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` CHAR (50) NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.CHAR, "test");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("test", insertOptional.orElse(null));
    }

    @Test
    public void addVARCHARColumn() throws JSQLParserException, SQLException {
        System.out.println("addVARCHARColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` VARCHAR (255) NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.VARCHAR, "test");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("test", insertOptional.orElse(null));
    }

    @Test
    public void addTINYBLOBColumn() throws JSQLParserException, SQLException {
        System.out.println("addTINYBLOBColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TINYBLOB NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TINYBLOB, "0x32");

        byte[] expectedBytes = "0x32".getBytes(StandardCharsets.ISO_8859_1);
        Optional<byte[]> insertOptional = getBytesData();
        Assert.assertArrayEquals(expectedBytes, insertOptional.orElse(null));
    }

    @Test
    public void addTINYTEXTColumn() throws JSQLParserException, SQLException {
        System.out.println("addTINYTEXTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TINYTEXT NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TINYTEXT, "test");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("test", insertOptional.orElse(null));
    }

    @Test
    public void addTEXTColumn() throws JSQLParserException, SQLException {
        System.out.println("addTEXTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` TEXT NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.TEXT, "test");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("test", insertOptional.orElse(null));
    }

    @Test
    public void addMEDIUMTEXTColumn() throws JSQLParserException, SQLException {
        System.out.println("addTEXTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` MEDIUMTEXT NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.MEDIUMTEXT, "test");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("test", insertOptional.orElse(null));
    }

    @Test
    public void addLONGTEXTColumn() throws JSQLParserException, SQLException {
        System.out.println("addLONGTEXTColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` LONGTEXT NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.LONGTEXT, "test");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("test", insertOptional.orElse(null));
    }

    @Test
    public void addBLOBColumn() throws JSQLParserException, SQLException {
        System.out.println("addBLOBColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` BLOB NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.BLOB, "0x32");
        byte[] expectedBytes = "0x32".getBytes(StandardCharsets.ISO_8859_1);
        Optional<byte[]> insertOptional = getBytesData();
        Assert.assertArrayEquals(expectedBytes, insertOptional.orElse(null));
    }

    @Test
    public void addMEDIUMBLOBColumn() throws JSQLParserException, SQLException {
        System.out.println("addBLOBColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` MEDIUMBLOB NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.MEDIUMBLOB, "0x32");
        byte[] expectedBytes = "0x32".getBytes(StandardCharsets.ISO_8859_1);
        Optional<byte[]> insertOptional = getBytesData();
        Assert.assertArrayEquals(expectedBytes, insertOptional.orElse(null));
    }

    @Test
    public void addLONGBLOBColumn() throws JSQLParserException, SQLException {
        System.out.println("addLONGBLOBColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` LONGBLOB NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.LONGBLOB, "0x32");
        byte[] expectedBytes = "0x32".getBytes(StandardCharsets.ISO_8859_1);
        Optional<byte[]> insertOptional = getBytesData();
        Assert.assertArrayEquals(expectedBytes, insertOptional.orElse(null));
    }

    @Test
    public void addMultiColumns() throws JSQLParserException, SQLException {
        System.out.println("addMultiColumns");
        String msqlAddColumn = String.format("ALTER TABLE %s\n" +
                "\tADD COLUMN `col1` DECIMAL(10,0) NOT NULL,\n" +
                "\tADD COLUMN `col2` TEXT NOT NULL AFTER `col1`;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }
    }

    @Test
    public void changeColumnType() throws SQLException, JSQLParserException {
        System.out.println("addMultiColumns");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` INTEGER NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String mysqlChangeColumnType = String.format("ALTER TABLE `%s`\n" +
                "\tCHANGE COLUMN `col1` `col1` VARCHAR(255) NULL DEFAULT NULL;", getTestTableName());
        List<String> changeColumnTypeSqls = getAlterColumnSqlConverter().convert(mysqlChangeColumnType);
        for (String sql : changeColumnTypeSqls) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.VARCHAR, "TEST");
        Optional<String> insertOptional = getData();
        Assert.assertEquals("TEST", insertOptional.orElse(null));
    }

    @Test
    public void renameColumn() throws SQLException, JSQLParserException {
        System.out.println("addMultiColumns");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col2` INTEGER NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String mysqlChangeColumnType = String.format("ALTER TABLE `%s`\n" +
                "\tCHANGE COLUMN `col2` `col1` INT(11) NULL DEFAULT NULL;", getTestTableName());
        List<String> changeColumnTypeSqls = getAlterColumnSqlConverter().convert(mysqlChangeColumnType);
        for (String sql : changeColumnTypeSqls) {
            executeAlterSql(sql);
        }

        insertData(MysqlDataType.INT, "1");
    }

    @Test
    public void dropColumn() throws SQLException, JSQLParserException {
        if ("hive".equalsIgnoreCase(databaseInfo.getDatabaseType())) {
            // current not support drop column for hive.
            return;
        }


        System.out.println("dropColumn");
        String msqlAddColumn = String.format("ALTER TABLE `%s`\n" +
                "\tADD COLUMN `col1` MEDIUMTEXT NULL;", getTestTableName());

        List<String> result = getAlterColumnSqlConverter().convert(msqlAddColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }

        String mysqlDropColumn = String.format("alter table `%s` drop column `%s`;", getTestTableName(), "col1");
        result = getAlterColumnSqlConverter().convert(mysqlDropColumn);
        for (String sql : result) {
            executeAlterSql(sql);
        }
    }

    @Test
    public void sqlCleanTest() throws JSQLParserException {
        String testSql = "ALTER TABLE canal_test_db.mssql_test MODIFY COLUMN c6 char(11) CHARACTER SET utf8 COLLATE utf8_general_ci";
        getAlterColumnSqlConverter().convert(testSql);
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
            ValuePlaceholder valuePlaceholder = getValuePlaceholderConverter().convert(mysqlDataType, value);

            String insertSql;
            if ("hive".equalsIgnoreCase(databaseInfo.getDatabaseType())) {
                insertSql = String.format("INSERT INTO %S VALUES (%s, %s)",
                        getTestTableName(), 1, valuePlaceholder.getPlaceholder());
            } else {
                insertSql = String.format("INSERT INTO %S (USER_ID, col1) VALUES (%s, %s)",
                        getTestTableName(), 1, valuePlaceholder.getPlaceholder());
            }

            try (PreparedStatement statement = dbConnection.prepareStatement(insertSql)) {
                System.out.println(String.format("[%s] insertSql: %s", mysqlDataType.getText(), insertSql));
                Object inputValue = valuePlaceholder.getValue();
                if (inputValue instanceof byte[]) {
                    try {
                        try (InputStream targetStream = new ByteArrayInputStream((byte[]) inputValue)) {
                            statement.setBinaryStream(1, targetStream);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    statement.setObject(1, valuePlaceholder.getValue());
                }

                statement.execute();
                System.out.println("execute success");
            }
        }
    }

    // get col1 value after insert
    private synchronized Optional<String> getData() throws SQLException {
        String sql = String.format("SELECT col1 FROM %s", getTestTableName());
        try (Connection dbConnection = getConnection();
             Statement statement = dbConnection.createStatement()) {
            // ResultSet
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                return Optional.ofNullable(rs.getString(1));
            }
        }
        return Optional.empty();
    }

    private synchronized Optional<byte[]> getBytesData() throws SQLException {
        String sql = String.format("SELECT col1 FROM %s", getTestTableName());
        try (Connection dbConnection = getConnection();
             Statement statement = dbConnection.createStatement()) {
            // ResultSet
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                if ("hive".equalsIgnoreCase(databaseInfo.getDatabaseType())) {
                    try {
                        try (InputStream is = rs.getBinaryStream(1)) {
                            byte[] bytes = IOUtils.toByteArray(is);
                            return Optional.of(bytes);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    return Optional.ofNullable(rs.getBytes(1));
                }
            }
        }
        return Optional.empty();
    }

    private void tryDropTestTable() {
        String dropTableSql = String.format("DROP TABLE %s", getTestTableName());
        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            // execute the SQL statement
            statement.execute(dropTableSql);
            System.out.println("Drop table success");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void createTestTable() throws SQLException {
        String createTableSQL = String.format("CREATE TABLE %s (USER_ID INT)",
                getTestTableName());

        try (Connection dbConnection = getConnection(); Statement statement = dbConnection.createStatement()) {
            statement.execute(createTableSQL);
            System.out.println("create table success");
        }


    }
}
