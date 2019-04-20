package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.model.OracleDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.junit.Test;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class OracleColDataTypeConverterTest {
    private final OracleColDataTypeConverter converter = new OracleColDataTypeConverter();

    @Test
    public void testInvalidType() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("xxxxxxxx");
        List<String> args = new ArrayList<>();
        args.add("200");
        colDataType.setArgumentsStringList(args);
        Optional<ColDataType> result = converter.convert(colDataType);
        assertFalse(result.isPresent());
    }

    @Test
    public void tesBIGINT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("bigint");
        List<String> args = new ArrayList<>();
        args.add("200");
        colDataType.setArgumentsStringList(args);
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("19", resultValue.getArgumentsStringList().get(0));
        assertEquals("0", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testBIT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("bit");
        List<String> args = new ArrayList<>();
        args.add("200");
        colDataType.setArgumentsStringList(args);
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
    }

    @Test
    public void testCHAR() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("CHAR");
        List<String> args = new ArrayList<>();
        args.add("200");
        colDataType.setArgumentsStringList(args);
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.CHAR.getText(), resultValue.getDataType());
        assertEquals("200", resultValue.getArgumentsStringList().get(0));
    }

    @Test
    public void testBLOB() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("blob");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.BLOB.getText(), resultValue.getDataType());
    }

    @Test
    public void testDATE() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DATE");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.DATE.getText(), resultValue.getDataType());
    }

    @Test
    public void testDATETIME() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DATETIME");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.DATE.getText(), resultValue.getDataType());
    }


    @Test
    public void testDECIMAL() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DECIMAL");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.FLOAT.getText(), resultValue.getDataType());
        assertEquals("24", resultValue.getArgumentsStringList().get(0));
    }

    @Test
    public void testDOUBLE() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DOUBLE");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.FLOAT.getText(), resultValue.getDataType());
        assertEquals("24", resultValue.getArgumentsStringList().get(0));
    }

    @Test
    public void testENUM() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("ENUM");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NVARCHAR2.getText(), resultValue.getDataType());
    }

    @Test
    public void testFLOAT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("FLOAT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.FLOAT.getText(), resultValue.getDataType());
    }

    @Test
    public void testINT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("INT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("10", resultValue.getArgumentsStringList().get(0));
        assertEquals("0", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testINTEGER() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("INTEGER");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("10", resultValue.getArgumentsStringList().get(0));
        assertEquals("0", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testLONGBLOB() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("LONGBLOB");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.BLOB.getText(), resultValue.getDataType());
    }

    @Test
    public void testLONGTEXT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("LONGTEXT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NCLOB.getText(), resultValue.getDataType());
    }

    @Test
    public void testMEDIUMINT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("MEDIUMINT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("7", resultValue.getArgumentsStringList().get(0));
        assertEquals("0", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testMEDIUMTEXT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("MEDIUMTEXT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NCLOB.getText(), resultValue.getDataType());
    }

    @Test
    public void testNUMERIC() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("NUMERIC");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
    }

    @Test
    public void testREAL() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("REAL");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.FLOAT.getText(), resultValue.getDataType());
        assertEquals("24", resultValue.getArgumentsStringList().get(0));
    }

    @Test
    public void testSET() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("SET");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NVARCHAR2.getText(), resultValue.getDataType());
    }

    @Test
    public void testSMALLINT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("SMALLINT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("5", resultValue.getArgumentsStringList().get(0));
    }

    @Test
    public void testTEXT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("TEXT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NCLOB.getText(), resultValue.getDataType());
    }

    @Test
    public void testTIME() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("TIME");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.DATE.getText(), resultValue.getDataType());
    }

    @Test
    public void testTIMESTAMP() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("TIMESTAMP");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.DATE.getText(), resultValue.getDataType());
    }

    @Test
    public void testTINYBLOB() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("TINYBLOB");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.BLOB.getText(), resultValue.getDataType());
    }

    @Test
    public void testTINYINT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("TINYINT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("3", resultValue.getArgumentsStringList().get(0));
        assertEquals("0", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testTINYTEXT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("TINYINT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("3", resultValue.getArgumentsStringList().get(0));
        assertEquals("0", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testVARCHAR() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("VARCHAR");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NVARCHAR2.getText(), resultValue.getDataType());
    }

    @Test
    public void testYEAR() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("YEAR");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
    }
}
