package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.model.OracleDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.junit.Test;

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
        assertEquals("20", resultValue.getArgumentsStringList().get(0));
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
        assertEquals(OracleDataType.NVARCHAR2.getText(), resultValue.getDataType());
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
        assertEquals(OracleDataType.TIMESTAMP.getText(), resultValue.getDataType());
    }

    @Test
    public void testDATETIME() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DATETIME");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.TIMESTAMP.getText(), resultValue.getDataType());
    }


    @Test
    public void testDECIMAL() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DECIMAL");
        List<String> args = new ArrayList<>();
        args.add("20");
        args.add("3");
        colDataType.setArgumentsStringList(args);
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("20", resultValue.getArgumentsStringList().get(0));
        assertEquals("3", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testDOUBLE() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DOUBLE");
        List<String> args = new ArrayList<>();
        args.add("10");
        args.add("4");
        colDataType.setArgumentsStringList(args);
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("10", resultValue.getArgumentsStringList().get(0));
        assertEquals("4", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testFLOAT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("FLOAT");
        List<String> args = new ArrayList<>();
        args.add("8");
        args.add("2");
        colDataType.setArgumentsStringList(args);
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("8", resultValue.getArgumentsStringList().get(0));
        assertEquals("2", resultValue.getArgumentsStringList().get(1));
    }

    @Test
    public void testINT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("INT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("11", resultValue.getArgumentsStringList().get(0));
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
        assertEquals("11", resultValue.getArgumentsStringList().get(0));
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
        assertEquals("8", resultValue.getArgumentsStringList().get(0));
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
    public void testSMALLINT() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("SMALLINT");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.NUMBER.getText(), resultValue.getDataType());
        assertEquals("6", resultValue.getArgumentsStringList().get(0));
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
        assertEquals(OracleDataType.VARCHAR2.getText(), resultValue.getDataType());
    }

    @Test
    public void testTIMESTAMP() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("TIMESTAMP");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElse(null);
        assertEquals(OracleDataType.TIMESTAMP.getText(), resultValue.getDataType());
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
        assertEquals("4", resultValue.getArgumentsStringList().get(0));
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
        assertEquals("4", resultValue.getArgumentsStringList().get(0));
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
}
