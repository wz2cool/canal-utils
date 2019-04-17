package com.github.wz2cool.canal.utils.converter.oracle;

import com.github.wz2cool.canal.utils.model.OracleDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class OracleColDataTypeConverterTest {


/*    String sql = "ALTER TABLE `test`\n" +
            "\tADD COLUMN `Column4` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER `date_test`";
    String sql2 = "ALTER TABLE `test`\n" +
            "\tADD INDEX `Column 4` (`Column 4`)";*/

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

        ColDataType resultValue = result.orElseGet(null);
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
        assertEquals(OracleDataType.RAW.getText(), resultValue.getDataType());
    }

    @Test
    public void testBLOB() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("blob");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElseGet(null);
        assertEquals(OracleDataType.BLOB.getText(), resultValue.getDataType());
    }

    @Test
    public void testDATE() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DATE");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElseGet(null);
        assertEquals(OracleDataType.DATE.getText(), resultValue.getDataType());
    }

    @Test
    public void testDATETIME() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DATETIME");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElseGet(null);
        assertEquals(OracleDataType.DATE.getText(), resultValue.getDataType());
    }


    @Test
    public void testDECIMAL() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DECIMAL");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElseGet(null);
        assertEquals(OracleDataType.FLOAT.getText(), resultValue.getDataType());
        assertEquals("24", resultValue.getArgumentsStringList().get(0));
    }

    @Test
    public void testDOUBLE() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("DOUBLE");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElseGet(null);
        assertEquals(OracleDataType.FLOAT.getText(), resultValue.getDataType());
        assertEquals("24", resultValue.getArgumentsStringList().get(0));
    }

    @Test
    public void testENUM() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("ENUM");
        Optional<ColDataType> result = converter.convert(colDataType);
        assertTrue(result.isPresent());

        ColDataType resultValue = result.orElseGet(null);
        assertEquals(OracleDataType.NVARCHAR2.getText(), resultValue.getDataType());
    }
}
