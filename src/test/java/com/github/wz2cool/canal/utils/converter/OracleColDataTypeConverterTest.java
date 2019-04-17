package com.github.wz2cool.canal.utils.converter;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class OracleColDataTypeConverterTest {

    private final OracleColDataTypeConverter converter = new OracleColDataTypeConverter();

    @Test
    public void test() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("varchar");

        assertNull(converter.convert(colDataType));
    }
}
