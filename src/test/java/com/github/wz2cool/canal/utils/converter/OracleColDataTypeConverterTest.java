package com.github.wz2cool.canal.utils.converter;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class OracleColDataTypeConverterTest {


/*    String sql = "ALTER TABLE `test`\n" +
            "\tADD COLUMN `Column4` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER `date_test`";
    String sql2 = "ALTER TABLE `test`\n" +
            "\tADD INDEX `Column 4` (`Column 4`)";*/

    private final OracleColDataTypeConverter converter = new OracleColDataTypeConverter();

    @Test
    public void test() {
        ColDataType colDataType = new ColDataType();
        colDataType.setDataType("varchar");

        assertNull(converter.convert(colDataType));
    }
}
