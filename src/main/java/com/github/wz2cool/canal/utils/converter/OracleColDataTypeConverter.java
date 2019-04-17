package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.MysqlDataType;
import com.github.wz2cool.canal.utils.model.OracleDataType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.Optional;

public class OracleColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public ColDataType convert(ColDataType mysqlColDataType) {
        String mysqlColDataTypeString = mysqlColDataType.getDataType();
        getOracleDataTypeString(mysqlColDataTypeString);
        return null;
    }

    private Optional<String> getOracleDataTypeString(String mysqlColDataTypeString) {
        Optional<MysqlDataType> mysqlTypeOptional = MysqlDataType.getDataType(mysqlColDataTypeString);
        if (!mysqlTypeOptional.isPresent()) {
            return Optional.empty();
        }


        return Optional.empty()
    }

    // https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABGACIF
    private OracleDataType getOracleDataType(final MysqlDataType mysqlDataType) {
        switch (mysqlDataType) {
            case BIT:
                return OracleDataType.RAW;
            case INT:
            case INTEGER:
            case TINYINT:
            case BIGINT:
            case SMALLINT:
            case MEDIUMINT:
            case NUMERIC:
            case YEAR:
                return OracleDataType.NUMBER;
            case DATE:
            case DATETIME:
            case TIMESTAMP:
            case TIME:
                return OracleDataType.DATE;
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case REAL:
                return OracleDataType.Float;
            case CHAR:
                return OracleDataType.CHAR;
            case LONGTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case TINYTEXT:
                return OracleDataType.NCLOB;
            case ENUM:
            case SET:
            case VARCHAR:
                return OracleDataType.NVARCHAR2;
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
                return OracleDataType.BLOB;
            default:
                return OracleDataType.NCLOB;
        }
    }
}
