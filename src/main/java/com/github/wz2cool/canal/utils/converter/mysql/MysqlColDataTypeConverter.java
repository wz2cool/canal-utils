package com.github.wz2cool.canal.utils.converter.mysql;

import com.github.wz2cool.canal.utils.converter.IColDataTypeConverter;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.Optional;

public class MysqlColDataTypeConverter implements IColDataTypeConverter {
    @Override
    public Optional<ColDataType> convert(ColDataType mysqlColDataType) {
        return Optional.of(mysqlColDataType);
    }
}
