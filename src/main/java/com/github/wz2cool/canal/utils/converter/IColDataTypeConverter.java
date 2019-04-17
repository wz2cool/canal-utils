package com.github.wz2cool.canal.utils.converter;

import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.Optional;

public interface IColDataTypeConverter {
    Optional<ColDataType> convert(ColDataType mysqlColDataType);
}
