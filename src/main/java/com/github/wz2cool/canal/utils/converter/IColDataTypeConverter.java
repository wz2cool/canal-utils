package com.github.wz2cool.canal.utils.converter;

import net.sf.jsqlparser.statement.create.table.ColDataType;

public interface IColDataTypeConverter {
    ColDataType convert(ColDataType mysqlColDataType);
}
