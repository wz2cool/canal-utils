package com.github.wz2cool.canal.utils.converter;

import net.sf.jsqlparser.statement.alter.Alter;

import java.util.List;

public interface IAlterColumnSqlConverter {
    List<String> convert(Alter mysqlAlter);
}
