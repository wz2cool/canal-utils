package com.github.wz2cool.canal.utils.converter;

import com.github.wz2cool.canal.utils.model.AlterColumnExpression;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AlterColumnSqlConverterBase {
    public abstract List<String> convert(Alter mysqlAlter);

    protected List<AlterColumnExpression> getAlterColumnExpressions(Alter mysqlAlter) {
        if (mysqlAlter == null) {
            return new ArrayList<>();
        }

        List<AlterExpression> alterExpressions = mysqlAlter.getAlterExpressions();
        List<AlterExpression> alterColumns = new ArrayList<>();

        for (AlterExpression alterExpression : alterExpressions) {
            String optionalSpecifier = alterExpression.getOptionalSpecifier();
            if (!"COLUMN".equalsIgnoreCase(optionalSpecifier)) {
                continue;
            }


        }
    }

    private
}
