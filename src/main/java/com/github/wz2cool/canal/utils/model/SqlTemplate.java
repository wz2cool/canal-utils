package com.github.wz2cool.canal.utils.model;

/**
 * Sql template
 *
 * @author Frank
 */
public class SqlTemplate {

    private String expression;
    private Object[] params = new Object[]{};

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public Object[] getParams() {
        return params == null ? null : params.clone();
    }

    public void setParams(Object[] params) {
        this.params = params == null ? null : params.clone();
    }
}
