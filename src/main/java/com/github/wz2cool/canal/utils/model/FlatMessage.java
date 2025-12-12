package com.github.wz2cool.canal.utils.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlatMessage implements Serializable {
    private static final long serialVersionUID = -3386650678735860050L;
    private String gtid;
    private long id;
    private String database;
    private String table;
    private List<String> pkNames;
    private Boolean isDdl;
    private String type;
    private Long es;
    private Long ts;
    private String sql;
    private Map<String, Integer> sqlType;
    private Map<String, String> mysqlType;
    private List<Map<String, String>> data;
    private List<Map<String, String>> old;

    public FlatMessage() {
    }

    public FlatMessage(long id) {
        this.id = id;
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return this.table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getPkNames() {
        return this.pkNames;
    }

    public void addPkName(String pkName) {
        if (this.pkNames == null) {
            this.pkNames = new ArrayList<>();
        }

        this.pkNames.add(pkName);
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Boolean getIsDdl() {
        return this.isDdl;
    }

    public void setIsDdl(Boolean isDdl) {
        this.isDdl = isDdl;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTs() {
        return this.ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getSql() {
        return this.sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, Integer> getSqlType() {
        return this.sqlType;
    }

    public void setSqlType(Map<String, Integer> sqlType) {
        this.sqlType = sqlType;
    }

    public Map<String, String> getMysqlType() {
        return this.mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public List<Map<String, String>> getData() {
        return this.data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }

    public List<Map<String, String>> getOld() {
        return this.old;
    }

    public void setOld(List<Map<String, String>> old) {
        this.old = old;
    }

    public Long getEs() {
        return this.es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public String toString() {
        return "FlatMessage [id=" + this.id + ", gtid=" + this.gtid + ", database=" + this.database + ", table=" + this.table + ", isDdl=" + this.isDdl + ", type=" + this.type + ", es=" + this.es + ", ts=" + this.ts + ", sql=" + this.sql + ", sqlType=" + this.sqlType + ", mysqlType=" + this.mysqlType + ", data=" + this.data + ", old=" + this.old + "]";
    }
}
