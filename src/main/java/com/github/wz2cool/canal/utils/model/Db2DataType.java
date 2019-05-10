package com.github.wz2cool.canal.utils.model;

/**
 * @author Frank
 */
public enum Db2DataType {
    /**
     * A big integer is a binary integer with a precision of 63 bits.
     */
    BIGINT("BIGINT"),
    /**
     * A small integer is a binary integer with a precision of 15 bits. The range of small integers is -32768 to +32767.
     */
    SMALLINT("SMALLINT"),
    /**
     * A large integer is a binary integer with a precision of 31 bits.
     */
    INTEGER("INTEGER"),
    /**
     * A double precision floating-point number is a long (64 bits) floating-point number.
     */
    DOUBLE("DOUBLE"),
    /**
     * A decimal number is a packed decimal number with an implicit decimal point.
     */
    DECIMAL("DECIMAL"),
    /**
     * A single precision floating-point number is a short (32 bits) floating-point number.
     */
    REAL("REAL"),
    /**
     * Fixed length of Character strings.
     */
    CHAR("CHAR"),
    /**
     * Varying length character strings.
     */
    VARCHAR("VARCHAR"),
    /**
     * large object strings, you use this when a character string might exceed the limits of the VARCHAR data type.
     */
    CLOB("CLOB"),
    /**
     * It represents date of the day in three parts in the form of year, month and day.
     */
    DATE("DATE"),
    /**
     * It represents the time of the day in hours, minutes and seconds.
     */
    TIME("TIME"),
    /**
     * It represents seven values of the date and time in the form of year, month, day, hours, minutes, seconds and microseconds.
     */
    TIMESTAMP("TIMESTAMP"),
    /**
     * Binary string in large object
     */
    BLOB("BLOB");


    private String text;

    Db2DataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }
}
