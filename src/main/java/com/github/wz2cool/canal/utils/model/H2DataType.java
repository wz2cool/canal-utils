package com.github.wz2cool.canal.utils.model;

/**
 * @author Frank
 */
public enum H2DataType {
    /**
     * A large integer is a binary integer with a precision of 31 bits.
     */
    INT("INT"),
    /**
     * Possible values are: -128 to 127.
     */
    TINYINT("TINYINT"),
    /**
     * A small integer is a binary integer with a precision of 15 bits. The range of small integers is -32768 to +32767.
     */
    SMALLINT("SMALLINT"),
    /**
     * A big integer is a binary integer with a precision of 63 bits.
     */
    BIGINT("BIGINT"),
    /**
     * Data type with fixed precision and scale. This data type is recommended for storing currency values.
     */
    DECIMAL("DECIMAL"),
    /**
     * A single precision floating point number.
     */
    DOUBLE("DOUBLE"),
    /**
     * It represents the time of the day in hours, minutes and seconds.
     */
    TIME("TIME"),
    /**
     * It represents date of the day in three parts in the form of year, month and day.
     */
    DATE("DATE"),
    /**
     * It represents seven values of the date and time in the form of year, month, day, hours, minutes, seconds and microseconds.
     */
    TIMESTAMP("TIMESTAMP"),
    /**
     * Varying length character strings.
     */
    VARCHAR("VARCHAR"),
    /**
     * Fixed length of Character strings.
     */
    CHAR("CHAR"),
    /**
     * binary string in large object
     */
    BLOB("BLOB"),
    /**
     * Large object strings, you use this when a character string might exceed the limits of the VARCHAR data type.
     */
    CLOB("CLOB");

    private String text;

    H2DataType(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }
}
