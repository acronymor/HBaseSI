package com.hbase.demo.utils;

/**
 * @author apktool
 * @title com.hbase.demo.utils.Constants
 * @description Define parts of constants
 * @date 2019-10-02 11:37
 */
public class Constants {
    /**
     * index table
     */
    public static final String INDEX_TABLE_SUFFIX = "sidx";
    public static final String INDEX_TABLE_NAME_SEPARATOR = "_";
    public static final String INDEX_TABLE_COLUMN_FAMILY = "f";
    public static final String INDEX_TABLE_COLUMN_QUALIFIER = "c";
    public static final String INDEX_TABLE_ROWKEY_SEPARATOR = "_";
    public static final String INDEX_TABLE_ROWKEY_ESCAPE_SEPARATOR_BYTE = "__";

    /**
     * meta table
     */
    public static final String META_TABLE_NAME = "sidx.meta.table";
    public static final String META_TABLE_COLUMN_FAMILY = "f";
    public static final String META_TABLE_COLUMN_QUALIFIER_TABLE_CONFIG = "1";
    public static final String META_TABLE_COLUMN_QUALIFIER_TABLE_COLUMNS = "2";
}
