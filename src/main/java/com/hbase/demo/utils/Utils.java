package com.hbase.demo.utils;

import com.hbase.demo.client.SidxTable;
import com.hbase.demo.configuration.SidxTableConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author apktool
 * @title: com.hbase.demo.utils.Utils
 * @description: utils
 * @date 2019-10-02 11:03
 */
public class Utils {
    /**
     * @param struct
     * @return List<String>
     * @description IndexTableName = DataTableName_DataTableColumnFamily_DataTableColumn_IndexTableSUFFIX
     */
    public static List<String> deduceIndexTableNames(SidxTableConfig struct) {
        List<String> indexTables = new ArrayList<>(10);

        struct.getTableColumns().stream()
            .filter(t -> t.isIndex())
            .forEach(t -> indexTables.add(deduceIndexTableName(struct.getTableName(), t.getFamily(), t.getQualifier())));

        return indexTables;
    }


    /**
     * @param dataTableName
     * @param columnFamily
     * @param qualifier
     * @return String
     * @description IndexTableName = DataTableName_DataTableColumnFamily_DataTableColumn_IndexTableSUFFIX
     */
    public static String deduceIndexTableName(String dataTableName, String columnFamily, String qualifier) {
        StringBuilder sb = new StringBuilder();
        sb.append(dataTableName);
        sb.append(Constants.INDEX_TABLE_NAME_SEPARATOR);
        sb.append(columnFamily);
        sb.append(Constants.INDEX_TABLE_NAME_SEPARATOR);
        sb.append(qualifier);
        sb.append(Constants.INDEX_TABLE_NAME_SEPARATOR);
        sb.append(Constants.INDEX_TABLE_SUFFIX);
        return sb.toString();
    }

    /**
     * @param sidxTable
     * @param columnFamily
     * @param qualifier
     * @return String
     * @description IndexTableName = DataTableName_DataTableColumnFamily_DataTableColumn_IndexTableSUFFIX
     */
    public static String deduceIndexTableName(SidxTable sidxTable, String columnFamily, String qualifier) {
        String dataTableName = sidxTable.getTableName().getNameAsString();
        return deduceIndexTableName(dataTableName, columnFamily, qualifier);
    }


    /**
     * @param dRowKey data table row key
     * @param column  data table column value
     * @return iRowKey index table row key
     * @throws IOException
     * @description iRowKey = ((byte) (hash(dRowKey) % bucketsNum) + dRowKey
     */
    public static byte[] deduceIndexRowkey(byte[] dRowKey, byte[] column, int bucketsNum) {
        byte[] iRowKey = new byte[0];

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(0);
            outputStream.write(Constants.INDEX_TABLE_ROWKEY_SEPARATOR.getBytes());

            outputStream.write(column);
            outputStream.write(Constants.INDEX_TABLE_ROWKEY_SEPARATOR.getBytes());

            outputStream.write(dRowKey);

            iRowKey = outputStream.toByteArray();
            byte salt = salt(iRowKey, 2, column.length, bucketsNum);
            iRowKey[0] = salt;

            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return iRowKey;
    }

    /**
     * @param value
     * @param offset
     * @param length
     * @param bucketsNum
     * @return byte
     * @description (byte)(( hash ( key) % BUCKETS_NUMBER)
     */
    private static byte salt(byte[] value, int offset, int length, int bucketsNum) {
        int hash = hash(value, offset, length);
        return (byte) Math.abs(hash % bucketsNum);
    }


    /**
     * @param a
     * @param offset
     * @param length
     * @return int
     * @description create hash
     */
    private static int hash(byte a[], int offset, int length) {
        if (a == null) {
            return 0;
        }
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }

    /**
     * @param regions
     * @return byte
     * @description create pre-region bytes array against number of region
     */
    public static byte[][] presplit(int regions) {
        byte[][] splitKeys = new byte[regions - 1][];

        for (int i = 1; i < regions; ++i) {
            splitKeys[i - 1] = new byte[]{(byte) i};
        }

        return splitKeys;
    }
}
