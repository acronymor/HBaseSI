package com.hbase.demo.client;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.client.Delete;

/**
 * @author apktool
 * @title com.hbase.demo.client.SidxDelete
 * @description TODO
 * @date 2019-10-13 08:05
 */
@Getter
@NoArgsConstructor
public class SidxDelete {
    private Delete delete;

    private byte[] columnFamily;

    private byte[] qualifier;

    public SidxDelete of(byte[] rowKey) {
        delete = new Delete(rowKey);
        return this;
    }

    public SidxDelete addColumnFamily(byte[] columnFamily) {
        this.columnFamily = columnFamily;
        delete.addFamily(columnFamily);
        return this;
    }

    public SidxDelete addQualifier(byte[] columnFamily, byte[] qualifier) {
        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
        delete.addColumn(columnFamily, qualifier);
        return this;
    }

    public SidxDelete build() {
        return this;
    }
}
