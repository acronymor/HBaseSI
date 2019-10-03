package com.hbase.demo.client;

import lombok.Getter;
import org.apache.hadoop.hbase.client.Get;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxGet
 * @description: TODO
 * @date 2019-10-03 09:51
 */
public class SidxGet {
    @Getter
    private Get get;
    private byte[] columnFamily = new byte[0];

    public SidxGet of(byte[] rowKey) {
        get = new Get(rowKey);
        return this;
    }

    public SidxGet addColumnFamily(byte[] columnFamily) {
        get.addFamily(columnFamily);
        this.columnFamily = columnFamily;
        return this;
    }

    public SidxGet addQualifier(byte[] qualifier) {
        get.addColumn(columnFamily, qualifier);
        return this;
    }

    public SidxGet build() {
        return this;
    }

}
