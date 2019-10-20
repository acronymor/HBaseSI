package com.hbase.demo.client;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author apktool
 * @title com.hbase.demo.client.SidxTable
 * @description POJO
 * @date 2019-10-01 21:44
 */
@NoArgsConstructor
public class SidxTable {
    @Getter
    private TableName tableName;

    private TableDescriptorBuilder tdb;
    @Getter
    private TableDescriptor td;

    private ColumnFamilyDescriptorBuilder cdb;

    public SidxTable of(String tableName) {
        this.tableName = TableName.valueOf(tableName);
        this.tdb = TableDescriptorBuilder.newBuilder(this.tableName);
        return this;
    }

    public SidxTable addColumnFamily(String columnFamily) {
        cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
        return this;
    }

    public SidxTable setDataBlockEncoding(String encoding) {
        cdb.setDataBlockEncoding(DataBlockEncoding.valueOf(encoding.toUpperCase()));
        return this;
    }

    public SidxTable setBlockSize(int blockSize) {
        cdb.setBlocksize(blockSize);
        return this;
    }

    public SidxTable setCompressType(String compressType) {
        cdb.setCompressionType(Compression.Algorithm.valueOf(compressType.toUpperCase()));
        return this;
    }

    public SidxTable buildCF() {
        ColumnFamilyDescriptor cfd;
        cfd = cdb.build();
        tdb.setColumnFamily(cfd);
        return this;
    }

    public SidxTable build() {
        td = tdb.build();
        return this;
    }
}
