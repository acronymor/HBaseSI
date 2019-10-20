package com.hbase.demo;

import com.hbase.demo.client.SidxDelete;
import com.hbase.demo.client.SidxOperation;
import com.hbase.demo.client.SidxTable;
import lombok.Setter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author apktool
 * @title com.hbase.demo.DeleteDemo
 * @description TODO
 * @date 2019-10-13 08:44
 */
@Service
public class DeleteDemo {
    @Setter(onMethod = @__({@Autowired}))
    private SidxOperation sidxOperation;

    public void start(String[] args) {
        SidxTable table = new SidxTable().of("test");
        // deleteRow(table);
        deleteSingleFamily(table);
        deleteSingleColumn(table);
    }

    private void deleteRow(SidxTable table) {
        byte[] rowKey = Bytes.toBytes("row06");

        SidxDelete delete = new SidxDelete().of(rowKey);

        boolean flag = sidxOperation.deleteSync(table, delete);
        System.out.println(flag);
    }

    private void deleteSingleFamily(SidxTable table) {
        byte[] columnFamily = Bytes.toBytes("f2");
        byte[] rowKey = Bytes.toBytes("row07");

        SidxDelete delete = new SidxDelete().of(rowKey).addColumnFamily(columnFamily);

        boolean flag = sidxOperation.deleteSync(table, delete);
        System.out.println(flag);
    }

    private void deleteSingleColumn(SidxTable table) {
        byte[] columnFamily = Bytes.toBytes("f2");
        byte[] qualifier = Bytes.toBytes("c1");

        byte[] rowKey = Bytes.toBytes("row05");

        SidxDelete delete = new SidxDelete().of(rowKey).addQualifier(columnFamily, qualifier);

        boolean flag = sidxOperation.deleteSync(table, delete);
        System.out.println(flag);
    }

}
