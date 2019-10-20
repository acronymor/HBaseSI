package com.hbase.demo;

import com.hbase.demo.client.SidxOperation;
import com.hbase.demo.client.SidxResult;
import com.hbase.demo.client.SidxScan;
import com.hbase.demo.client.SidxTable;
import lombok.Setter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Iterator;

/**
 * @author apktool
 * @title com.hbase.demo.ScanDemo
 * @description TODO
 * @date 2019-10-13 16:35
 */
@Service
public class ScanDemo {

    @Setter(onMethod = @__({@Autowired}))
    private SidxOperation sidxOperation;


    public void start(String[] args) {
        SidxTable table = new SidxTable().of("test");
        simpleScan(table);
    }

    public void simpleScan(SidxTable table) {
        byte[] columnFamily = Bytes.toBytes("f1");
        byte[] qulifier = Bytes.toBytes("c1");

        SidxScan scan = new SidxScan().of().addColumnFamily(columnFamily).addQualifier(qulifier).build();

        SidxResult result = sidxOperation.scan(table, scan);

        for (Iterator<Result> iterator = result.getIterator(); iterator.hasNext(); ) {
            Result next = iterator.next();
            System.out.println(Bytes.toString(next.getRow()));
        }
    }
}
