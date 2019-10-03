package com.hbase.demo;

import com.hbase.demo.client.SidxGet;
import com.hbase.demo.client.SidxOperation;
import com.hbase.demo.client.SidxResult;
import com.hbase.demo.client.SidxTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author apktool
 * @title: com.hbase.demo.Demo
 * @description: TODO
 * @date 2019-09-30 22:39
 */

@Service
public class Demo {

    @Autowired
    private SidxOperation sidxOperation;


    /**
     * @param args
     * @throws IOException
     */
    public void start(String[] args) throws IOException {
        boolean flag = sidxOperation.createTable();
        System.out.println(flag);

        /*
        String tableName = "test";
        String rowKey = "row01";
        String columnFamily = "f1";
        String qualifier = "c1";

        SidxTable table = new SidxTable().of(tableName).build();
        SidxGet get = new SidxGet().of(Bytes.toBytes(rowKey)).build();
        SidxResult result = sidxOperation.get(table, get);

        byte[] value = result.getResult().getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        System.out.println(new String(value));

        SidxTable table = new SidxTable().of(tableName);

        for (int i = 0; i < 10; i++) {
            String rowKey = String.format("row%02d", i);

            SidxPut data = new SidxPut().of(Bytes.toBytes(rowKey));

            sidxTableConfig.getTableColumns().forEach(t -> {
                data.addColumnFamily(Bytes.toBytes(t.getFamily()))
                    .addQualifier(Bytes.toBytes(t.getQualifier()))
                    .addValue(Bytes.toBytes(UUID.randomUUID().toString()))
                    .addTs(System.currentTimeMillis())
                    .buildCell();
            });

            data.build();

            sidxOperation.put(table, data);

        }
         */

        SidxTable table = new SidxTable().of("test");
        String value = "770e37a0-d227-42aa-9790-ddc069a8b0f8";
        Iterator<SidxResult> iterator = sidxOperation.get(table, Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(value));
        while (iterator.hasNext()) {
            Result result = iterator.next().getResult();
            byte[] data = result.getValue(Bytes.toBytes("f2"), Bytes.toBytes("c1"));
            System.out.println(Bytes.toString(data));
            System.out.println(result.size());
        }
    }
}
