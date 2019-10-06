package com.hbase.demo;

import com.hbase.demo.client.*;
import com.hbase.demo.configuration.SidxTableConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

/**
 * @author apktool
 * @title: com.hbase.demo.PutDemo
 * @description: TODO
 * @date 2019-10-06 11:43
 */
@Service
public class PutDemo {
    @Autowired
    private SidxOperation sidxOperation;

    @Autowired
    private SidxTableConfig sidxTableConfig;

    public void start(String[] args) throws IOException {
        SidxTable table = new SidxTable().of(sidxTableConfig.getTableName());

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

    }
}
