package com.hbase.demo;

import com.hbase.demo.client.SidxOperation;
import com.hbase.demo.client.SidxTable;
import com.hbase.demo.client.SidxUpdate;
import com.hbase.demo.configuration.SidxTableConfig;
import lombok.Setter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author apktool
 * @title com.hbase.demo.UpdateDemo
 * @description TODO
 * @date 2019-10-13 14:45
 */
@Service
public class UpdateDemo {
    @Setter(onMethod = @__({@Autowired}))
    private SidxOperation sidxOperation;

    @Setter(onMethod = @__({@Autowired}))
    private SidxTableConfig sidxTableConfig;

    public void start(String[] args) {
        SidxTable table = new SidxTable().of(sidxTableConfig.getTableName());
        updateIndexColumn(table);
        updateNotIndexColumn(table);
    }

    private void updateIndexColumn(SidxTable table) {
        String rowKey = "row03";
        SidxUpdate data = new SidxUpdate().of(Bytes.toBytes(rowKey))
            .addColumnFamily(Bytes.toBytes("f1"))
            .addQualifier(Bytes.toBytes("c1"))
            .addValue(Bytes.toBytes("Hello world"))
            .buildCell()
            .build();

        boolean flag = sidxOperation.updateSync(table, data);
        System.out.println(flag);
    }

    private void updateNotIndexColumn(SidxTable table) {
        String rowKey = "row04";
        SidxUpdate data = new SidxUpdate().of(Bytes.toBytes(rowKey))
            .addColumnFamily(Bytes.toBytes("f2"))
            .addQualifier(Bytes.toBytes("c1"))
            .addValue(Bytes.toBytes("Hello Java"))
            .buildCell()
            .build();

        boolean flag = sidxOperation.updateSync(table, data);
        System.out.println(flag);
    }

}
