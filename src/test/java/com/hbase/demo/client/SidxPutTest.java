package com.hbase.demo.client;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxPutTest
 * @description: TODO
 * @date 2019-10-02 21:55
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SidxPutTest {
    private static final String tableName = "test";

    @Autowired
    private HbaseOperator operator;

    public SidxTable getTable(String tableName) {
        return new SidxTable().of(tableName).addColumnFamily("f1").buildCF().addColumnFamily("f2").buildCF().build();
    }

    @Before
    public void createTable() {
        SidxTable table = getTable(tableName);

        if (!operator.isTableExisted(table)) {
            operator.createTable(table, 1);
        }
    }

    @Test
    public void putOneFamilyOneColumn() {

        String rowKey = "rowKey";
        String dataTableColumnFamily = "f1";
        String dataTableQualifier = "c";
        String value = "value";


        SidxPut data = new SidxPut().of(Bytes.toBytes(rowKey))
            .addColumnFamily(Bytes.toBytes(dataTableColumnFamily))
            .addQualifier(Bytes.toBytes(dataTableQualifier))
            .addValue(Bytes.toBytes(value))
            .addTs(System.currentTimeMillis())
            .buildCell()
            .build();

        SidxTable table = getTable(tableName);
        operator.put(table, data);
    }

    @Test
    public void putOneFamilyTwoColumns() {
        String rowKey = "rowKey";

        String dataTableColumnFamily = "f1";
        String dataTableQualifier1 = "1";
        String dataTableQualifier2 = "2";

        String value01 = "value01";
        String value02 = "value02";


        SidxPut data = new SidxPut().of(Bytes.toBytes(rowKey))
            .addColumnFamily(Bytes.toBytes(dataTableColumnFamily))
            .addQualifier(Bytes.toBytes(dataTableQualifier1))
            .addValue(Bytes.toBytes(value01))
            .buildCell()
            .addQualifier(Bytes.toBytes(dataTableQualifier2))
            .addValue(Bytes.toBytes(value02))
            .addTs(System.currentTimeMillis())
            .buildCell()
            .build();

        SidxTable table = getTable(tableName);
        operator.put(table, data);
    }

    @Test
    public void putTwoFamilyTwoColumns() {
        String rowKey = "rowKey";

        String dataTableColumnFamily1 = "f1";
        String dataTableColumnFamily2 = "f2";
        String dataTableQualifier1 = "1";
        String dataTableQualifier2 = "2";

        String value01 = "value01";
        String value02 = "value02";


        SidxPut data = new SidxPut().of(Bytes.toBytes(rowKey))
            .addColumnFamily(Bytes.toBytes(dataTableColumnFamily1))
            .addQualifier(Bytes.toBytes(dataTableQualifier1))
            .addValue(Bytes.toBytes(value01))
            .buildCell()
            .addTs(System.currentTimeMillis())
            .addColumnFamily(Bytes.toBytes(dataTableColumnFamily2))
            .addQualifier(Bytes.toBytes(dataTableQualifier2))
            .addValue(Bytes.toBytes(value02))
            .addTs(System.currentTimeMillis())
            .buildCell()
            .build();

        SidxTable table = getTable(tableName);
        operator.put(table, data);
    }

    public void deleteTable() {
        SidxTable table = getTable(tableName);
        operator.deleteTable(table);
    }
}
