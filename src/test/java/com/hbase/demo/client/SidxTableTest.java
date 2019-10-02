package com.hbase.demo.client;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxTableTest
 * @description: TODO
 * @date 2019-10-02 23:55
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SidxTableTest {
    private static final String tableName = "test";

    @Autowired
    private HbaseOperator operator;

    @Test
    public void createTableOneFamily() {
        SidxTable table = new SidxTable().of(tableName)
            .addColumnFamily("f")
            .buildCF()
            .build();

        if (operator.isTableExisted(table)) {
        }

        operator.createTable(table, 1);
    }

    @Test
    public void createTableTwoFamily() {
        SidxTable table = new SidxTable().of(tableName)
            .addColumnFamily("f1")
            .buildCF()
            .addColumnFamily("f2")
            .buildCF()
            .build();

        if (!operator.isTableExisted(table)) {
            operator.deleteTable(table);
        }
        operator.createTable(table, 1);
    }

    @After
    public void deleteTable() {

        SidxTable table = new SidxTable().of(tableName);

        if (operator.isTableExisted(table)) {
            operator.deleteTable(table);
        }

    }
}
