package com.hbase.demo;

import com.hbase.demo.client.SidxOperation;
import com.hbase.demo.client.SidxPut;
import com.hbase.demo.client.SidxTable;
import com.hbase.demo.configuration.SidxTableConfig;
import lombok.Setter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Random;

/**
 * @author apktool
 * @title com.hbase.demo.PutDemo
 * @description TODO
 * @date 2019-10-06 11:43
 */
@Service
public class PutDemo {
    @Setter(onMethod = @__({@Autowired}))
    private SidxOperation sidxOperation;

    @Setter(onMethod = @__({@Autowired}))
    private SidxTableConfig sidxTableConfig;

    public void start(String[] args) throws IOException {
        SidxTable table = new SidxTable().of(sidxTableConfig.getTableName());

        int range = 10;

        for (int i = 0; i < range; i++) {
            String rowKey = String.format("row%02d", i);

            SidxPut data = new SidxPut().of(Bytes.toBytes(rowKey));

            sidxTableConfig.getTableColumns().forEach(t -> {
                Random random = new Random();
                if ("java.lang.Integer".equals(t.getType().getTypeClassName())) {
                    int tmp = random.nextInt(range);

                    data.addColumnFamily(Bytes.toBytes(t.getFamily()))
                        .addQualifier(Bytes.toBytes(t.getQualifier()))
                        .addValue(Bytes.toBytes(tmp))
                        .addTs(System.currentTimeMillis())
                        .buildCell();
                }
                if ("java.lang.Long".equals(t.getType().getTypeClassName())) {
                    long tmp = random.nextInt(range);

                    data.addColumnFamily(Bytes.toBytes(t.getFamily()))
                        .addQualifier(Bytes.toBytes(t.getQualifier()))
                        .addValue(Bytes.toBytes(tmp))
                        .addTs(System.currentTimeMillis())
                        .buildCell();
                }
            });

            data.build();

            sidxOperation.putSync(table, data);

        }

    }
}
