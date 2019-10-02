package com.hbase.demo.client;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxPut
 * @description: POJO
 * @date 2019-10-01 22:28
 */
@NoArgsConstructor
public class SidxPut {
    @Getter
    private Put put;

    private List<Cell> cells = new ArrayList<>();

    private CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);

    public SidxPut of(byte[] rowKey) {
        this.cellBuilder.setRow(rowKey);
        this.put = new Put(rowKey);
        return this;
    }

    public SidxPut addColumnFamily(byte[] columnFamily) {
        this.cellBuilder.setFamily(columnFamily);
        return this;
    }

    public SidxPut addQualifier(byte[] qualifier) {
        this.cellBuilder.setQualifier(qualifier);
        return this;
    }

    public SidxPut addValue(byte[] value) {
        this.cellBuilder.setValue(value);
        return this;
    }

    public SidxPut addTs(long ts) {
        this.cellBuilder.setTimestamp(ts);
        return this;
    }

    public SidxPut buildCell() {
        this.cellBuilder.setType(Cell.Type.Put);
        Cell cell = this.cellBuilder.build();
        cells.add(cell);

        return this;
    }

    public SidxPut build() {

        cells.forEach(c -> {
            try {
                this.put.add(c);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return this;
    }
}
