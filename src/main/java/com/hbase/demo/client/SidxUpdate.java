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
 * @title com.hbase.demo.client.SidxUpdate
 * @description TODO
 * @date 2019-10-13 14:20
 */
@NoArgsConstructor
public class SidxUpdate {
    @Getter
    private Put update;

    private List<Cell> cells = new ArrayList<>();

    private CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);

    public SidxUpdate of(byte[] rowKey) {
        this.cellBuilder.setRow(rowKey);
        this.update = new Put(rowKey);
        return this;
    }

    public SidxUpdate addColumnFamily(byte[] columnFamily) {
        this.cellBuilder.setFamily(columnFamily);
        return this;
    }

    public SidxUpdate addQualifier(byte[] qualifier) {
        this.cellBuilder.setQualifier(qualifier);
        return this;
    }

    public SidxUpdate addValue(byte[] value) {
        this.cellBuilder.setValue(value);
        return this;
    }

    public SidxUpdate addTs(long ts) {
        this.cellBuilder.setTimestamp(ts);
        return this;
    }

    public SidxUpdate buildCell() {
        this.cellBuilder.setType(Cell.Type.Put);
        Cell cell = this.cellBuilder.build();
        cells.add(cell);

        return this;
    }

    public SidxUpdate build() {

        cells.forEach(c -> {
            try {
                this.update.add(c);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return this;
    }
}
