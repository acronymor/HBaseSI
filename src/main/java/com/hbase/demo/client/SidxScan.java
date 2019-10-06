package com.hbase.demo.client;

import com.hbase.demo.condition.SidxOperator;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxScan
 * @description: TODO
 * @date 2019-10-03 17:16
 */
@NoArgsConstructor
public class SidxScan {
    @Getter
    private Scan scan;

    @Setter
    private FilterList filters = new FilterList();

    private byte[] columnFamily = new byte[0];

    private byte[] qualifier = new byte[0];

    public SidxScan of() {
        scan = new Scan();
        return this;
    }

    public SidxScan addColumnFamily(byte[] columnFamily) {
        scan.addFamily(columnFamily);
        this.columnFamily = columnFamily;
        return this;
    }

    public SidxScan addQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
        scan.addColumn(columnFamily, qualifier);
        return this;
    }

    public SidxScan setRowFilter(SidxOperator.SidxKind kind, byte[] row) {
        CompareOperator compareOperator = convert(kind);
        Filter filter = new RowFilter(compareOperator, new SubstringComparator(Bytes.toString(row)));
        filters.addFilter(filter);
        return this;
    }

    public SidxScan setValueFilter(SidxOperator.SidxKind sidxCompareOperator, byte[] value) {
        CompareOperator compareOperator = convert(sidxCompareOperator);
        Filter filter = new ValueFilter(compareOperator, new BinaryComparator(value));
        filters.addFilter(filter);
        return this;
    }

    public SidxScan setSingleColumnValueFilter(SidxOperator.SidxKind kind, byte[] value) {
        CompareOperator compareOperator = convert(kind);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(columnFamily, qualifier, compareOperator, value);
        filter.setFilterIfMissing(true);
        filters.addFilter(filter);
        return this;
    }

    public SidxScan setSingleColumnValueExcludeFilter(SidxOperator.SidxKind kind, byte[] value) {
        CompareOperator compareOperator = convert(kind);
        Filter filter = new SingleColumnValueExcludeFilter(columnFamily, qualifier, compareOperator, value);
        filters.addFilter(filter);
        return this;
    }

    public SidxScan setKeyOnlyFilter() {
        Filter filter = new KeyOnlyFilter();
        filters.addFilter(filter);
        return this;
    }

    public SidxScan setPageFilter(long pageSize) {
        Filter filter = new PageFilter(pageSize);
        filters.addFilter(filter);
        return this;
    }

    public SidxScan setColumnCountGetFilter(int columnCount) {
        Filter filter = new ColumnCountGetFilter(columnCount);
        filters.addFilter(filter);
        return this;
    }

    public SidxScan setPrefixFilter(String prefix) {
        Filter filter = new PrefixFilter(Bytes.toBytes(prefix));
        filters.addFilter(filter);
        return this;
    }

    public SidxScan setMultipleColumnPrefixFilter(List<String> prefix) {
        byte[][] prefixes = new byte[prefix.size()][];
        for (int i = 0; i < prefix.size(); i++) {
            prefixes[i] = Bytes.toBytes(prefix.get(i));
        }

        Filter filter = new MultipleColumnPrefixFilter(prefixes);
        filters.addFilter(filter);
        return this;
    }

    public SidxScan buildFilter() {
        scan.setFilter(filters);
        return this;
    }

    public SidxScan build() {
        return this;
    }

    private CompareOperator convert(SidxOperator.SidxKind compareOperator) {
        switch (compareOperator) {
            case EQUAL:
                return CompareOperator.EQUAL;
            case GREATER:
                return CompareOperator.GREATER;
            case GREATER_OR_EQUAL:
                return CompareOperator.GREATER_OR_EQUAL;
            case LESS:
                return CompareOperator.LESS;
            case LESS_OR_EQUAL:
                return CompareOperator.LESS_OR_EQUAL;
            case NOT_EQUAL:
                return CompareOperator.NOT_EQUAL;
            default:
                return CompareOperator.NO_OP;
        }
    }
}
