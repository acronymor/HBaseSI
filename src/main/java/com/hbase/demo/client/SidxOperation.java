package com.hbase.demo.client;

import com.hbase.demo.condition.*;
import com.hbase.demo.configuration.SidxTableConfig;
import com.hbase.demo.utils.Constants;
import com.hbase.demo.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author apktool
 * @title com.hbase.demo.client.SidxOperation
 * @description Basic operation of Sidx Table
 * @date 2019-10-01 20:48
 */

@Service
@Slf4j
public class SidxOperation extends AbstractSidxOperation {

    /**
     * @return boolean
     * @description Create tables synchronously
     */
    @Override
    public boolean createTableSync() {
        boolean flag = true;
        flag &= createMetaTable();
        flag &= createDataTable();
        flag &= createIndexTable();

        log.info("All table have been created successfully");

        return flag;
    }

    /**
     * @return boolean
     * @description Create tables asynchronously
     */
    @Override
    public boolean createTableAsync() {
        return false;
    }

    /**
     * @param sidxTable
     * @param sidxPut
     * @return boolean
     * @description put data synchronously
     */
    @Override
    public boolean putSync(SidxTable sidxTable, SidxPut sidxPut) {

        boolean flag = put(sidxTable, sidxPut);

        SidxTableConfig tableConfig = achieveMeta(sidxTable);

        List<SidxTableConfig.TableColumn> tableColumns = tableConfig.getTableColumns().stream().filter(SidxTableConfig.TableColumn::isIndex).collect(Collectors.toList());

        for (SidxTableConfig.TableColumn t : tableColumns) {

            List<Cell> list = sidxPut.getPut().get(Bytes.toBytes(t.getFamily()), Bytes.toBytes(t.getQualifier()));
            Cell item = list.get(0);

            int offset = item.getValueOffset();
            int len = item.getValueLength();
            byte[] column = Arrays.copyOfRange(item.getRowArray(), offset, offset + len);
            byte[] indexRowKey = Utils.deduceIndexRowkey(sidxPut.getPut().getRow(), column, tableConfig.getTableConfig().getIndexTableRegions());

            SidxPut indexPut = new SidxPut().of(indexRowKey)
                .addColumnFamily(Bytes.toBytes(Constants.INDEX_TABLE_COLUMN_FAMILY))
                .addQualifier(Bytes.toBytes(Constants.INDEX_TABLE_COLUMN_QUALIFIER))
                .addValue(Bytes.toBytes(""))
                .buildCell()
                .build();

            String indexTableName = Utils.deduceIndexTableName(tableConfig.getTableName(), t.getFamily(), t.getQualifier());
            SidxTable indexTable = new SidxTable().of(indexTableName);

            flag &= put(indexTable, indexPut);
        }

        return flag;
    }

    /**
     * @param sidxTable
     * @param sidxPut
     * @return boolean
     * @description put data asynchronously
     */
    @Override
    public boolean putAsync(SidxTable sidxTable, SidxPut sidxPut) {
        return false;
    }

    /**
     * @param sidxTable
     * @param node
     * @return boolean
     * @description get data synchronously
     */
    @Override
    public SidxResult getSync(SidxTable sidxTable, SidxCall node) {
        AbstractSidxNode[] operators = node.getOperators();

        SidxIdentifier identifier = (SidxIdentifier) operators[0];
        SidxLiteral literal = (SidxLiteral) operators[1];
        SidxOperator.SidxKind kind = node.getOperator().getKind();

        byte[] family = identifier.getFamilyIdentifier();
        byte[] qualifier = identifier.getColumnIdentifier();
        byte[] value = literal.getLiteral();

        SidxTableConfig meta = achieveMeta(sidxTable);

        List<String> indexTableNames = Utils.deduceIndexTableNames(meta);
        String indexTableName = Utils.deduceIndexTableName(sidxTable, Bytes.toString(family), Bytes.toString(qualifier));

        if (!indexTableNames.contains(indexTableName)) {
            SidxScan sidxScan = new SidxScan().of()
                .addColumnFamily(family)
                .addQualifier(qualifier)
                .setSingleColumnValueFilter(kind, value)
                .setColumnCountGetFilter(10)
                .setPageFilter(10L)
                .buildFilter()
                .build();
            SidxResult dataTableResult = scan(sidxTable, sidxScan);
            Iterator<Result> iterator = dataTableResult.getIterator();

            List<SidxGet> list = new LinkedList<>();
            iterator.forEachRemaining(result -> {
                byte[] dataRow = result.getRow();
                SidxGet dataGet = new SidxGet().of(dataRow).build();
                list.add(dataGet);
            });

            // 根据数据表拿到的RowKey反查数据表，获取整行数据
            return get(sidxTable, list);
        } else {
            SidxScan sidxScan = new SidxScan().of()
                .setKeyOnlyFilter()
                .setRowFilter(kind, value)
                .buildFilter()
                .build();

            SidxTable indexTable = new SidxTable().of(indexTableName).build();
            SidxResult indexTableResult = scan(indexTable, sidxScan);
            Iterator<Result> iterator = indexTableResult.getIterator();

            List<SidxGet> list = new LinkedList<>();
            iterator.forEachRemaining(result -> {
                String[] indexRow = Bytes.toString(result.getRow()).split(Constants.INDEX_TABLE_NAME_SEPARATOR);
                String dataRow = indexRow[indexRow.length - 1];
                SidxGet dataGet = new SidxGet().of(Bytes.toBytes(dataRow)).build();
                list.add(dataGet);
            });

            // 根据数据表拿到的RowKey反查数据表，获取整行数据
            return get(sidxTable, list);
        }
    }

    /**
     * @param sidxTable
     * @param node
     * @return boolean
     * @description get data asynchronously
     */
    @Override
    public SidxResult getAsync(SidxTable sidxTable, SidxCall node) {
        return null;
    }

    /**
     * @param sidxTable
     * @param sidxDelete
     * @return boolean
     * @description delete data synchronously
     */
    @Override
    public boolean deleteSync(SidxTable sidxTable, SidxDelete sidxDelete) {
        SidxTableConfig tableConfig = achieveMeta(sidxTable);

        boolean delFlag = true;

        byte[] columnFamily = sidxDelete.getColumnFamily();
        byte[] qualifier = sidxDelete.getQualifier();

        SidxGet get = new SidxGet().of(sidxDelete.getDelete().getRow());
        if (columnFamily != null) {
            get.addColumnFamily(columnFamily).addQualifier(qualifier).build();
        }

        if (qualifier != null) {
            get.addQualifier(qualifier).build();
        }

        // 删除索引表
        SidxResult result = get(sidxTable, get);

        List<SidxTableConfig.TableColumn> tableColumns;

        if (columnFamily == null && qualifier == null) {
            tableColumns = tableConfig.getTableColumns().stream().filter(SidxTableConfig.TableColumn::isIndex).collect(Collectors.toList()
            );
        } else if (qualifier == null) {
            tableColumns = tableConfig.getTableColumns().stream().filter(t ->
                t.isIndex() && t.getFamily().equals(Bytes.toString(columnFamily))
            ).collect(Collectors.toList());
        } else {
            tableColumns = tableConfig.getTableColumns().stream().filter(t ->
                t.isIndex() && t.getFamily().equals(Bytes.toString(columnFamily)) && t.getQualifier().equals(Bytes.toString(qualifier))
            ).collect(Collectors.toList());
        }

        for (SidxTableConfig.TableColumn entry : tableColumns) {
            byte[] column = result.getResult().getValue(Bytes.toBytes(entry.getFamily()), Bytes.toBytes(entry.getQualifier()));
            byte[] indexRowKey = Utils.deduceIndexRowkey(sidxDelete.getDelete().getRow(), column, tableConfig.getTableConfig().getIndexTableRegions());

            String indexTableName = Utils.deduceIndexTableName(sidxTable, entry.getFamily(), entry.getQualifier());
            SidxTable indexTable = new SidxTable().of(indexTableName);
            SidxDelete delete = new SidxDelete().of(indexRowKey);

            delFlag = delFlag & delete(indexTable, delete);
        }

        // 删除数据表
        delFlag &= delete(sidxTable, sidxDelete);

        return delFlag;
    }

    /**
     * @param sidxTable
     * @param sidxDelete
     * @return boolean
     * @description delete data asynchronously
     */
    @Override
    public boolean deleteAsync(SidxTable sidxTable, SidxDelete sidxDelete) {
        return false;
    }


    /**
     * @param sidxTable
     * @param sidxUpdate
     * @return boolean
     * @description update data synchronously
     */
    @Override
    public boolean updateSync(SidxTable sidxTable, SidxUpdate sidxUpdate) {

        SidxGet get = new SidxGet().of(sidxUpdate.getUpdate().getRow());
        SidxResult getResult = get(sidxTable, get);
        if (getResult.getResult().isEmpty()) {
            return false;
        }

        SidxTableConfig tableConfig = achieveMeta(sidxTable);
        for (SidxTableConfig.TableColumn column : tableConfig.getTableColumns()) {
            byte[] family = Bytes.toBytes(column.getFamily());
            byte[] qualifier = Bytes.toBytes(column.getQualifier());
            List<Cell> cells = sidxUpdate.getUpdate().get(family, qualifier);
            if (cells == null || cells.size() == 0) {
                byte[] value = getResult.getResult().getValue(family, qualifier);
                sidxUpdate.addColumnFamily(family).addQualifier(qualifier).addValue(value).buildCell();
            }
        }

        sidxUpdate.build();

        SidxDelete delete = new SidxDelete().of(sidxUpdate.getUpdate().getRow());
        boolean flag = delete(sidxTable, delete);

        if (flag) {
            flag &= put(sidxTable, new SidxPut().copyOf(sidxUpdate.getUpdate()));
        }

        return flag;
    }

    /**
     * @param sidxTable
     * @param sidxUpdate
     * @return boolean
     * @description update data asynchronously
     */
    @Override
    public boolean updateAsync(SidxTable sidxTable, SidxUpdate sidxUpdate) {
        return false;
    }
}
