package com.hbase.demo.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hbase.demo.condition.*;
import com.hbase.demo.configuration.SidxTableConfig;
import com.hbase.demo.utils.Constants;
import com.hbase.demo.utils.Utils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxOperation
 * @description: Basic operation of Sidx Table
 * @date 2019-10-01 20:48
 */
@Component
public class SidxOperation {
    private static final Logger logger = LoggerFactory.getLogger(SidxOperation.class);

    @Autowired
    private HbaseOperator operator;


    @Autowired
    private SidxTableConfig tableConfig;

    /**
     * @return boolean
     * @description: Create Table from SidxTableConfig
     */
    public boolean createTable() {
        boolean flag = true;
        flag &= createDataTable();
        flag &= createIndexTable();
        flag &= createMetaTable();
        return flag;
    }

    /**
     * @return boolean
     * @description create index table
     */
    private boolean createDataTable() {
        String dataTableName = tableConfig.getTableName();

        SidxTable dataTable = new SidxTable().of(dataTableName);

        boolean flag = true;

        Set<String> sets = new HashSet<>();
        tableConfig.getTableColumns().forEach(t -> sets.add(t.getFamily()));

        for (String item : sets) {
            dataTable.addColumnFamily(item)
                .setDataBlockEncoding(tableConfig.getTableConfig().getDataTableBlockEncoding())
                .setCompressType(tableConfig.getTableConfig().getDataTableCompression())
                .setBlockSize(tableConfig.getTableConfig().getDataTableBlockSize())
                .buildCF();
        }

        dataTable.build();

        if (!operator.isTableExisted(dataTable)) {
            flag = operator.createTable(dataTable, tableConfig.getTableConfig().getDataTableRegions());
        }

        if (!flag) {
            logger.error("DataTable [" + dataTableName + "] have been created failed");
            return false;
        }

        logger.info("DataTable [" + dataTableName + "] have been created successfully");
        return true;
    }

    /**
     * @return boolean
     * @description create index table
     */
    private boolean createIndexTable() {
        List<String> indexTableNames = Utils.deduceIndexTableNames(tableConfig);

        boolean flag = indexTableNames.stream().map(t ->
            new SidxTable().of(t)
                .addColumnFamily(Constants.INDEX_TABLE_COLUMN_FAMILY)
                .setDataBlockEncoding(tableConfig.getTableConfig().getIndexTableBlockEncoding())
                .setCompressType(tableConfig.getTableConfig().getIndexTableCompression())
                .setBlockSize(tableConfig.getTableConfig().getIndexTableBlockSize())
                .buildCF()
                .build())
            .filter(indexTable -> !operator.isTableExisted(indexTable))
            .map(indexTable -> operator.createTable(indexTable, tableConfig.getTableConfig().getIndexTableRegions()))
            .reduce(true, (a, b) -> a && b);

        if (!flag) {
            logger.error("IndexTables " + indexTableNames + " have been created failed");
        }
        logger.info("IndexTables " + indexTableNames + " have been created successfully");

        return flag;
    }

    /**
     * @return boolean
     * @description create meta table
     */
    private boolean createMetaTable() {
        byte[] tableConfigs = new byte[0];
        byte[] tableColumns = new byte[0];

        try {
            ObjectMapper mapper = new ObjectMapper();
            tableConfigs = mapper.writeValueAsBytes(tableConfig.getTableConfig());
            tableColumns = mapper.writeValueAsBytes(tableConfig.getTableColumns());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        String row = tableConfig.getTableName();
        SidxTable metaTable = new SidxTable().of(Constants.META_TABLE_NAME)
            .addColumnFamily(Constants.META_TABLE_COLUMN_FAMILY)
            .buildCF()
            .build();

        SidxPut metaPut = new SidxPut().of(Bytes.toBytes(row))
            .addColumnFamily(Bytes.toBytes(Constants.META_TABLE_COLUMN_FAMILY))
            .addQualifier(Bytes.toBytes(Constants.META_TABLE_COLUMN_QUALIFIER_TABLE_CONFIG))
            .addValue(tableConfigs)
            .buildCell()
            .addQualifier(Bytes.toBytes(Constants.META_TABLE_COLUMN_QUALIFIER_TABLE_COLUMNS))
            .addValue(tableColumns)
            .buildCell()
            .build();

        boolean flag = true;
        if (!operator.isTableExisted(metaTable)) {
            flag &= operator.createTable(metaTable, 1);
        }

        if (!flag) {
            logger.error("MetaTable [" + Constants.META_TABLE_NAME + "] have been created failed");
            return false;
        }
        logger.info("MetaTable [" + Constants.META_TABLE_NAME + "] have been created successfully");

        flag = flag & operator.put(metaTable, metaPut);

        if (!flag) {
            logger.error("MetaData " + row + " have been put failed");
        }
        logger.info("MetaData [" + row + "] have been put successfully");

        return true;
    }

    public boolean put(SidxTable sidxTable, SidxPut dataPut) {
        operator.put(sidxTable, dataPut);

        SidxTableConfig tableConfig = achieveMeta(sidxTable);

        tableConfig.getTableColumns().stream().filter(t -> t.isIndex()).forEach(t -> {

            List<Cell> list = dataPut.getPut().get(Bytes.toBytes(t.getFamily()), Bytes.toBytes(t.getQualifier()));
            Cell item = list.get(0);

            int offset = item.getValueOffset();
            int len = item.getValueLength();
            byte[] column = Arrays.copyOfRange(item.getRowArray(), offset, offset + len);
            byte[] indexRowKey = Utils.deduceIndexRowkey(dataPut.getPut().getRow(), column, tableConfig.getTableConfig().getIndexTableRegions());

            SidxPut indexPut = new SidxPut().of(indexRowKey)
                .addColumnFamily(Bytes.toBytes(Constants.INDEX_TABLE_COLUMN_FAMILY))
                .addQualifier(Bytes.toBytes(Constants.INDEX_TABLE_COLUMN_QUALIFIER))
                .addValue(Bytes.toBytes(""))
                .buildCell()
                .build();

            String indexTableName = Utils.deduceIndexTableName(tableConfig.getTableName(), t.getFamily(), t.getQualifier());
            SidxTable indexTable = new SidxTable().of(indexTableName);

            operator.put(indexTable, indexPut);
        });

        return true;
    }

    /**
     * @param table
     * @return SidxTableConfig
     * @description: get meta from sidx.meta.table
     */
    public SidxTableConfig achieveMeta(SidxTable table) {
        SidxTable metaTable = new SidxTable().of(Constants.META_TABLE_NAME)
            .addColumnFamily(Constants.META_TABLE_COLUMN_FAMILY)
            .buildCF()
            .build();

        SidxGet get = new SidxGet()
            .of(table.getTableName().getName())
            .build();

        SidxResult result = get(metaTable, get);

        List<SidxTableConfig.TableColumn> columns = new ArrayList<>();
        SidxTableConfig.TableConfig config = new SidxTableConfig.TableConfig();
        try {
            ObjectMapper mapper = new ObjectMapper();

            byte[] configsValue = result.getResult().getValue(Bytes.toBytes(Constants.META_TABLE_COLUMN_FAMILY), Bytes.toBytes(Constants.META_TABLE_COLUMN_QUALIFIER_TABLE_CONFIG));
            config = mapper.readValue(configsValue, SidxTableConfig.TableConfig.class);

            byte[] columnsValue = result.getResult().getValue(Bytes.toBytes(Constants.META_TABLE_COLUMN_FAMILY), Bytes.toBytes(Constants.META_TABLE_COLUMN_QUALIFIER_TABLE_COLUMNS));
            columns = mapper.readValue(columnsValue, mapper.getTypeFactory().constructCollectionType(List.class, SidxTableConfig.TableColumn.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        SidxTableConfig meta = new SidxTableConfig();
        meta.setTableName(table.getTableName().getNameAsString());
        meta.setTableConfig(config);
        meta.setTableColumns(columns);

        return meta;
    }

    /**
     * @param sidxTable
     * @param sidxGet
     * @return SidxResult
     * @description: get data from table
     */
    public SidxResult get(SidxTable sidxTable, SidxGet sidxGet) {
        SidxResult result = operator.get(sidxTable, sidxGet);
        return result;
    }

    /**
     * @param sidxTable
     * @param sidxGets
     * @return SidxResult
     * @description: get any data from table
     */
    public SidxResult get(SidxTable sidxTable, List<SidxGet> sidxGets) {
        SidxResult result = operator.get(sidxTable, sidxGets);
        return result;
    }

    /**
     * @param sidxTable
     * @param node
     * @return SidxResult
     * @description: get data from index table and data table
     */
    public SidxResult get(SidxTable sidxTable, SidxCall node) {
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
            SidxResult dataTableResult = operator.scan(sidxTable, sidxScan);
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
            SidxResult indexTableResult = operator.scan(indexTable, sidxScan);
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
     * @param sidxDelete
     * @return boolean
     * @description: delete data from index table and data table
     */
    public boolean delete(SidxTable sidxTable, SidxDelete sidxDelete) {
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
            tableColumns = tableConfig.getTableColumns().stream().filter(t ->
                t.isIndex()).collect(Collectors.toList()
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

            delFlag = delFlag & operator.delete(indexTable, delete);
        }

        // 删除数据表
        delFlag &= operator.delete(sidxTable, sidxDelete);

        return delFlag;
    }
}
