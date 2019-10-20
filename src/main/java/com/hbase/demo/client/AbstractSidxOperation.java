package com.hbase.demo.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hbase.demo.condition.SidxCall;
import com.hbase.demo.configuration.SidxTableConfig;
import com.hbase.demo.utils.Constants;
import com.hbase.demo.utils.Utils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author apktool
 * @title com.hbase.demo.client.AbstractSidxOperation
 * @description Basic operation of SidxTable
 * @date 2019-10-20 10:21
 */
@Slf4j
@Service
public abstract class AbstractSidxOperation {

    @Setter(onMethod = @__({@Autowired}))
    private HbaseOperator operator;

    @Setter(onMethod = @__({@Autowired}))
    private SidxTableConfig tableConfig;

    /**
     * Create tables synchronously
     *
     * @return boolean
     */
    public abstract boolean createTableSync();

    /**
     * Create tables asynchronously
     *
     * @return boolean
     */
    public abstract boolean createTableAsync();

    /**
     * @return boolean
     * @description Create data tables
     */
    protected boolean createDataTable() {
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
            log.error("DataTable [" + dataTableName + "] have been created failed");
            return false;
        }

        log.info("DataTable [" + dataTableName + "] have been created successfully");
        return true;
    }

    /**
     * @return boolean
     * @description create index table
     */
    protected boolean createIndexTable() {
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
            log.error("IndexTables " + indexTableNames + " have been created failed");
        }
        log.info("IndexTables " + indexTableNames + " have been created successfully");

        return flag;
    }

    /**
     * @return boolean
     * @description Create meta table
     */
    protected boolean createMetaTable() {
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
            log.error("MetaTable [" + Constants.META_TABLE_NAME + "] have been created failed");
            return false;
        }
        log.info("MetaTable [" + Constants.META_TABLE_NAME + "] have been created successfully");

        flag = flag & operator.put(metaTable, metaPut);

        if (!flag) {
            log.error("MetaData " + row + " have been put failed");
        }
        log.info("MetaData [" + row + "] have been put successfully");

        return true;
    }


    /**
     * @param table
     * @return SidxTableConfig
     * @description Get meta from sidx.meta.table
     */
    protected SidxTableConfig achieveMeta(SidxTable table) {
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
     * @param sidxPut
     * @return boolean
     */
    protected boolean put(SidxTable sidxTable, SidxPut sidxPut) {
        return operator.put(sidxTable, sidxPut);
    }

    /**
     * @param sidxTable
     * @param sidxPut
     * @return boolean
     */
    public boolean put(SidxTable sidxTable, List<SidxPut> sidxPut) {
        return operator.put(sidxTable, sidxPut);
    }

    /**
     * Put data synchronously
     *
     * @param sidxTable
     * @param sidxPut
     * @return boolean
     */
    public abstract boolean putSync(SidxTable sidxTable, SidxPut sidxPut);

    /**
     * Put data asynchronously
     *
     * @param sidxTable
     * @param sidxPut
     * @return boolean
     */
    public abstract boolean putAsync(SidxTable sidxTable, SidxPut sidxPut);


    /**
     * Get data from table
     *
     * @param sidxTable
     * @param sidxGet
     * @return SidxResult
     */
    protected SidxResult get(SidxTable sidxTable, SidxGet sidxGet) {
        return operator.get(sidxTable, sidxGet);
    }

    /**
     * Get any data from table
     *
     * @param sidxTable
     * @param sidxGets
     * @return SidxResult
     */
    public SidxResult get(SidxTable sidxTable, List<SidxGet> sidxGets) {
        return operator.get(sidxTable, sidxGets);
    }

    /**
     * Get data synchronously
     *
     * @param sidxTable
     * @param node
     * @return boolean
     */
    public abstract SidxResult getSync(SidxTable sidxTable, SidxCall node);

    /**
     * Get data asynchronously
     *
     * @param sidxTable
     * @param node
     * @return boolean
     */
    public abstract SidxResult getAsync(SidxTable sidxTable, SidxCall node);

    /**
     * @param sidxTable
     * @param sidxScan
     * @return SidxResult
     * @description Scan data from data table
     */
    public SidxResult scan(SidxTable sidxTable, SidxScan sidxScan) {
        return operator.scan(sidxTable, sidxScan);
    }

    /**
     * @param sidxTable
     * @param sidxDelete
     * @return boolean
     * @description Delete data from index table and data table
     */
    protected boolean delete(SidxTable sidxTable, SidxDelete sidxDelete) {
        return operator.delete(sidxTable, sidxDelete);
    }

    /**
     * Delete data synchronously
     *
     * @param sidxTable
     * @param sidxDelete
     * @return boolean
     */
    public abstract boolean deleteSync(SidxTable sidxTable, SidxDelete sidxDelete);

    /**
     * Delete data asynchronously
     *
     * @param sidxTable
     * @param sidxDelete
     * @return boolean
     */
    public abstract boolean deleteAsync(SidxTable sidxTable, SidxDelete sidxDelete);

    /**
     * @param sidxTable
     * @param sidxUpdate
     * @return boolean
     */
    protected boolean update(SidxTable sidxTable, SidxUpdate sidxUpdate) {
        return put(sidxTable, new SidxPut().copyOf(sidxUpdate.getUpdate()));
    }

    /**
     * update data synchronously
     *
     * @param sidxTable
     * @param sidxUpdate
     * @return boolean
     */
    public abstract boolean updateSync(SidxTable sidxTable, SidxUpdate sidxUpdate);

    /**
     * update data asynchronously
     *
     * @param sidxTable
     * @param sidxUpdate
     * @return boolean
     */
    public abstract boolean updateAsync(SidxTable sidxTable, SidxUpdate sidxUpdate);
}
