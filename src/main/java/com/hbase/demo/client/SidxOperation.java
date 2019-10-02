package com.hbase.demo.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hbase.demo.configuration.SidxTableConfig;
import com.hbase.demo.utils.Constants;
import com.hbase.demo.utils.Utils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

        tableConfig.getTableColumns().stream().filter(t -> t.isIndex()).forEach(t -> {

            byte[] column = dataPut.getPut().get(Bytes.toBytes(t.getFamily()), Bytes.toBytes(t.getQualifier())).get(0).getValueArray();
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
}
