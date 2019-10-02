package com.hbase.demo.client;

import com.hbase.demo.utils.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxOperation
 * @description: Basic operation of Sidx Table
 * @date 2019-10-01 20:48
 */
@Component
public class HbaseOperator {
    private static final Logger logger = LoggerFactory.getLogger(HbaseOperator.class);

    @Autowired
    private SidxConnection sidxConnection;

    public boolean isTableExisted(SidxTable sidxTable) {

        Connection conn = sidxConnection.getHbaseConnection();
        try (Admin admin = conn.getAdmin()) {
            if (admin.tableExists(sidxTable.getTableName())) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean createTable(SidxTable sidxTable, int regions) {
        byte[][] splitKeys = Utils.presplit(regions);
        return createTable(sidxTable, splitKeys);
    }

    public boolean createTable(SidxTable sidxTable, byte[][] splitKeys) {
        Connection conn = sidxConnection.getHbaseConnection();

        try (Admin admin = conn.getAdmin()) {
            TableDescriptor td = sidxTable.getTd();
            admin.createTable(td, splitKeys);

            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }


    public boolean deleteTable(SidxTable sidxTable) {
        Connection conn = sidxConnection.getHbaseConnection();

        try (Admin admin = conn.getAdmin()) {
            TableName tableName = sidxTable.getTableName();
            if (!admin.isTableDisabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean put(SidxTable sidxTable, SidxPut sidxPut) {
        Connection conn = sidxConnection.getHbaseConnection();

        try (Table table = conn.getTable(sidxTable.getTableName())) {
            table.put(sidxPut.getPut());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean put(SidxTable sidxTable, List<SidxPut> puts) {
        Connection conn = sidxConnection.getHbaseConnection();

        List<Put> list = new LinkedList<>();
        puts.forEach(t -> list.add(t.getPut()));

        try (Table table = conn.getTable(sidxTable.getTableName())) {
            table.put(list);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void close() {
        Connection conn = sidxConnection.getHbaseConnection();
        try {
            if (!conn.isClosed()) {
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isClosed() {
        Connection conn = sidxConnection.getHbaseConnection();
        return conn.isClosed();
    }
}