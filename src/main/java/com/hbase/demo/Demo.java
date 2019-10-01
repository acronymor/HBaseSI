package com.hbase.demo;

import com.hbase.demo.connection.SidxConnection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author apktool
 * @title: com.hbase.demo.Demo
 * @description: TODO
 * @date 2019-09-30 22:39
 */

@Service
public class Demo {

    @Autowired
    private SidxConnection sidxConnection;

    public void start(String[] args) throws IOException {
        Connection conn = sidxConnection.getHbaseConnection();
        TableName[] tables = conn.getAdmin().listTableNames();
        Arrays.asList(tables).stream().forEach(t -> System.out.println(t.getNameAsString()));
    }
}
