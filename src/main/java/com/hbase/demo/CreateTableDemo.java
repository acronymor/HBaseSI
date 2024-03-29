package com.hbase.demo;

import com.hbase.demo.client.SidxOperation;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author apktool
 * @title com.hbase.demo.CreateTableDemo
 * @description TODO
 * @date 2019-10-06 11:42
 */
@Service
public class CreateTableDemo {
    @Setter(onMethod = @__({@Autowired}))
    private SidxOperation sidxOperation;

    /**
     * @param args
     * @throws IOException
     */
    public void start(String[] args) throws IOException {
        createTableSyncDemo();
        createTableAsyncDemo();
    }

    private void createTableSyncDemo() {
        boolean flag = sidxOperation.createTableSync();
        System.out.println(flag);
    }

    private void createTableAsyncDemo() {
        boolean flag = sidxOperation.createTableAsync();
        System.out.println(flag);
    }
}
