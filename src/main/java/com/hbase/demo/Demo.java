package com.hbase.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author apktool
 * @title: com.hbase.demo.Demo
 * @description: TODO
 * @date 2019-09-30 22:39
 */

@Service
public class Demo {

    @Autowired
    private CreateTableDemo createTableDemo;

    @Autowired
    private GetDemo getDemo;

    @Autowired
    private PutDemo putDemo;

    /**
     * @param args
     * @throws IOException
     */
    public void start(String[] args) throws IOException {
        createTableDemo.start(args);
        putDemo.start(args);
        getDemo.start(args);
    }
}
