package com.hbase.demo;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author apktool
 * @title com.hbase.demo.Demo
 * @description TODO
 * @date 2019-09-30 22:39
 */

@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class Demo {

    private final CreateTableDemo createTableDemo;

    private final GetDemo getDemo;

    private final PutDemo putDemo;

    private final DeleteDemo deleteDemo;

    private final UpdateDemo updateDemo;

    private final ScanDemo scanDemo;

    /**
     * @param args
     * @throws IOException
     */
    public void start(String[] args) throws IOException {
        createTableDemo.start(args);
        putDemo.start(args);
        getDemo.start(args);
        deleteDemo.start(args);
        updateDemo.start(args);
        scanDemo.start(args);
    }
}
