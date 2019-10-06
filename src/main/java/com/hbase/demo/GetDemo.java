package com.hbase.demo;

import com.hbase.demo.client.SidxOperation;
import com.hbase.demo.client.SidxResult;
import com.hbase.demo.client.SidxTable;
import com.hbase.demo.condition.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Iterator;

/**
 * @author apktool
 * @title: com.hbase.demo.GetDemo
 * @description: TODO
 * @date 2019-10-06 11:47
 */
@Service
public class GetDemo {

    @Autowired
    private SidxOperation sidxOperation;

    public void start(String[] args) {
        SidxTable table = new SidxTable().of("test");

        /* c1:f2 = 1L */

        Long value = 1L;

        SidxIdentifier identifier = new SidxIdentifier(Bytes.toBytes("f2"), Bytes.toBytes("c1"));

        SidxLiteral literal = new SidxLiteral(Bytes.toBytes(value));

        AbstractSidxNode[] operators = new AbstractSidxNode[2];
        operators[0] = identifier;
        operators[1] = literal;

        SidxOperator operator = new SidxOperator("EQUAL", SidxOperator.SidxKind.EQUAL);

        SidxCall call = new SidxCall(operator, operators);

        SidxResult sidxResult = sidxOperation.get(table, call);
        Iterator<Result> iterator = sidxResult.getIterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            System.out.println(new String(result.getRow()));
            byte[] data = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
            System.out.println(Bytes.toInt(data));
        }
    }
}


