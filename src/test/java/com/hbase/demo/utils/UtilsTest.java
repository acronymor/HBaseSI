package com.hbase.demo.utils;

import org.junit.Assert;
import org.junit.Test;


/**
 * @author apktool
 * @title: com.hbase.demo.utils.UtilsTest
 * @description: TODO
 * @date 2019-10-02 14:12
 */
public class UtilsTest {

    @Test
    public void buildIndexRowkey() {
        byte[] rowKey = "salt_c1_row".getBytes();
        byte[] column = "c1".getBytes();
        int bucketsNum = 1;

        byte[] result = Utils.deduceIndexRowkey(rowKey, column, bucketsNum);
        byte[] bytes = {0x0, 0x5f, 0x63, 0x31, 0x5f, 0x73, 0x61, 0x6c, 0x74, 0x5f, 0x63, 0x31, 0x5f, 0x72, 0x6f, 0x77, 0x5f};

        Assert.assertArrayEquals(result, bytes);
    }

    @Test
    public void presplit() {
        int regions = 5;
        byte[][] bytes = {{1}, {2}, {3}, {4}};
        byte[][] result = Utils.presplit(regions);
        Assert.assertArrayEquals(result, bytes);
    }
}
