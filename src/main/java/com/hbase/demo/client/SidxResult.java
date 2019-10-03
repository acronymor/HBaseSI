package com.hbase.demo.client;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxResult
 * @description: TODO
 * @date 2019-10-03 10:08
 */
@NoArgsConstructor
public class SidxResult {
    @Getter
    private Result result;

    public SidxResult of(Result result) {
        this.result = result;
        return this;
    }

    public SidxResult build() {
        return this;
    }
}
