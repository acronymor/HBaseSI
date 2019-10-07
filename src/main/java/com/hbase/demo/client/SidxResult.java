package com.hbase.demo.client;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

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

    @Getter
    private Iterator<Result> iterator;

    public SidxResult of(Result result) {
        this.result = result;
        return this;
    }

    public SidxResult of(Iterator<Result> iterator) {
        this.iterator = iterator;
        return this;
    }

    /**
     * @param append
     * @return SidxResult
     * @description: Obtain union result between two SidxResults
     */
    public SidxResult addAll(SidxResult append) {
        LinkedHashMap<String, Result> map = new LinkedHashMap<>();

        Iterator<Result> itr = append.getIterator();
        if (itr != null) {
            itr.forEachRemaining(t -> map.put(Bytes.toString(t.getRow()), t));
        }

        if (iterator != null) {
            iterator.forEachRemaining(t -> map.put(Bytes.toString(t.getRow()), t));
        }

        iterator = map.values().iterator();

        return this;
    }

    /**
     * @param retain
     * @return SidxResult
     * @description: Obtain intersection result between two SidxResults
     */
    public SidxResult retainAll(SidxResult retain) {
        LinkedHashMap<String, Result> map = new LinkedHashMap<>();
        List<Result> list = new LinkedList<>();

        Iterator<Result> itr = retain.getIterator();
        if (itr != null) {
            itr.forEachRemaining(t -> map.put(Bytes.toString(t.getRow()), t));
        }

        if (iterator != null) {
            iterator.forEachRemaining(t -> {
                if (map.containsKey(Bytes.toString(t.getRow()))) {
                    list.add(t);
                }
            });
        }

        map.clear();

        iterator = list.iterator();

        return this;
    }

    public SidxResult build() {
        return this;
    }
}
