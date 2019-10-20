package com.hbase.demo.condition;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author apktool
 * @title com.hbase.demo.condition.SidxCall
 * @description TODO
 * @date 2019-10-06 11:59
 */
@Getter
@AllArgsConstructor
public class SidxCall extends AbstractSidxNode {
    private final SidxOperator operator;

    private final AbstractSidxNode[] operators;

}
