package com.hbase.demo.client;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author apktool
 * @title: com.hbase.demo.client.SidxOperatorNode
 * @description: TODO
 * @date 2019-10-06 11:59
 */
@Getter
@AllArgsConstructor
public class SidxOperatorNode {

    private SidxCompareOperatorKind kind;

    private byte[] familyIdentifier;

    private byte[] columnIdentifier;

    private byte[] literal;

    /**
     * Wrapper of CompareOperator
     */
    public enum SidxCompareOperatorKind {
        /*
         * EQUAL             |   ==
         * GREATER           |   >
         * GREATER_OR_EQUAL  |   >=
         * LESS              |   <
         * LESS_OR_EQUAL     |   <=
         * NOT_EQUAL         |   !=
         * NO_OP
         */
        EQUAL,
        GREATER,
        GREATER_OR_EQUAL,
        LESS,
        LESS_OR_EQUAL,
        NOT_EQUAL,
        NO_OP;
    }
}
