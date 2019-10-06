package com.hbase.demo.condition;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author apktool
 * @title: com.hbase.demo.condition.SidxOperator
 * @description: TODO
 * @date 2019-10-06 21:08
 */
@Getter
@AllArgsConstructor
public class SidxOperator {
    private String name;

    private SidxKind kind;

    public enum SidxKind {
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
        NO_OP,

        AND,
        OR;
    }
}
