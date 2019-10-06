package com.hbase.demo.condition;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

/**
 * @author apktool
 * @title: com.hbase.demo.condition.SidxIdentifier
 * @description: TODO
 * @date 2019-10-06 21:30
 */
@Getter
@AllArgsConstructor
public class SidxIdentifier extends AbstractSidxNode {
    @NonNull
    private byte[] familyIdentifier;

    @NonNull
    private byte[] columnIdentifier;
}
