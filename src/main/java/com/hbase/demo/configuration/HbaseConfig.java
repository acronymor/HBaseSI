package com.hbase.demo.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

/**
 * @author apktool
 * @title: com.hbase.demo.configuration.HbaseConfig
 * @description: Build HBase config from hbase.properties
 * @date 2019-09-30 22:45
 */

@PropertySources({
    @PropertySource(value = "file:${user.dir}/config/hbase.properties", ignoreResourceNotFound = true, encoding = "UTF-8"),
    @PropertySource(value = "classpath:hbase.properties", ignoreResourceNotFound = true, encoding = "UTF-8")
})
@Configuration
public class HbaseConfig {
    @Value("${hbase.zookeeper.property.clientPort}")
    public Integer hbaseZkClientPort;

    @Value("${hbase.zookeeper.quorumm}")
    public String hbaseZkQuorumm;
}
