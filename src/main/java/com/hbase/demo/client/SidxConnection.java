package com.hbase.demo.client;

import com.hbase.demo.configuration.HbaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author apktool
 * @title: com.hbase.demo.connection.SidxConnection
 * @description: Create HBase common connection
 * @date 2019-09-30 21:41
 */

@Component
public class SidxConnection {

    final static Logger logger = LoggerFactory.getLogger(SidxConnection.class);

    @Autowired
    private HbaseConfig hbaseConfig;

    private Connection hbaseConnection;

    public Connection getHbaseConnection() {
        if (hbaseConnection == null) {
            Configuration conf = HBaseConfiguration.create();

            conf.setInt("hbase.zookeeper.property.clientPort", hbaseConfig.hbaseZkClientPort);
            conf.set("hbase.zookeeper.quorum", hbaseConfig.hbaseZkQuorumm);

            if (logger.isDebugEnabled()) {
                Iterator iterator = conf.iterator();
                while (iterator.hasNext()) {
                    logger.debug(String.valueOf(iterator.next()));
                }
            }

            logger.info("Initialize configuration of hbase finished");
            try {
                hbaseConnection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        return hbaseConnection;
    }
}
