package edu.hust.soict.bigdata.facilities.platform.hbase;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author sondn
 * @since 2020/06/25
 */
public class HBaseConnectionProvider extends ObjectPool<Connection> {

    @Override
    protected Connection create() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path(Properties.getProperty(Const.HBASE_CONFIG_FILE)));
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
