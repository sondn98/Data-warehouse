package edu.hust.soict.bigdata.facilities.platform.hbase;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author sondn
 * @since 2020/06/25
 */
public class HBaseConnectionProvider extends ObjectPool<Connection> {

    private static HBaseConnectionProvider hProvider;

    public static HBaseConnectionProvider getInstance(){
        if(hProvider == null)
            hProvider = new HBaseConnectionProvider();
        return hProvider;
    }

    @Override
    protected Connection create() {
        logger.info("Creating connection to Hbase");
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path(Config.getProperty(Const.HBASE_CONFIG_FILE)));
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.error("Error while creating connection to hbase", e);
            return null;
        }
    }
}
