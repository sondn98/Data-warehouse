package edu.hust.soict.bigdata.facilities.platform.hadoop;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

public class HdfsConnectionProvider extends ObjectPool<FileSystem> {

    private static HdfsConnectionProvider hdfsProvider;

    public static HdfsConnectionProvider getInstance(){
        if(hdfsProvider == null)
            hdfsProvider = new HdfsConnectionProvider();
        return hdfsProvider;
    }

    @Override
    protected FileSystem create() {
        try {
            logger.info("Creating connection to HDFS");
            return FileSystem.get(URI.create(
                    Config.getProperty(Const.HADOOP_FS_URI, "hdfs://localhost:9000/")), new Configuration());
        } catch (IOException e) {
            logger.info("Can not create connection to HDFS");
            return null;
        }
    }
}
