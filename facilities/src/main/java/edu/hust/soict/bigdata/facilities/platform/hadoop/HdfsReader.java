package edu.hust.soict.bigdata.facilities.platform.hadoop;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class HdfsReader {
    private static final String DEFAULT_FS = "hdfs://localhost:9000/";
    private static FileSystem fs;

    private static final Logger logger = LoggerFactory.getLogger(HdfsReader.class);

    public HdfsReader() throws IOException {
        if(fs == null)
            fs = FileSystem.get(URI.create(
                    Properties.getProperty(Const.HADOOP_FS_URI, DEFAULT_FS)), new Configuration());
        logger.info("Created FileSystem on hdfs: " + fs.getUri().toString());
    }

    public static FileSystem getFs(){
        return fs;
    }
}
