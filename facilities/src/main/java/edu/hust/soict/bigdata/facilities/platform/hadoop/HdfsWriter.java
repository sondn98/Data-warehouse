package edu.hust.soict.bigdata.facilities.platform.hadoop;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HdfsWriter {

    private FileSystem fs;

    private static final Logger logger = LoggerFactory.getLogger(HdfsWriter.class);

    public HdfsWriter() {
        fs = HdfsConnectionProvider
                .getInstance()
                .getOrCreate(Config.getProperty(Const.HDFS_CLIENT_CONNECTION_NAME, "default"));
        logger.info("Created FileSystem on hdfs: " + fs.getUri().toString());
    }

    public boolean pushFile(String source, String target){
        try {
            fs.copyFromLocalFile(new Path(source), new Path(fs.getUri() + target));
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    public boolean detele(String filePath){
        try {
            return fs.deleteOnExit(new Path(filePath));
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    public void close() throws IOException {
        fs.close();
    }
}
