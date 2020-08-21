package edu.hust.soict.bigdata.facilities.platform.hadoop;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Hdfs {

    private FileSystem fs;

    private static final Logger logger = LoggerFactory.getLogger(Hdfs.class);

    public Hdfs(String name) {
        fs = HdfsConnectionProvider
                .getInstance()
                .getOrCreate(name);
        logger.info("Created FileSystem on hdfs: " + fs.getUri().toString());
    }

    public boolean reconnect(){
        fs = HdfsConnectionProvider
                .getInstance()
                .getOrCreate(Config.getProperty(Const.HDFS_CLIENT_CONNECTION_NAME, "default"));
        return fs != null;
    }

    public boolean pushFile(String source, String target){
        try {
            fs.copyFromLocalFile(new Path(source), new Path(fs.getUri() + target));
            return true;
        } catch (IOException e) {
            logger.error("Error while push file to hdfs", e);
            return false;
        }
    }

    public boolean pullFile(String source, String target){
        try {
            fs.copyToLocalFile(new Path(source), new Path(fs.getUri() + target));
            return true;
        } catch (IOException e) {
            logger.error("Error while download file from hdfs", e);
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
