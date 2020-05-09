package edu.hust.soict.bigdata.facilities.platform.hadoop;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class HdfsWriter {

    private static final String DEFAULT_FS = "hdfs://localhost:9000/";
    private FileSystem fs;

    private static final Logger logger = LoggerFactory.getLogger(HdfsWriter.class);

    public HdfsWriter(Properties props) throws IOException {
        fs = FileSystem.get(URI.create(props.getProperty(Const.HADOOP_FS_URI, DEFAULT_FS)), new Configuration());
        logger.info("Created FileSystem on hdfs: " + fs.getUri().toString());
    }

    public boolean write(String filePath, String data) {
        Path file = new Path(fs.getUri() + filePath);

        try {
            FSDataOutputStream os = fs.create(file, true);

            os.write(data.getBytes());
            os.close();
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    public boolean pushFile(String source, String target){
        try {
            fs.copyFromLocalFile(new Path(source), new Path(fs.getUri() + "/" + target));
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
