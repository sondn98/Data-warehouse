package edu.hust.soict.bigdata.collector.action;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.impl.LocalWalFile;
import edu.hust.soict.bigdata.facilities.model.WalInfo;
import edu.hust.soict.bigdata.facilities.platform.hadoop.HdfsWriter;
import edu.hust.soict.bigdata.facilities.platform.zookeeper.ZKClient;
import edu.hust.soict.bigdata.facilities.platform.zookeeper.ZookeeperClientProvider;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class ActionChecksum extends TimerTask {

    private String folder;
    private HdfsWriter hdfsWriter;
    private ZKClient zookeeperClient;

    private static final Logger logger = LoggerFactory.getLogger(ActionChecksum.class);
    private static final ObjectMapper om = new ObjectMapper();

    public ActionChecksum(String name) throws IOException {
        this.folder = Properties.getProperty(Const.LOCAL_FS_WAL_FOLDER);
        this.hdfsWriter = new HdfsWriter();

        zookeeperClient = ZookeeperClientProvider.getOrCreate(name, ZKClient.class);
    }

    @Override
    public void run() {
        logger.info("Starting checksum");
        File directory = new File(folder);
        File[] files = directory.listFiles();
        if(files != null){
            for(File file : files){
                try {
                    WalFile wal = new LocalWalFile(file.getAbsolutePath(),
                            Properties.getProperty(Const.WAL_WRITER_CODEC, "simple"),
                            Properties.getLongProperty(Const.WAL_MAX_SIZE, 1048576));
                    if(wal.exists()){
                        BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
                        long lastModified = attr.lastModifiedTime().toMillis();
                        long now = System.currentTimeMillis();
                        long expiredTime = Properties.getLongProperty(Const.WAL_EXPIRATION_TIME, 86400);
                        if(wal.isReachedLimit() || now - lastModified > expiredTime){
                            String hdfsFilePath = Properties.getProperty(Const.HDFS_DATA_FOLDER) + wal.name();
                            logger.info("---> Pushing a file on hdfs: " + hdfsFilePath);
                            logger.info("---> File size: " + wal.length());
                            logger.info("---> Last modified: " + FileTime.from(lastModified, TimeUnit.MILLISECONDS));
                            hdfsWriter.pushFile(file.getAbsolutePath(), hdfsFilePath);

                            WalInfo keeper = wal.getInfo();
                            zookeeperClient.create(
                                    Properties.getProperty(Const.ZK_INFO_HDFS_NEW_FILE_ZNODE) + wal.name(),
                                    om.writeValueAsString(keeper));
                            wal.delete();
                        }
                    }
                } catch (IOException | InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
        }
        logger.info("Checksum successfully");
    }
}
