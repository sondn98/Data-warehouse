package edu.hust.soict.bigdata.collector.datacollection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.collector.action.ActionCollect;
import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.util.LoopableLifeCircle;
import edu.hust.soict.bigdata.facilities.common.util.Strings;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.impl.LocalWalFile;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.model.WalInfo;
import edu.hust.soict.bigdata.facilities.platform.hadoop.Hdfs;
import edu.hust.soict.bigdata.facilities.platform.zookeeper.ZKClient;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

public class CollectorJob extends LoopableLifeCircle implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CollectorJob.class);
    private static final ObjectMapper om = new ObjectMapper();

    private CollectorJobAttributes attributes;
    private ActionCollect collector;

    private Hdfs hdfs;
    private ZKClient zkClient;

    public CollectorJob() throws KeeperException, InterruptedException, JsonProcessingException {
        super();
        this.attributes = Config.getRuntimeObj(CollectorConst.COLLECTOR_JOB_ATTR_OBJECT_KEY, CollectorJobAttributes.class);
        collector = new ActionCollect(attributes);
        this.hdfs = new Hdfs(attributes.HDFS_CONNECTION_NAME);
        this.zkClient = new ZKClient(attributes.ZK_CONNECTION_NAME);

        String node = Strings.concatFilePath(
                Config.getProperty(CollectorConst.COLLECTION_JOB_ZK_PARENT), CollectorConst.HOST_NAME, attributes.SCHEMA_NAME);
        if(!zkClient.exists(node)) {
            zkClient.create(node, om.writeValueAsString(attributes));
            logger.info("Created job node: " + node);
        } else
            zkClient.setData(node, om.writeValueAsString(attributes));
    }

    public <M extends DataModel> void collect(M data){
        collector.handle(data);
    }

    @Override
    public void run() {
        try {
            if(!zkClient.exists(Config.getProperty(Const.ZK_INFO_HDFS_NEW_FILE_ZNODE))){
                zkClient.createRecursive(Config.getProperty(Const.ZK_INFO_HDFS_NEW_FILE_ZNODE));
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error while creating znode for wal file moved to hdfs", e);
        }

        File directory = new File(attributes.LOCAL_WAL_FOLDER);
        File[] files = directory.listFiles();
        if(files != null){
            for(File file : files){
                try {
                    WalFile wal = new LocalWalFile(file.getAbsolutePath(),
                            Config.getProperty(Const.WAL_WRITER_CODEC, "simple"),
                            Config.getLongProperty(Const.WAL_MAX_SIZE, 1048576));
                    if(wal.exists()){
                        BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
                        long lastModified = attr.lastModifiedTime().toMillis();
                        long now = System.currentTimeMillis();
                        long expiredTime = Config.getLongProperty(Const.WAL_EXPIRATION_TIME, 86400);
                        if(wal.isReachedLimit() || now - lastModified > expiredTime){
                            if(attributes.ENABLE_BATCH){
                                String hdfsFilePath = Strings.concatFilePath(attributes.HDFS_WAL_FOLDER, attributes.SCHEMA_NAME, wal.name());
                                logger.info("---> Pushing a file on hdfs: " + hdfsFilePath);
                                logger.info("---> File size: " + wal.length());
                                logger.info("---> Last modified: " + FileTime.from(lastModified, TimeUnit.MILLISECONDS));
                                hdfs.pushFile(file.getAbsolutePath(), hdfsFilePath);

                                WalInfo keeper = wal.getInfo();
                                //TODO: KeeperErrorCode = NoNode for /test/wal-963600270058688646.log

                                zkClient.create(
                                        Strings.concatFilePath(Config.getProperty(Const.ZK_INFO_HDFS_NEW_FILE_ZNODE), wal.name()),
                                        om.writeValueAsString(keeper));
                            }
                            wal.delete();
                        }
                    }
                } catch (IOException | InterruptedException | KeeperException e) {
                    logger.error("Error while processing wal file", e);
                }
            }
        }
        try {
            Thread.sleep(attributes.COLLECT_INTERVAL);
        } catch (InterruptedException e) {
            logger.error("Something went wrong", e);
        }
    }

    @Override
    public void close() throws IOException {
        this.collector.close();
        hdfs.close();
        try {
            zkClient.close();
        } catch (InterruptedException e) {
            logger.info("Error while closing zookeeper session", e);
        }
    }
}
