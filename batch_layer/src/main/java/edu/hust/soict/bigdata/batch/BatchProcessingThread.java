package edu.hust.soict.bigdata.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.batch.common.BatchConst;
import edu.hust.soict.bigdata.batch.handler.Handler;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.model.WalInfo;
import edu.hust.soict.bigdata.facilities.platform.hadoop.HdfsReader;
import edu.hust.soict.bigdata.facilities.platform.zookeeper.ZKClient;
import edu.hust.soict.bigdata.facilities.platform.zookeeper.ZookeeperClientProvider;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchProcessingThread<M extends DataModel> implements Runnable{

    private static ExecutorService service;
    private static HdfsReader reader;
    private static Long handleInterval;
    private static Long bulkloadTimeout;

    private AtomicBoolean running;

    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(BatchProcessingThread.class);

    public BatchProcessingThread() throws IOException {
        if(service == null)
            service = Executors.newFixedThreadPool(Properties.getIntProperty(BatchConst.PROCESSING_THREAD_COUNT, 10));
        if(reader == null)
            reader = new HdfsReader();
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        bulkloadTimeout = Properties.getLongProperty(BatchConst.PROCESSING_THREAD_BULKLOAD_TIMEOUT, 600000);
        running = new AtomicBoolean(true);
        handleInterval = Properties.getLongProperty(BatchConst.PROCESSING_THREAD_INTERVAL, 100);
    }

    private void handle(){
        try{
            ZKClient zkClient = ZookeeperClientProvider.getOrCreate(Properties.getProperty(Const.ZK_CLIENT_NAME), ZKClient.class);
            String zNodeParent = Properties.getProperty(Const.ZK_INFO_HDFS_NEW_FILE_ZNODE);
            String data = zkClient.getDataAndDelete(zNodeParent);
            if(data == null)
                return;

            WalInfo keeper = om.readValue(data, WalInfo.class);
            logger.info("Processing file: " + keeper.filePath);
            Future<?> fut = service.submit(() -> {
                Handler<M> handler;
                handler = HandlerFactory.getHandler(keeper);
                handler.handle();
            });
            try {
                fut.get(bulkloadTimeout, TimeUnit.MILLISECONDS);
                logger.info("---> File " + data + " processed successfully");
            } catch (ExecutionException e) {
                e.printStackTrace();
                logger.error("---> File " + data + " failed to processed. Trying to create znode for next processing");
                String[] path = keeper.filePath.split("/");
                String fileName = path[path.length - 1];
                zkClient.create(Properties.getProperty(Const.ZK_INFO_HDFS_NEW_FILE_ZNODE) + "/" + fileName, data);
                logger.info("Created znode for next processing");
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        } catch (IOException | KeeperException | InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void run() {
        logger.info("Start processing thread");
        while(running.get()){
            logger.info("Processor is running normally");
            handle();
            try {
                Thread.sleep(handleInterval);
            } catch (InterruptedException e) {
                logger.warn(e.getLocalizedMessage());
            }
        }
        while(service.isTerminated()){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.warn(e.getLocalizedMessage());
            }
        }
        service.shutdown();
        logger.info("Processing thread stopped");
    }

    public void stop(){
        logger.info("Stopping processing thread");
        running.set(false);
    }
}
