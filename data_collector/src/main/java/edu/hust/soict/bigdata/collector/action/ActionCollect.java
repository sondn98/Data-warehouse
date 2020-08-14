package edu.hust.soict.bigdata.collector.action;

import edu.hust.soict.bigdata.collector.action.executors.KafkaWriterExecutor;
import edu.hust.soict.bigdata.collector.action.executors.WalWriterExecutor;
import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.wal.WalFactory;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ActionCollect<M extends DataModel>{

    private String currentWalFolder;
    private Boolean activeWal;
    private Boolean activeKafka;

    private WalFile wal;

    private static ExecutorService executorService;
    private static final Logger logger = LoggerFactory.getLogger(ActionCollect.class);

    public ActionCollect(){
        if(null == executorService)
            executorService = Executors.newFixedThreadPool(
                    Config.getIntProperty(CollectorConst.ACTION_WRITE_EXECUTOR_POOL_SIZE, 10));

        this.activeKafka = Config.getBoolProperty(CollectorConst.ACTION_WRITE_KAFKA_ACTIVE, true);
        this.activeWal = Config.getBoolProperty(CollectorConst.ACTION_WRITE_WAL_ACTIVE, false);
        this.currentWalFolder = Config.getProperty(Const.LOCAL_FS_WAL_FOLDER);
        this.wal = WalFactory.getShortestWalFile(currentWalFolder);
        logger.info("Specified wal folder: " + this.currentWalFolder);
    }

    public void handle(M data) {
        if(!wal.exists() || wal.isReachedLimit()) {
            wal = WalFactory.getShortestWalFile(currentWalFolder);
        }

        if(activeWal){
            Future<?> futWalRs =  executorService.submit(new WalWriterExecutor<>(wal, data));
            try {
                futWalRs.get(Config.getIntProperty(CollectorConst.ACTION_WRITE_WAL_TIMEOUT, 3000), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Error while writing record to wal", e);
                futWalRs.cancel(true);
            }
        }

        if(activeKafka){
            Future<?> futKafkaRs = executorService.submit(new KafkaWriterExecutor<>(data));
            try {
                futKafkaRs.get(Config.getIntProperty(CollectorConst.ACTION_WRITE_KAFKA_TIMEOUT, 3000), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Error while writing messages to kafka broker", e);
                futKafkaRs.cancel(true);
            }
        }
    }
}
