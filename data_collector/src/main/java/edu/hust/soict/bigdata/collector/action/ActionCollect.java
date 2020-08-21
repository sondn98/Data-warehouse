package edu.hust.soict.bigdata.collector.action;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.collector.datacollection.CollectorJobAttributes;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.exceptions.WalException;
import edu.hust.soict.bigdata.facilities.common.wal.WalFactory;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalWriter;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaBrokerWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.*;

public class ActionCollect implements Closeable {

    private CollectorJobAttributes attributes;
    private WalFile wal;

    private static ExecutorService executorService;
    private static final Logger logger = LoggerFactory.getLogger(ActionCollect.class);
    private static final ObjectMapper om = new ObjectMapper();

    public ActionCollect(CollectorJobAttributes attributes){
        if(executorService == null || executorService.isTerminated())
            executorService = Executors.newFixedThreadPool(attributes.NUM_COLLECTOR_THREADS);

        this.wal = WalFactory.getShortestWalFile(attributes.LOCAL_WAL_FOLDER);
    }

    public <M extends DataModel> void handle(M data) {
        if(!wal.exists() || wal.isReachedLimit()) {
            wal = WalFactory.getShortestWalFile(attributes.LOCAL_WAL_FOLDER);
        }

        if(attributes.ENABLE_BATCH){
            Future<?> futWalRs =  executorService.submit(() -> {
                try (WalWriter<M> writer = WalFactory.getWriter(wal)){
                    writer.append(data);
                } catch (IOException e) {
                    logger.error("Error while write data to wal", new WalException(e));
                }
            });
            try {
                futWalRs.get(Config.getIntProperty(CollectorConst.ACTION_WRITE_WAL_TIMEOUT, 3000), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Error while writing record to wal", e);
                futWalRs.cancel(true);
            }
        }

        if(attributes.ENABLE_STREAMING){
            Future<?> futKafkaRs = executorService.submit(() -> {
                try (KafkaBrokerWriter writer = new KafkaBrokerWriter(attributes.KAFKA_TOPIC)){
                    String message = om.writeValueAsString(data);
                    String id = data.getId();
                    writer.write(id, message);
                    logger.info("Write data to kafka successfully: " + message);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            });
            try {
                futKafkaRs.get(Config.getIntProperty(CollectorConst.ACTION_WRITE_KAFKA_TIMEOUT, 3000), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Error while writing messages to kafka broker", e);
                futKafkaRs.cancel(true);
            }
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
        try {
            while(executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)){
                logger.info("Stopping action collect");
            }
        } catch (InterruptedException e) {
            logger.error("Something went wrong", e);
        }
    }
}
