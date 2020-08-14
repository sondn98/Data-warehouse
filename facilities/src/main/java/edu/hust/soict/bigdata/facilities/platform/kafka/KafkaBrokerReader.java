package edu.hust.soict.bigdata.facilities.platform.kafka;

import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author sondn on 2020/03/10
 */
public class KafkaBrokerReader {

    private static final String KEY_NUM_CONSUMERS = "kafka.num.consumers";

    protected Collection<String> topics;
    private AtomicBoolean running;

    private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerReader.class);

    public KafkaBrokerReader() {
        this.topics = Config.getCollection(Const.CONSUMER_TOPICS);
        // number executor threads equals number consumer
    }

    public void start() {
        running = new AtomicBoolean(true);

        int numConsumers = Config.getIntProperty(KEY_NUM_CONSUMERS, 1);
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            executor.submit(() -> startKafkaConsumer(topics));
        }
        executor.shutdown();
    }

    public void stop() {
        running.set(false);
        logger.info("Stopped kafka consumer reader");
    }

    private void startKafkaConsumer(Collection<String> topics) {
        while (running.get()) {
            try (KafkaConsumer<String, byte[]> consumer =
                         new KafkaConsumer<>(KafkaConfig.loadKafkaConf())) {
                consumer.subscribe(topics);
                logger.info(Thread.currentThread().getName() + " start subscribe topic: "+ topics);

                while (running.get()) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        invokeHandlers(records);
                        logger.info(records.count() + " records handled");
                        consumer.commitAsync();
                    }
                }
            } catch (Exception e) {
                logger.error("Unexpected error", e);
            }
        }
        logger.info("Consumer stopped at thread: " + Thread.currentThread().getName());
    }

    protected void setTopics(Collection<String> topics) {
        this.topics = topics;
    }

    public void invokeHandlers(ConsumerRecords<String, byte[]> records){
        // Should be overridden
    }
}
