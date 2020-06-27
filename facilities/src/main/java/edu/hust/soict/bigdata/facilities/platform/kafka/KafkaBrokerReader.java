package edu.hust.soict.bigdata.facilities.platform.kafka;

import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.model.Record;
import edu.hust.soict.bigdata.facilities.model.Records;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author sondn on 2020/03/10
 */
public class KafkaBrokerReader{

    private static final String KEY_MIN_RECORDS = "kafka.min.records";
    private static final String KEY_TOPICS = "kafka.consumer.topics";
    private static final String KEY_NUM_CONSUMERS = "kafka.num.consumers";
    private static final String KEY_MONITOR_RATE = "kafka.consumer.mornitor.rate";

    protected Collection<String> topics;
    private int numConsumers;
    private int minRecords; // min number record to try retrieve before sent to handlers
    private ExecutorService executor;
    private Double timeMornitor;
    private AtomicBoolean running;

    private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerReader.class);

    public KafkaBrokerReader() {
        this.configure();
    }

    private void configure() {
        this.topics = Properties.getCollection(KEY_TOPICS);
        this.minRecords = Properties.getIntProperty(KEY_MIN_RECORDS, 1);
        this.timeMornitor = Properties.getDoubleProperty(KEY_MONITOR_RATE, 0.1);

        this.numConsumers = Properties.getIntProperty(KEY_NUM_CONSUMERS, Runtime.getRuntime().availableProcessors());
        // number executor threads equals number consumer
        this.executor = Executors.newFixedThreadPool(numConsumers);
    }

    public void start() {
        running = new AtomicBoolean(true);
        for (int i = 0; i < numConsumers; i++) {
            executor.submit(() -> startKafkaConsumer(topics));
        }
    }

    public void stop() {
        running.set(false);
        logger.info("Stop Kafka queue reader");
        executor.shutdown();
    }

    private void startKafkaConsumer(Collection<String> topics) {
        while (running.get()) {
            try (KafkaConsumer<String, byte[]> consumer =
                         new KafkaConsumer<>(Properties.getProps())) {
                consumer.subscribe(topics);
                logger.info(Thread.currentThread().getName() + " start subscribe topic: "+ topics);

                Records queueRecords = new Records();
                while (running.get()) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(100);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        queueRecords.add(new Record(record.value(), record.timestamp()));
                    }
                    if (queueRecords.size() >= minRecords) {
                        invokeHandlers(queueRecords);
                        logger.info(queueRecords.size() + " records handled");
                        consumer.commitAsync();
                        queueRecords.clear();
                    } else if(new Random().nextFloat() < timeMornitor){
                        logger.info("Consumer is still alive!");
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

    public void invokeHandlers(Records records){
        // Should be overridden
    }
}
