package edu.hust.soict.bigdata.facilities.platform.kafka;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.util.Strings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaBrokerWriter implements AutoCloseable{

    private String topic;
    private Producer<String, byte[]> producer;

    private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerWriter.class);

    public KafkaBrokerWriter() {
        this.topic = Config.getProperty(Const.PRODUCER_TOPIC, "test");
        this.producer = new KafkaProducer<>(KafkaConfig.loadKafkaConf());
    }

    public KafkaBrokerWriter(String topic) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(KafkaConfig.loadKafkaConf());
    }

    public Future<RecordMetadata> write(byte[] b) {
        logger.info("Sending a message to kafka");
        return producer.send(new ProducerRecord<>(topic, b));
    }

    public Future<RecordMetadata> write(String k, String v) {
        logger.info("Sending a message to kafka");
        return producer.send(new ProducerRecord<>(topic, k, v.getBytes()));
    }

    @Override
    public void close() {
        this.producer.close();
    }

    static {
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);
    }
}
