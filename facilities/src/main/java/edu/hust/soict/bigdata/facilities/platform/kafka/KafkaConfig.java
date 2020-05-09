package edu.hust.soict.bigdata.facilities.platform.kafka;

import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.common.exceptions.CommonException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class KafkaConfig {

    private static final String PRODUCER_ENV_KEY = "kafka.producer.conf";
    private static final String CONSUMER_ENV_KEY = "kafka.consumer.conf";

    public static Properties producerProperties() {
        return load(PRODUCER_ENV_KEY, "kafka-producer.properties");
    }

    public static Properties consumerProperties() {
        return load(CONSUMER_ENV_KEY, "kafka-consumer.properties");
    }

    private static Properties load(String envKey, String defaultFileInClassPath) {
        final String path = System.getProperty(envKey);
        if (path != null) return loadPropsOrDefault(path);

        try {
            final ClassLoader cl = KafkaConfig.class.getClassLoader();
            return loadProps(cl.getResourceAsStream(defaultFileInClassPath));
        } catch (IOException e) {
            throw new CommonException("Cannot find kafka properties in classpath", e);
        }
    }

    private static Properties loadPropsOrDefault(String path) {
        try {
            return loadProps(new FileInputStream(path));
        } catch (IOException e) {
            return new Properties();
        }
    }


    private static Properties loadProps(InputStream in) throws IOException {
        Properties p = new Properties();
        p.load(in);
        return p;
    }
}
