package edu.hust.soict.bigdata.facilities.platform.kafka;

import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
    private static Properties kafkaProps;

    public static Properties loadKafkaConf(){
        if(kafkaProps == null || kafkaProps.isEmpty()){
            String kafkaConfigFile = Config.getProperty(Const.KAFKA_CONFIG_FILE, "kafka-conf.properties");
            try {
                kafkaProps = Config.loadDefault(kafkaConfigFile);

                String externalConfFolder = System.getProperty(Const.CONFIG_FOLDER);
                if(externalConfFolder != null) {
                    kafkaProps.load(new InputStreamReader(
                            new FileInputStream(Strings.concatFilePath(externalConfFolder, kafkaConfigFile)),
                            StandardCharsets.UTF_8));
                }

                return kafkaProps;
            } catch (IOException e) {
                logger.error("Can not load kafka properties", e);
                return new Properties();
            }
        } else {
          return kafkaProps;
        }
    }
}
