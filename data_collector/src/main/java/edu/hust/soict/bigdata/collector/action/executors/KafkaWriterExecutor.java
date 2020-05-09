package edu.hust.soict.bigdata.collector.action.executors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.common.exceptions.CommonException;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaBrokerWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaWriterExecutor<M extends DataModel> implements Runnable{

    private M data;
    private Properties props;

    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(KafkaWriterExecutor.class);

    public KafkaWriterExecutor(Properties props, M data){
        this.props = props;
        this.data = data;
    }

    @Override
    public void run() {
        try (KafkaBrokerWriter writer = new KafkaBrokerWriter(props)){
            String message = om.writeValueAsString(data);
            String id = data.getId();
            writer.write(id, message);
            logger.info("Write data to kafka successfully: " + message);
        } catch (JsonProcessingException e) {
            logger.error(new CommonException(e).getMessage());
        }
    }
}
