package edu.hust.soict.bigdata.speed.action;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaBrokerReader;
import edu.hust.soict.bigdata.speed.justification.Justification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public abstract class ActionReadBroker<M extends DataModel> extends KafkaBrokerReader implements Justification<M> {

    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ActionReadBroker.class);

    public ActionReadBroker(String KEY_TOPICS){
        super();
        setTopics(Config.getCollection(KEY_TOPICS));
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    @Override
    public void invokeHandlers(ConsumerRecords<String, byte[]> records) {
        List<M> objs = new LinkedList<>();
        for(ConsumerRecord<String, byte[]> rc : records){
            String data = new String(rc.value());
            logger.info("Got: " + data);
            try {
                M obj = om.readValue(data, new TypeReference<M>() {});
                objs.add(obj);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        handle(objs);
        save(objs);
    }

    @Override
    public abstract void save(List<M> product);
}
