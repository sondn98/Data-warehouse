package edu.hust.soict.bigdata.serving.action;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.model.Record;
import edu.hust.soict.bigdata.facilities.model.Records;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaBrokerReader;
import edu.hust.soict.bigdata.serving.justification.Justification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public abstract class ActionReadBroker<M extends DataModel> extends KafkaBrokerReader implements Justification<M> {

    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ActionReadBroker.class);

    private Properties props;

    public ActionReadBroker(Properties props, String KEY_TOPICS){
        super(props);
        setTopics(props.getCollection(KEY_TOPICS));
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        this.props = props;
    }

    @Override
    public void invokeHandlers(Records records) {
        List<M> objs = new LinkedList<>();
        for(Record rc : records){
            String data = new String(rc.data());
            logger.info("Got: " + data);
            try {
                M obj = om.readValue(data, new TypeReference<M>() {});
                objs.add(obj);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        handle(objs, props);
        save(objs);
    }

    @Override
    public abstract void save(List<M> product);
}
