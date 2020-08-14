package edu.hust.soict.bigdata.speed.action;

import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.elasticsearch.ESRepository;

import java.util.List;

public class ActionChecksum<M extends DataModel> extends ActionReadBroker<M> {

    private String onFailureTopic;
    private ESRepository<M> esRepository;

    private Config props;

    public ActionChecksum(Config props, String KEY_TOPICS) {
        super(KEY_TOPICS);
        this.esRepository = new ESRepository<M>("Checksum-topic-records-on-failure") {};

        this.props = props;
    }

    @Override
    public void save(List<M> product) {
        esRepository.bulkInsert(product, Config.getProperty(""));
    }
}
