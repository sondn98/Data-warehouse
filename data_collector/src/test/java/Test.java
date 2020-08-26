import edu.hust.soict.bigdata.collector.datacollection.SchemaGenerator;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaBrokerWriter;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaConfig;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Stack;

public class Test<T extends DataModel> {

    public static void main(String[] args) throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        KafkaBrokerWriter writer = new KafkaBrokerWriter();

    }
}
