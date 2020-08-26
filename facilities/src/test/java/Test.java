import edu.hust.soict.bigdata.facilities.common.util.SequenceGenerator;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaBrokerWriter;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaConfig;

import java.io.IOException;
import java.util.Properties;

public class Test {

    public static void main(String[] args) throws IOException {
        SequenceGenerator sg = new SequenceGenerator();
        for(int i = 0 ; i < 100 ; i ++)
        System.out.println(sg.nextId());
    }
}
