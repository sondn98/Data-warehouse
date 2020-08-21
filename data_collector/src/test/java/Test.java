import edu.hust.soict.bigdata.collector.datacollection.SchemaGenerator;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Stack;

public class Test {

    public static void main(String[] args) throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        Config.addResource("data_collector/src/main/resources/collector.properties");
        JSONObject j = new JSONObject("{\n" +
                "  \"name\": \"ModelCustom\",\n" +
                "\n" +
                "  \"hive-model\": {\n" +
                "    \"table\": \"test\",\n" +
                "    \"schema\": \"default\"\n" +
                "  },\n" +
                "\n" +
                "  \"schema\": {\n" +
                "    \"c1\": \"iNt\",\n" +
                "    \"c2\": \"string\",\n" +
                "    \"c3\": \"TIMESTAMP\"\n" +
                "  }\n" +
                "}");

//        System.out.println(SchemaGenerator.buildClass(j).newInstance().toString());
//        System.out.println(SchemaGenerator.buildSource(j));
    }
}
