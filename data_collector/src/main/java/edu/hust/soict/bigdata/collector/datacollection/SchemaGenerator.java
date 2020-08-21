package edu.hust.soict.bigdata.collector.datacollection;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.json.JSONObject;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SchemaGenerator {

    private static final String MODEL_NAME = "name";
    private static final String HIVE_MODEL_KEY = "hive-model";
    private static final String[] HIVE_MODEL_PROPS = {"schema", "table"};
    private static final String SCHEMA_MODEL_KEY = "schema";
    private static final Map<String, String> DATA_TYPES = new HashMap<String, String>(){
        {
            put("LONG", "Long");
            put("INT", "Integer");
            put("SHORT", "Short");
            put("FLOAT", "Float");
            put("DOUBLE", "Double");
            put("STRING", "String");
            put("BIG_DECIMAL", "BigDecimal");
            put("BOOLEAN", "Boolean");
            put("TIMESTAMP", "Timestamp");
        }
    };

    private static final ObjectMapper om = new ObjectMapper();


    public static void buildClass(JSONObject jsonSchema) throws
            IOException{
        String source = buildSource(jsonSchema);
        File root = new File(Config.getProperty(CollectorConst.COLLECTOR_SCHEMA_FOLDER));
        File sourceFile = new File(root, jsonSchema.getString(MODEL_NAME) + ".java");
        if(!sourceFile.getParentFile().exists())
            sourceFile.getParentFile().mkdirs();
        Files.write(sourceFile.toPath(), source.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, sourceFile.getPath());

//        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { root.toURI().toURL() });
//        Class<?> cls = Class.forName(jsonSchema.getString(MODEL_NAME), true, classLoader);
//        return ((T) cls.newInstance()).getClass();
    }

    @SuppressWarnings("unchecked")
    public static <T extends DataModel> T newInstance(String schemaName, JSONObject data) throws
            ClassNotFoundException, IOException {
        File root = new File(Config.getProperty(CollectorConst.COLLECTOR_SCHEMA_FOLDER));

        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { root.toURI().toURL() });
        Class<?> cls = Class.forName(schemaName, true, classLoader);
        return (T) om.readValue(data.toString(), cls);
    }

    public static String buildSource(JSONObject jsonSchema){
        StringBuilder source = new StringBuilder();

        source
                .append("import com.fasterxml.jackson.databind.ObjectMapper;").append("\n")
                .append("import com.fasterxml.jackson.core.JsonProcessingException;").append("\n")
                .append("import edu.hust.soict.bigdata.facilities.model.DataModel;").append("\n")
                .append("import edu.hust.soict.bigdata.facilities.model.HiveModel;").append("\n\n");
        source
                .append("import java.sql.Timestamp;").append("\n")
                .append("import java.math.BigDecimal;").append("\n\n");

        if(jsonSchema.has(HIVE_MODEL_KEY)){
            JSONObject hiveModel = jsonSchema.getJSONObject(HIVE_MODEL_KEY);

            List<String> hiveSchemaProps = new LinkedList<>();
            for(String p : HIVE_MODEL_PROPS){
                if(hiveModel.has(p))
                    hiveSchemaProps.add(String.format(p + " = \"%s\"", hiveModel.get(p)));
            }
            String hiveAnnot = "@HiveModel(" + String.join(", ", hiveSchemaProps) + ")";
            source
                    .append(hiveAnnot).append("\n");
        }
        source
                .append(String.format("public class %s extends DataModel {", jsonSchema.get(MODEL_NAME))).append("\n\n");

        JSONObject schemaModel = jsonSchema.getJSONObject(SCHEMA_MODEL_KEY);
        for(String field : schemaModel.keySet())
            source
                    .append(String.format("    public %s %s;", DATA_TYPES.get(schemaModel.getString(field).toUpperCase()), field)).append("\n\n");

        source
                .append("    @Override").append("\n")
                .append("    public String toString() {").append("\n")
                .append("        try {").append("\n")
                .append("           return new ObjectMapper().writeValueAsString(this);").append("\n")
                .append("        } catch (JsonProcessingException e) {").append("\n")
                .append("            return \"\";").append("\n")
                .append("        }").append("\n")
                .append("    }").append("\n");
        source
                .append("}");


        return source.toString();
    }
}
