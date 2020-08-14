package edu.hust.soict.bigdata.facilities.common.config;

import com.google.inject.internal.util.$AsynchronousComputationException;
import edu.hust.soict.bigdata.facilities.common.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Config {

    private static final String CONFIG_FILE = "config.properties";

    private static Map<String, Object> runtimeObj = new HashMap<>();
    private static Properties props;

    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    static {
        try {
            props = loadDefault(CONFIG_FILE);
            String externalConfFolder = System.getProperty(Const.CONFIG_FOLDER);
            if(externalConfFolder != null)
                addResource(Strings.concatFilePath(externalConfFolder, CONFIG_FILE));
        } catch (IOException e) {
            logger.info("Can not load properties", e);
        }
    }

    public static Properties loadDefault(String confFile) throws IOException {
        Properties p = new Properties();
        p.load(new InputStreamReader(
                Objects.requireNonNull(Config.class.getClassLoader()
                        .getResourceAsStream(confFile)),
                StandardCharsets.UTF_8));
        return p;
    }

    public static void addResource(String configFilePath){
        try {
            props.load(new InputStreamReader(
                    new FileInputStream(configFilePath),
                    StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("Can not load config from " + configFilePath);
        }
    }

    public static String getProperty(String key){
        return props.getProperty(key);
    }

    public static String getProperty(String key, String def){
        String val = props.getProperty(key);
        return val == null ? def : val;
    }

    /**
     * Lấy về giá trị có kiểu int
     *
     * @param key    key của giá trị cần lấy
     * @param defVal giá trị mặc định trả về
     * @return số int của giá trị cần lấy hoặc giá trị mặc định nếu
     * không tìm thấy giá trị nào ứng với key cần tìm
     */
    public static int getIntProperty(String key, int defVal) {
        try {
            return Integer.parseInt(props.getProperty(key));
        } catch (Exception ignored) {
            return defVal;
        }
    }

    /**
     * Lấy về giá trị có kiểu long
     *
     * @param key    key của giá trị cần lấy
     * @param defVal giá trị mặc định trả về
     * @return số long của giá trị cần lấy hoặc giá trị mặc định nếu
     * không tìm thấy giá trị nào ứng với key cần tìm
     */
    public static long getLongProperty(String key, long defVal) {
        try {
            return Long.parseLong(props.getProperty(key));
        } catch (Exception ignored) {
            return defVal;
        }
    }

    /**
     * Lấy về giá trị có kiểu double
     *
     * @param key    key của giá trị cần lấy
     * @param defVal giá trị mặc định trả về
     * @return số double của giá trị cần lấy hoặc giá trị mặc định nếu
     * không tìm thấy giá trị nào ứng với key cần tìm
     */
    public static double getDoubleProperty(String key, double defVal) {
        try {
            return Double.parseDouble(props.getProperty(key));
        } catch (Exception ignored) {
            return defVal;
        }
    }

    /**
     * Lấy về giá trị có kiểu bool
     *
     * @param key    key của giá trị cần lấy
     * @param defVal giá trị mặc định trả về
     * @return số double của giá trị cần lấy hoặc giá trị mặc định nếu
     * không tìm thấy giá trị nào ứng với key cần tìm
     */
    public static boolean getBoolProperty(String key, boolean defVal) {
        try {
            return Boolean.parseBoolean(props.getProperty(key));
        } catch (Exception ignored) {
            return defVal;
        }
    }

    /**
     * Lấy về giá trị có kiểu list String
     *
     * @param key key của giá trị cần lấy
     * @return list String các giá trị được phân cách bởi dấu phẩy
     */
    public static List<String> getCollection(String key) {
        try {
            return Arrays.asList(props.getProperty(key).split(","));
        } catch (Exception ignored) {
            return Collections.emptyList();
        }
    }

    /**
     * Lấy về giá trị có kiểu list String
     *
     * @param key       key của giá trị cần lấy
     * @param delimiter chuỗi dùng để phân tách các giá trị
     * @return list String các giá trị được phân cách bởi delimiter
     */
    public static List<String> getCollection(String key, String delimiter) {
        try {
            return Arrays.asList(props.getProperty(key).split(delimiter));
        } catch (Exception ignored) {
            return Collections.emptyList();
        }
    }

    /**
     * Lấy về giá trị có kiểu DateTime theo format yyyy-MM-dd HH:mm:ss
     *
     * @param key    key của giá trị cần lấy
     * @param defVal giá trị mặc định trả về
     * @return object Date của giá trị cần lấy hoặc giá trị mặc định nếu
     * không tìm thấy giá trị nào ứng với key cần tìm hoặc value
     * ở dạng raw không phù hợp để convert thành Date
     */
    public static Date getDateTime(String key, Date defVal) {
        return getDateTime(key, "yyyy-MM-dd HH:mm:ss", defVal);
    }

    /**
     * Lấy về giá trị có kiểu DateTime
     *
     * @param key    key của giá trị cần lấy
     * @param format format để convert từ giá trị raw String về object Date
     * @param defVal giá trị mặc định trả về
     * @return object Date của giá trị cần lấy hoặc giá trị mặc định nếu
     * không tìm thấy giá trị nào ứng với key cần tìm hoặc value
     * ở dạng raw không phù hợp để convert thành Date
     */
    public static Date getDateTime(String key, String format, Date defVal) {
        try {
            DateFormat df = new SimpleDateFormat(format);
            return df.parse(props.getProperty(key));
        } catch (Exception e) {
            return defVal;
        }
    }

    /**
     * Lấy về một hoặc nhiều class
     *
     * @param key key của giá trị cần lấy
     * @return danh sách các object Classs
     * @throws ClassNotFoundException nếu có lỗi xảy ra khi convert
     *                                từ tên class thành object Class
     */
    public static Collection<Class<?>> getClasses(String key) throws ClassNotFoundException {
        List<Class<?>> classes = new LinkedList<>();
        for (String className : getCollection(key)) {
            classes.add(Class.forName(className));
        }
        return classes;
    }

    /**
     * Lấy ra một object trong quá trình chạy của hệ thống
     * @param key property's key
     * @param clazz object class
     * @return
     */
    public static <T> T getRuntimeObj(String key, Class<T> clazz){
        Object obj = runtimeObj.get(key);
        if(!obj.getClass().isAssignableFrom(clazz))
            throw new RuntimeException("Fail to get runtime object. Parsing error");

        return clazz.cast(obj);
    }

    /**
     * Set một object vào config trong quá trình chạy của hệ thống
     * @param key
     * @param obj
     */
    public static void setRuntimeObj(String key, Object obj){
        runtimeObj.put(key, obj);
    }

    public static Properties getProps(){
        return props;
    }

    public static Properties particularProps(String configFilePath, boolean includeDefault){
        Properties p = includeDefault ? new Properties(props) : new Properties();
        try {
            p.load(new InputStreamReader(
                    new FileInputStream(configFilePath),
                    StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("Can not load config from " + configFilePath);
        }

        return p;
    }

    public static String toStr(){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Defined properties: \n");
        for(Map.Entry<Object, Object> kv : props.entrySet()){
            stringBuilder.append("\t");
            stringBuilder.append(kv.getKey());
            stringBuilder.append(" = ");
            stringBuilder.append(kv.getValue());
            stringBuilder.append("\n");
        }

        return stringBuilder.toString();
    }
}
