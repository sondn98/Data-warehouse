package edu.hust.soict.bigdata.facilities.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Properties extends java.util.Properties{
    private static final String DEFAULT_CONFIG = "config.properties";

    private static final Logger logger = LoggerFactory.getLogger(Properties.class);

    public Properties(){
        try {
            load(new InputStreamReader(
                    Objects.requireNonNull(Properties
                            .class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG)),
                    StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("Can not load config from config.properties");
        }
    }

    public Properties addResource(String configFilePath){
        try {
            load(new InputStreamReader(
                    Objects.requireNonNull(Properties
                            .class.getClassLoader().getResourceAsStream(configFilePath)),
                    StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("Can not load config from " + configFilePath);
        }

        return this;
    }

    /**
     * Tạo ra SubProperties từ Properties hiện tại với group là null
     *
     * @param prefix tên của SubProperties cần tạo
     * @return new object SubProperties
     */
    public SubProperties toSubProperties(String prefix) {
        return new SubProperties(prefix, this);
    }

    /**
     * Lấy về giá trị có kiểu int
     *
     * @param key    key của giá trị cần lấy
     * @param defVal giá trị mặc định trả về
     * @return số int của giá trị cần lấy hoặc giá trị mặc định nếu
     * không tìm thấy giá trị nào ứng với key cần tìm
     */
    public int getIntProperty(String key, int defVal) {
        try {
            return Integer.parseInt(getProperty(key));
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
    public long getLongProperty(String key, long defVal) {
        try {
            return Long.parseLong(getProperty(key));
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
    public double getDoubleProperty(String key, double defVal) {
        try {
            return Double.parseDouble(getProperty(key));
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
    public boolean getBoolProperty(String key, boolean defVal) {
        try {
            return Boolean.parseBoolean(getProperty(key));
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
    public List<String> getCollection(String key) {
        try {
            return Arrays.asList(getProperty(key).split(","));
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
    public List<String> getCollection(String key, String delimiter) {
        try {
            return Arrays.asList(getProperty(key).split(delimiter));
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
    public Date getDateTime(String key, Date defVal) {
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
    public Date getDateTime(String key, String format, Date defVal) {
        try {
            DateFormat df = new SimpleDateFormat(format);
            return df.parse(getProperty(key));
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
    public Collection<Class<?>> getClasses(String key) throws ClassNotFoundException {
        List<Class<?>> classes = new LinkedList<>();
        for (String className : getCollection(key)) {
            classes.add(Class.forName(className));
        }
        return classes;
    }

    @Override
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Defined properties: \n");
        for(Map.Entry<Object, Object> kv : this.entrySet()){
            stringBuilder.append("\t");
            stringBuilder.append(kv.getKey());
            stringBuilder.append(" = ");
            stringBuilder.append(kv.getValue());
            stringBuilder.append("\n");
        }

        return stringBuilder.toString();
    }
}
