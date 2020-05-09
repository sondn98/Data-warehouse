package edu.hust.soict.bigdata.facilities.platform.hbase;

import com.google.common.reflect.TypeToken;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.common.util.Reflects;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

public abstract class HbaseRepository<T extends DataModel> implements AutoCloseable{

    private String namespace;
    private String tableName;

    protected static Connection connection;
    protected byte[] cf;
    protected Properties props;

    private static Logger logger = LoggerFactory.getLogger(HbaseRepository.class);

    public HbaseRepository(Properties props) {
        if(null == connection)
            connection = HBaseConnectionProvider.getDefault(HBaseConfig.loadConfig());
        this.tableName = props.getProperty(Const.HBASE_TABLE);
        this.namespace = props.getProperty(Const.HBASE_SCHEMA);
        this.cf = props.getProperty(Const.HBASE_COLUMN_FAMILY).getBytes();
        this.props = props;

        logger.info("Created repository on table " + tableName);
    }

    public int add(List<T> objects){
        try (Table table = getTable()) {
            List<Put> puts = new ArrayList<>();
            for (T obj : objects) {
                puts.add(createPut(obj));
            }
            table.put(puts);
            logger.info("Put " + objects.size() + " record(s) to hbase");
            return objects.size();
        }catch (Exception e){
            logger.info("Fail to add record into hbase");
            logger.error(e.getMessage());
        }

        return -1;
    }

    public List<T> getAll(){
        try (Table table = getTable()){
            Scan scan = new Scan();
            ResultScanner rsScan = table.getScanner(scan);
            List<T> rsList = new LinkedList<>();
            for(Result rs : rsScan){
                rsList.add(resultToObject(rs));
            }

            return rsList;
        } catch (IOException | IllegalAccessException e) {
            logger.info(e.getMessage());
        }

        return null;
    }

    public T getOneById(byte[] rowkey){
        try (Table table = getTable()) {
            Get get = new Get(rowkey);
            Result rs = table.get(get);

            return resultToObject(rs);
        }catch (Exception e){
            logger.info("Fail to get data from hbase");
            logger.error(e.getMessage());
        }

        return null;
    }

    public void deleteOneById(byte[] rowkey){
        try (Table table = getTable()) {
            Delete delete = new Delete(rowkey);
            table.delete(delete);
        }catch (Exception e){
            logger.info("Fail to delete record from hbase");
            logger.error(e.getMessage());
        }
    }

    public List<T> getManyByField(String fieldName, byte[] value){
        List<T> objects = new LinkedList<>();
        try(Table table = getTable()){
            Scan scan = new Scan();
            scan.setFilter(
                    new SingleColumnValueFilter(cf, Bytes.toBytes(fieldName), CompareOperator.EQUAL, value));

            ResultScanner rss = table.getScanner(scan);
            rss.iterator().forEachRemaining(rs -> {
                try {
                    objects.add(resultToObject(rs));
                } catch (IllegalAccessException e) {
                    logger.warn("Got a failure on converting result scanner to object");
                }
            });
        } catch (IOException e) {
            logger.error("Fail to get record from hbase");
        }

        return objects;
    }

    protected T resultToObject(Result rs) throws IllegalAccessException {
        T obj = Reflects.newInstance(classTypeParameter());

        NavigableMap<byte[], byte[]> rsMap = rs.getFamilyMap(cf);
        Field[] fields = obj.getClass().getFields();

        for (Field field : fields) {
            if(!rsMap.containsKey(field.getName().getBytes())){
                field.set(obj, null);
            } else{
                byte[] value = rsMap.get(field.getName().getBytes());
                field.set(obj, HBaseUtils.fromBytes(value, field.getType()));
            }
        }

        return obj;
    }

    protected Put createPut(T obj) throws IllegalAccessException {
        byte[] rowkey = buildRowKey(obj);

        Put put = new Put(rowkey);
        Field[] fields = obj.getClass().getFields();

        for (Field field : fields) {
            if (field.get(obj) != null)
                put.addColumn(cf, Bytes.toBytes(field.getName()),
                        HBaseUtils.toBytes(field.get(obj)));
        }

        return put;
    }

    protected static byte[] buildRowKey(String seed, byte[]... components){
        return HBaseUtils.buildCompositeKeyWithBucket(seed, components);
    }

    protected abstract byte[] buildRowKey(T obj);

    protected Table getTable() throws IOException {
        return connection.getTable(TableName.valueOf(namespace + ":" + tableName));
    }

    @SuppressWarnings("UnstableApiUsage")
    private String classTypeParameter(){
        return new TypeToken<T>(getClass()) { }.getType().getTypeName();
    }

    @Override
    public void close() {
        try {
            if (!connection.isClosed()) connection.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
