package edu.hust.soict.bigdata.facilities.platform.hive;

import com.google.common.reflect.TypeToken;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.util.Reflects;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.model.HiveModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public abstract class HiveRepository<M extends DataModel> implements AutoCloseable{

    protected String tableName;

    protected Connection connection;

    private static final Logger logger = LoggerFactory.getLogger(HiveRepository.class);

    @SuppressWarnings("unchecked")
    public HiveRepository() throws ClassNotFoundException {
        Class<M> clazz = (Class<M>)Class.forName(classTypeParameter());
        if(clazz.isAnnotationPresent(HiveModel.class)){
            HiveModel hiveModel = clazz.getAnnotation(HiveModel.class);
            this.tableName = hiveModel.schema() + "." + hiveModel.table();

            connection = HiveConnectionProvider
                    .getInstance()
                    .getOrCreate(Config.getProperty(Const.HIVE_CLIENT_CONNECTION_NAME, "default"));
        } else{
            throw new RuntimeException("Type parameter must be annotated by HiveModel annotation");
        }
    }

    public boolean reconnect(){
        connection = HiveConnectionProvider
                .getInstance()
                .getOrCreate(Config.getProperty(Const.HIVE_CLIENT_CONNECTION_NAME, "default"));
        return connection != null;
    }

    public void add(M data) throws SQLException, IllegalAccessException {
        if(data == null) return;

        Field[] field = data.getClass().getFields();
        String query = "INSERT INTO TABLE " + tableName + " VALUES (" + String.join(",", Collections.nCopies(field.length, "?")) + ")";
        PreparedStatement ps = connection.prepareStatement(query);

        for(int i = 0 ; i < field.length ; i ++){
            HiveUtils.setParam(ps, i + 1, field[i].get(data));
        }

        ps.executeUpdate();
    }

    public void add(List<M> data) throws SQLException, IllegalAccessException {
        if(data == null || data.isEmpty())
            return;

        Field[] field = data.get(0).getClass().getFields();
        String query = "INSERT INTO TABLE " + tableName +
                String.join(" UNION ", "SELECT " + String.join(",", Collections.nCopies(field.length, "?")));

        PreparedStatement ps = connection.prepareStatement(query);
        for(int i = 0 ; i < data.size() ; i ++){
            for(int j = 0 ; j < field.length ; j ++){
                HiveUtils.setParam(ps, i*field.length + j, field[j].get(data));
            }
        }

        ps.executeLargeUpdate();
    }

    public void add(String path) {
        try{
            String query = "LOAD DATA INPATH ? INTO TABLE " + tableName;
            PreparedStatement ps = connection.prepareStatement(query);
            ps.setString(1, path);

            ps.executeUpdate();
        } catch (SQLException e){
            logger.error("Error while pushing file to hive", e);
        }
    }

    public M getOneById(String id) throws SQLException {
        String query = "SELECT * FROM " + tableName + " WHERE id=?";
        PreparedStatement ps = connection.prepareStatement(query);
        HiveUtils.setParam(ps, 1, id);

        MyResultSet rs = new MyResultSet(ps.executeQuery());
        return resultToObject(rs);
    }

    public <T> void delete(T id) throws SQLException {
        String query = "DELETE FROM " + tableName + " WHERE id=?";
        PreparedStatement ps = connection.prepareStatement(query);
        HiveUtils.setParam(ps, 1, id);

        ps.executeUpdate();

    }

    protected M resultToObject(MyResultSet rs) throws SQLException {
        M obj = Reflects.newInstance(classTypeParameter());
        Field[] fields = obj.getClass().getFields();

        for (Field field : fields) {
            HiveUtils.getValue(rs, field.getName(), field.getType());
        }

        return obj;
    }

    @SuppressWarnings("UnstableApiUsage")
    private String classTypeParameter(){
        return new TypeToken<M>(getClass()) { }.getType().getTypeName();
    }

    @Override
    public void close() throws SQLException {
        if(connection != null)
            connection.close();
    }
}
