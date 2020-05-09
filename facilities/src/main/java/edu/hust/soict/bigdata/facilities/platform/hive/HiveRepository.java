package edu.hust.soict.bigdata.facilities.platform.hive;

import com.google.common.reflect.TypeToken;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.common.exceptions.CommonException;
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

    private HiveModel hiveModel;
    protected String tableName;

    protected static Connection connection;
    protected Properties props;

    private static final Logger logger = LoggerFactory.getLogger(HiveRepository.class);

    @SuppressWarnings("unchecked")
    public HiveRepository(Properties props) throws ClassNotFoundException, SQLException {
        Class<M> clazz = (Class<M>)Class.forName(classTypeParameter());
        if(clazz.isAnnotationPresent(HiveModel.class)){
            this.hiveModel = clazz.getAnnotation(HiveModel.class);
            String table = props.getProperty(Const.HIVE_TABLE);
            String schema = props.getProperty(Const.HIVE_SCHEMA);
            this.tableName = schema + "." + table;

            if(connection == null)
                connection = HiveConnectionProvider.getOrCreate("default", props);
            this.props = props;
        } else{
            throw new RuntimeException("Type parameter must be annotated by HiveModel annotation");
        }
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
            logger.error(CommonException.getMessage(e));
        }
    }

    public <T> M getOneById(T id) throws SQLException {
        String query = "SELECT * FROM " + tableName + " WHERE " + hiveModel.id() + "=?";
        PreparedStatement ps = connection.prepareStatement(query);
        HiveUtils.setParam(ps, 1, id);

        MyResultSet rs = new MyResultSet(ps.executeQuery());
        return resultToObject(rs);
    }

    public <T> void delete(T id) throws SQLException {
        String query = "DELETE FROM " + tableName + " WHERE " + hiveModel.id() + "=?";
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
