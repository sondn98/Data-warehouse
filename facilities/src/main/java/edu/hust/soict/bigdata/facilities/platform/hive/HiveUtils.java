package edu.hust.soict.bigdata.facilities.platform.hive;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class HiveUtils {

    public static PreparedStatement setParam(PreparedStatement ps, int index, Object value) throws SQLException {
        Class<?> type = value.getClass();
        if (type.isAssignableFrom(Long.class)) {
            ps.setLong(index, (Long)value);
        } else if (type.isAssignableFrom(Integer.class)) {
            ps.setInt(index, (Integer)value);
        } else if (type.isAssignableFrom(Short.class)) {
            ps.setShort(index, (Short)value);
        } else if (type.isAssignableFrom(Float.class)) {
            ps.setFloat(index, (Float) value);
        } else if (type.isAssignableFrom(Double.class)) {
            ps.setDouble(index, (Double) value);
        } else if (type.isAssignableFrom(String.class)) {
            ps.setString(index, (String)value);
        } else if (type.isAssignableFrom(BigDecimal.class)) {
            ps.setBigDecimal(index, (BigDecimal) value);
        } else if (type.isAssignableFrom(Boolean.class)) {
            ps.setBoolean(index, (Boolean) value);
        } else if (type.isAssignableFrom(Blob.class)) {
            ps.setBlob(index, (Blob) value);
        } else if(type.isAssignableFrom(Timestamp.class)){
            ps.setTimestamp(index, (Timestamp) value);
        } else
            ps.setString(index, value.toString());

        return ps;
    }

    public static <T> Object getValue(MyResultSet rs, String fieldName, Class<T> clazz) throws SQLException {
        if (clazz.isAssignableFrom(Long.class)) {
            return rs.getLong(fieldName);
        } else if (clazz.isAssignableFrom(Integer.class)) {
            return rs.getInt(fieldName);
        } else if (clazz.isAssignableFrom(Short.class)) {
            return rs.getShort(fieldName);
        } else if (clazz.isAssignableFrom(Float.class)) {
            return rs.getFloat(fieldName);
        } else if (clazz.isAssignableFrom(Double.class)) {
            return rs.getDouble(fieldName);
        } else if (clazz.isAssignableFrom(String.class)) {
            return rs.getString(fieldName);
        } else if (clazz.isAssignableFrom(BigDecimal.class)) {
            return rs.getLong(fieldName);
        } else if (clazz.isAssignableFrom(Boolean.class)) {
            return rs.getBoolean(fieldName);
        } else if (clazz.isAssignableFrom(Blob.class)) {
            return rs.getBlob(fieldName);
        } else if (clazz.isAssignableFrom(Timestamp.class)) {
            return rs.getTimestamp(fieldName);
        } else
            return rs.getString(fieldName);
    }
}
