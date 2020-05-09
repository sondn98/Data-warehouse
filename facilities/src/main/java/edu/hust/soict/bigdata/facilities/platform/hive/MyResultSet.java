package edu.hust.soict.bigdata.facilities.platform.hive;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class MyResultSet {
    public ResultSet rs;

    public MyResultSet(ResultSet rs) {
        this.rs = rs;
    }

    public Long getLong(String column) throws SQLException {
        Long res = this.rs.getLong(column);
        if (this.rs.wasNull()) {
            res = null;
        }
        return res;
    }

    public Integer getInt(String index) throws SQLException {
        Integer res = this.rs.getInt(index);
        if (this.rs.wasNull()) {
            res = null;
        }
        return res;
    }

    public String getString(String column) throws SQLException {
        String res = this.rs.getString(column);
        if (this.rs.wasNull()) {
            res = null;
        }
        return res;
    }

    public Blob getBlob(String column) throws SQLException {
        Blob res = this.rs.getBlob(column);
        if (this.rs.wasNull()) {
            res = null;
        }
        return res;
    }

    public Timestamp getTimestamp(String column) throws SQLException {
        Timestamp res = this.rs.getTimestamp(column);
        if (this.rs.wasNull()) {
            res = null;
        }
        return res;
    }

    public Float getFloat(String column) throws SQLException {
        Float res = this.rs.getFloat(column);
        if(this.rs.wasNull()){
            res = null;
        }

        return res;
    }

    public Short getShort(String column) throws SQLException {
        Short res = this.rs.getShort(column);
        if(this.rs.wasNull()){
            res = null;
        }

        return res;
    }

    public Double getDouble(String column) throws SQLException {
        Double res = this.rs.getDouble(column);
        if(this.rs.wasNull()){
            res = null;
        }

        return res;
    }

    public Boolean getBoolean(String column) throws SQLException {
        Boolean res = this.rs.getBoolean(column);
        if(this.rs.wasNull()){
            res = null;
        }

        return res;
    }

    public boolean next() throws SQLException {
        return this.rs.next();
    }
    public boolean first() throws SQLException {
        return this.rs.first();
    }
}
