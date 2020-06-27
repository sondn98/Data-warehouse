package edu.hust.soict.bigdata.facilities.common.wal.impl;

import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.model.WalInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsFile implements WalFile {

    private static FileSystem fs;
    private WalInfo info;

    public HdfsFile(FileSystem fileSystem, String path, String codec){
        if(fs != null)
            fs = fileSystem;
        this.info = new WalInfo();
        this.info.filePath = path;
        this.info.codec = codec;
    }

    @Override
    public String name() {
        return info.filePath.substring(info.filePath.lastIndexOf("/") + 1);
    }

    @Override
    public String codec() {
        return this.info.codec;
    }

    @Override
    public Long length() {
        throw new UnsupportedOperationException("Shouldn't care of this. It's optimal");
    }

    @Override
    public WalInfo getInfo() {
        return info;
    }

    @Override
    public String absolutePath() {
        return info.filePath;
    }

    @Override
    public boolean exists() {
        try {
            return fs.exists(new Path(info.filePath));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public boolean isReachedLimit() {
        throw new UnsupportedOperationException("Shouldn't care of this. It's optimal");
    }

    @Override
    public boolean delete() {
        try {
            return fs.delete(new Path(info.filePath), false);
        } catch (IOException e) {
            return false;
        }
    }
}
