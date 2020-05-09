package edu.hust.soict.bigdata.facilities.common.wal.impl;

import edu.hust.soict.bigdata.facilities.common.wal.WalFile;

import java.io.File;

/**
 * Cài đặt dạng mặc định của file WAL, lưu trữ dưới dạng một
 * file thông thường trên local File System.
 *
 * @author <a href="https://github.com/tjeubaoit">tjeubaoit</a>
 */
public class LocalWalFile implements WalFile {

    private File file;
    private WalInfo info;

    /**
     * Tạo một đối tượng DefaultWalFile mới
     *
     * @param path           đường dẫn tuyệt đối của File
     * @param codec          codec dùng để tạo các đối tượng đọc ghi file
     * @param maxSizeInBytes số bytes quy định độ lớn tối đa của file
     */
    public LocalWalFile(String path, String codec, long maxSizeInBytes) {
        this.file = new File(path);

        this.info.filePath = path;
        this.info.codec = codec;
        this.info.maxSize = maxSizeInBytes;
    }

    @Override
    public Long length() {
        return file.length();
    }

    @Override
    public WalInfo getInfo() {
        return info;
    }

    @Override
    public String name() {
        return file.getName();
    }

    @Override
    public String codec() {
        return info.codec;
    }

    @Override
    public String absolutePath() {
        return this.file.getAbsolutePath();
    }

    @Override
    public boolean exists() {
        return this.file.exists();
    }

    @Override
    public boolean isReachedLimit() {
        return this.file.length() > info.maxSize;
    }

    @Override
    public boolean delete() {
        return this.file.delete();
    }
}
